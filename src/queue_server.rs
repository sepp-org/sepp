use crate::config::Config;
use crate::pb::sepp::v1::{
    AckRequest, AckResponse, EnqueueBatchRequest, EnqueueBatchResponse, ErrorDetails,
    ExtendRequest, ExtendResponse, GetServerInfoRequest, GetServerInfoResponse, JobResult,
    NackRequest, NackResponse, ReserveRequest, ReserveResponse, job_result,
    queue_service_server::QueueService,
};
use crate::storage::{Storage, now_ms};
use std::{collections::HashMap, time::Duration};

use prost_protovalidate::Validator;
use tokio::time::{Instant, sleep_until};
use tonic::{Request, Response, Status};

pub struct QueueServer {
    validator: Validator,
    storage: Storage,
    max_lease_duration_ms: u64,
    max_reserve_batch: u32,
}

impl QueueServer {
    pub fn new(config: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            validator: Validator::default(),
            storage: Storage::open(config)?,
            max_lease_duration_ms: config.limits.max_lease_duration_ms,
            max_reserve_batch: config.limits.max_reserve_batch,
        })
    }
}

#[tonic::async_trait]
impl QueueService for QueueServer {
    async fn enqueue_batch(
        &self,
        request: Request<EnqueueBatchRequest>,
    ) -> Result<Response<EnqueueBatchResponse>, Status> {
        let req = request.into_inner();
        if req.jobs.is_empty() {
            return Err(Status::invalid_argument(
                "batch must contain at least one job",
            ));
        }

        let mut valid = Vec::new();
        let mut slots: Vec<Option<JobResult>> = Vec::with_capacity(req.jobs.len());
        for job in req.jobs {
            match self.validator.validate(&job) {
                Ok(()) => {
                    slots.push(None);
                    valid.push(job);
                }
                Err(e) => slots.push(Some(JobResult {
                    outcome: Some(job_result::Outcome::Error(ErrorDetails {
                        code: "INVALID_ARGUMENT".to_string(),
                        message: e.to_string(),
                        context: HashMap::new(),
                    })),
                })),
            }
        }

        let mut enqueued = self.storage.enqueue(valid).await?.into_iter();
        let results = slots
            .into_iter()
            .map(|slot| slot.unwrap_or_else(|| enqueued.next().expect("one result per valid job")))
            .collect();

        Ok(Response::new(EnqueueBatchResponse { results }))
    }

    async fn reserve(
        &self,
        request: Request<ReserveRequest>,
    ) -> Result<Response<ReserveResponse>, Status> {
        let req = request.into_inner();
        self.validator
            .validate(&req)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let lease = req.lease_duration_ms.min(self.max_lease_duration_ms);
        let max_jobs = req.max_jobs.unwrap_or(1).clamp(1, self.max_reserve_batch) as usize;
        let deadline = Instant::now() + Duration::from_millis(req.wait_timeout_ms);
        let waiter = self.storage.job_waiter(&req.queues);

        loop {
            let armed = waiter.arm();

            let jobs = self
                .storage
                .reserve_once(req.queues.clone(), lease, max_jobs)
                .await?;
            if !jobs.is_empty() {
                return Ok(Response::new(ReserveResponse { jobs }));
            }

            if Instant::now() >= deadline {
                return Ok(Response::new(ReserveResponse { jobs: Vec::new() }));
            }

            tokio::select! {
                _ = armed => {}
                _ = sleep_until(deadline) => {
                    return Ok(Response::new(ReserveResponse { jobs: Vec::new() }));
                }
            }
        }
    }

    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let req = request.into_inner();
        self.validator
            .validate(&req)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        self.storage.ack(req.job_id.clone(), req.attempt).await?;
        Ok(Response::new(AckResponse { job_id: req.job_id }))
    }

    async fn nack(&self, request: Request<NackRequest>) -> Result<Response<NackResponse>, Status> {
        let req = request.into_inner();
        self.validator
            .validate(&req)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let job_id = req.job_id.clone();
        let dead_lettered = self.storage.nack(req).await?;
        Ok(Response::new(NackResponse {
            job_id,
            dead_lettered,
        }))
    }

    async fn extend(
        &self,
        request: Request<ExtendRequest>,
    ) -> Result<Response<ExtendResponse>, Status> {
        let req = request.into_inner();
        self.validator
            .validate(&req)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let job_id = req.job_id.clone();
        let lease_expires_at = self.storage.extend(req).await?;
        Ok(Response::new(ExtendResponse {
            job_id,
            lease_expires_at,
        }))
    }

    async fn get_server_info(
        &self,
        _request: Request<GetServerInfoRequest>,
    ) -> Result<Response<GetServerInfoResponse>, Status> {
        Ok(Response::new(GetServerInfoResponse {
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            supported_protocol_versions: vec!["1.0".to_string()],
            server_time_ms: now_ms(),
            allowed_encodings: vec!["application/json".into(), "application/octet-stream".into()],
            max_payload_bytes: 1 << 20,
            max_custom_entries: 64,
            max_custom_total_bytes: 16 << 10,
            max_custom_key_bytes: 256,
        }))
    }
}
