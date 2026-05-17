use crate::config::Config;
use crate::pb::sepp::v1::{
    AckRequest, AckResponse, EnqueueBatchRequest, EnqueueBatchResponse, ErrorDetails,
    ExtendRequest, ExtendResponse, GetServerInfoRequest, GetServerInfoResponse, JobResult,
    NackRequest, NackResponse, ReserveRequest, ReserveResponse, job_result,
    queue_service_server::QueueService,
};
use crate::storage::{Storage, now_ms};
use crate::telemetry;
use std::{collections::HashMap, time::Duration};

use prost_protovalidate::Validator;
use tokio::time::{Instant, sleep_until};
use tonic::{Request, Response, Status};
use tracing::Instrument;

pub struct QueueServer {
    validator: Validator,
    storage: Storage,
    max_lease_duration_ms: u64,
    max_reserve_batch: u32,
    max_wait_timeout_ms: u64,
}

impl QueueServer {
    pub fn new(config: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            validator: Validator::default(),
            storage: Storage::open(config)?,
            max_lease_duration_ms: config.limits.max_lease_duration_ms,
            max_reserve_batch: config.limits.max_reserve_batch,
            max_wait_timeout_ms: config.limits.max_wait_timeout_ms,
        })
    }
}

#[tonic::async_trait]
impl QueueService for QueueServer {
    async fn enqueue_batch(
        &self,
        request: Request<EnqueueBatchRequest>,
    ) -> Result<Response<EnqueueBatchResponse>, Status> {
        // The span must be parented *before* it is entered, so create it here
        // and `.instrument` the body rather than using `#[tracing::instrument]`.
        let span = tracing::info_span!("sepp.enqueue", job_ids = tracing::field::Empty);
        telemetry::set_parent_from_metadata(&span, request.metadata());
        async move {
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

            // Pass through a producer-supplied trace context untouched;
            // otherwise stamp this enqueue span so the worker has a span to
            // link to.
            for job in &mut valid {
                if job.trace_context.is_none() {
                    job.trace_context = telemetry::current_trace_context();
                }
            }

            let enqueued = self.storage.enqueue(valid).await?;
            let job_ids: Vec<&str> = enqueued
                .iter()
                .filter_map(|r| match &r.outcome {
                    Some(job_result::Outcome::Success(s)) => Some(s.job_id.as_str()),
                    _ => None,
                })
                .collect();
            tracing::Span::current().record("job_ids", tracing::field::debug(&job_ids));

            let mut enqueued = enqueued.into_iter();
            let results = slots
                .into_iter()
                .map(|slot| {
                    slot.unwrap_or_else(|| enqueued.next().expect("one result per valid job"))
                })
                .collect();

            Ok(Response::new(EnqueueBatchResponse { results }))
        }
        .instrument(span)
        .await
    }

    async fn reserve(
        &self,
        request: Request<ReserveRequest>,
    ) -> Result<Response<ReserveResponse>, Status> {
        let span = tracing::info_span!(
            "sepp.reserve",
            job_ids = tracing::field::Empty,
            worker_id = request.get_ref().worker_id.as_deref().unwrap_or("<none>"),
        );
        telemetry::set_parent_from_metadata(&span, request.metadata());
        async move {
            let req = request.into_inner();
            self.validator
                .validate(&req)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            let lease = req.lease_duration_ms.min(self.max_lease_duration_ms);
            let max_jobs = req.max_jobs.unwrap_or(1).clamp(1, self.max_reserve_batch) as usize;
            let wait = req.wait_timeout_ms.min(self.max_wait_timeout_ms);
            let deadline = Instant::now() + Duration::from_millis(wait);
            let waiter = self.storage.job_waiter(&req.queues);

            loop {
                let armed = waiter.arm();

                let jobs = self
                    .storage
                    .reserve_once(req.queues.clone(), lease, max_jobs)
                    .await?;
                if !jobs.is_empty() {
                    let job_ids: Vec<&str> = jobs.iter().map(|j| j.id.as_str()).collect();
                    tracing::Span::current().record("job_ids", tracing::field::debug(&job_ids));
                    // One short span per delivered job, sitting under this
                    // `sepp.reserve` span and linked to the job's originating
                    // (producer) trace — so delivery is cross-referenceable
                    // without extending the producer's trace.
                    for job in &jobs {
                        let deliver = tracing::info_span!(
                            "sepp.deliver",
                            job_id = %job.id,
                            job_type = %job.job_type,
                            attempt = job.attempt,
                        );
                        telemetry::link_from_proto(&deliver, job.trace_context.as_ref());
                        deliver.in_scope(|| {});
                    }
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
        .instrument(span)
        .await
    }

    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let span = tracing::info_span!("sepp.ack", job_id = %request.get_ref().job_id, worker_id = request.get_ref().worker_id.as_deref().unwrap_or("<none>"));
        telemetry::set_parent_from_metadata(&span, request.metadata());
        async move {
            let req = request.into_inner();
            self.validator
                .validate(&req)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            self.storage.ack(req.job_id.clone(), req.attempt).await?;
            Ok(Response::new(AckResponse { job_id: req.job_id }))
        }
        .instrument(span)
        .await
    }

    async fn nack(&self, request: Request<NackRequest>) -> Result<Response<NackResponse>, Status> {
        let span = tracing::info_span!("sepp.nack", job_id = %request.get_ref().job_id, worker_id = request.get_ref().worker_id.as_deref().unwrap_or("<none>"));
        telemetry::set_parent_from_metadata(&span, request.metadata());
        async move {
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
        .instrument(span)
        .await
    }

    async fn extend(
        &self,
        request: Request<ExtendRequest>,
    ) -> Result<Response<ExtendResponse>, Status> {
        let span = tracing::info_span!("sepp.extend", job_id = %request.get_ref().job_id, worker_id = request.get_ref().worker_id.as_deref().unwrap_or("<none>"));
        telemetry::set_parent_from_metadata(&span, request.metadata());
        async move {
            let mut req = request.into_inner();
            self.validator
                .validate(&req)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            req.lease_duration_ms = req.lease_duration_ms.min(self.max_lease_duration_ms);
            let job_id = req.job_id.clone();
            let lease_expires_at = self.storage.extend(req).await?;
            Ok(Response::new(ExtendResponse {
                job_id,
                lease_expires_at,
            }))
        }
        .instrument(span)
        .await
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
