use crate::config::{Config, EffectiveLimits};
use crate::metrics::{self, Metrics};
use crate::pb::sepp::v1::{
    AckRequest, AckResponse, EnqueueBatchRequest, EnqueueBatchResponse, EnqueueRequest,
    ErrorDetails, ExtendRequest, ExtendResponse, GetServerInfoRequest, GetServerInfoResponse,
    JobResult, NackRequest, NackResponse, PrimitiveValue, ReserveRequest, ReserveResponse,
    job_result, queue_service_server::QueueService,
};
use crate::queues::SharedRegistry;
use crate::storage::{Storage, now_ms};
use crate::telemetry;
use std::{collections::HashMap, time::Duration, time::Instant as StdInstant};

use opentelemetry::metrics::ObservableGauge;

use prost_protovalidate::Validator;
use tokio::time::{Instant, sleep_until};
use tonic::{Request, Response, Status};
use tracing::Instrument;

struct ServerLimits {
    max_reserve_batch: u32,
    max_reserve_queues: u32,
    max_wait_timeout_ms: u64,
    max_enqueue_batch: u32,
    max_queue_name_bytes: u32,
    max_job_type_bytes: u32,
    max_idempotency_key_bytes: u32,
}

pub struct QueueServer {
    validator: Validator,
    storage: Storage,
    registry: SharedRegistry,
    strict_queues: bool,
    server_limits: ServerLimits,
    metrics: Metrics,
    _command_queue_gauge: ObservableGauge<u64>,
    _queue_depth_gauges: Vec<ObservableGauge<u64>>,
}

impl QueueServer {
    pub fn new(
        config: &Config,
        registry: SharedRegistry,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let metrics = Metrics::new(config.metrics.enabled);
        let storage = Storage::open(config, registry.clone(), metrics.clone())?;
        let command_queue_gauge = {
            let storage = storage.clone();
            metrics::register_command_queue_gauge(move || storage.command_queue_depth() as u64)
        };
        let queue_depth_gauges = metrics.register_queue_depth_gauges();
        Ok(Self {
            validator: Validator::default(),
            storage,
            registry,
            strict_queues: config.server.strict_queues,
            server_limits: ServerLimits {
                max_reserve_batch: config.limits.max_reserve_batch,
                max_reserve_queues: config.limits.max_reserve_queues,
                max_wait_timeout_ms: config.limits.max_wait_timeout_ms,
                max_enqueue_batch: config.limits.max_enqueue_batch,
                max_queue_name_bytes: config.limits.max_queue_name_bytes,
                max_job_type_bytes: config.limits.max_job_type_bytes,
                max_idempotency_key_bytes: config.limits.max_idempotency_key_bytes,
            },
            metrics,
            _command_queue_gauge: command_queue_gauge,
            _queue_depth_gauges: queue_depth_gauges,
        })
    }

    fn check_enqueue_limits(
        &self,
        job: &EnqueueRequest,
        limits: &EffectiveLimits,
    ) -> Result<(), String> {
        let s = &self.server_limits;
        if job.queue.len() > s.max_queue_name_bytes as usize {
            return Err(format!(
                "queue name exceeds max_queue_name_bytes ({})",
                s.max_queue_name_bytes
            ));
        }
        if job.job_type.len() > s.max_job_type_bytes as usize {
            return Err(format!(
                "job_type exceeds max_job_type_bytes ({})",
                s.max_job_type_bytes
            ));
        }
        if let Some(allowed) = &limits.allowed_job_types
            && !allowed.iter().any(|t| t == &job.job_type)
        {
            return Err(format!(
                "job_type {:?} is not accepted by this queue",
                job.job_type
            ));
        }
        if let Some(key) = &job.idempotency_key
            && key.len() > s.max_idempotency_key_bytes as usize
        {
            return Err(format!(
                "idempotency_key exceeds max_idempotency_key_bytes ({})",
                s.max_idempotency_key_bytes
            ));
        }
        if let Some(payload) = &job.payload
            && payload.data.len() as u64 > limits.max_payload_bytes
        {
            return Err(format!(
                "payload exceeds max_payload_bytes ({})",
                limits.max_payload_bytes
            ));
        }
        if let Some(payload) = &job.payload
            && let Some(allowed) = &limits.allowed_encodings
            && !allowed.iter().any(|e| e == &payload.encoding)
        {
            return Err(format!(
                "payload encoding {:?} is not accepted by this server",
                payload.encoding
            ));
        }
        if job.custom.len() as u64 > limits.max_custom_entries as u64 {
            return Err(format!(
                "custom map exceeds max_custom_entries ({})",
                limits.max_custom_entries
            ));
        }
        let mut custom_bytes: u64 = 0;
        for (key, value) in &job.custom {
            if key.len() as u64 > limits.max_custom_key_bytes as u64 {
                return Err(format!(
                    "custom key exceeds max_custom_key_bytes ({})",
                    limits.max_custom_key_bytes
                ));
            }
            custom_bytes += key.len() as u64 + primitive_value_bytes(value);
        }
        if custom_bytes > limits.max_custom_total_bytes {
            return Err(format!(
                "custom map exceeds max_custom_total_bytes ({})",
                limits.max_custom_total_bytes
            ));
        }
        if let Some(at) = job.scheduled_at
            && at > now_ms().saturating_add(limits.max_schedule_horizon_ms as i64)
        {
            return Err(format!(
                "scheduled_at is beyond max_schedule_horizon_ms ({})",
                limits.max_schedule_horizon_ms
            ));
        }
        Ok(())
    }
}

fn primitive_value_bytes(value: &PrimitiveValue) -> u64 {
    use crate::pb::sepp::v1::primitive_value::Value;
    match &value.value {
        Some(Value::StringValue(s)) => s.len() as u64,
        Some(Value::DoubleValue(_) | Value::IntValue(_)) => 8,
        Some(Value::BoolValue(_)) => 1,
        None => 0,
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
        let span = tracing::info_span!(
            "sepp.enqueue",
            job_ids = tracing::field::Empty,
            error = tracing::field::Empty
        );
        telemetry::set_parent_from_metadata(&span, request.metadata());
        let started = StdInstant::now();
        let result = async move {
            let req = request.into_inner();
            if req.jobs.is_empty() {
                return Err(Status::invalid_argument(
                    "batch must contain at least one job",
                ));
            }
            if req.jobs.len() as u64 > self.server_limits.max_enqueue_batch as u64 {
                return Err(Status::invalid_argument(format!(
                    "batch exceeds max_enqueue_batch ({})",
                    self.server_limits.max_enqueue_batch
                )));
            }

            let registry = self.registry.load();
            let mut valid = Vec::new();
            let mut slots: Vec<Option<JobResult>> = Vec::with_capacity(req.jobs.len());
            for job in req.jobs {
                let rejection: Option<(&'static str, &'static str, String)> =
                    match self.validator.validate(&job) {
                        Ok(()) => {
                            if self.strict_queues && !registry.is_declared(&job.queue) {
                                Some((
                                    "UNKNOWN_QUEUE",
                                    "unknown_queue",
                                    format!("queue {:?} is not declared (strict mode)", job.queue),
                                ))
                            } else {
                                let limits = registry.effective(&job.queue);
                                self.check_enqueue_limits(&job, &limits)
                                    .err()
                                    .map(|m| ("INVALID_ARGUMENT", "invalid_argument", m))
                            }
                        }
                        Err(e) => Some(("INVALID_ARGUMENT", "invalid_argument", e.to_string())),
                    };
                match rejection {
                    None => {
                        slots.push(None);
                        valid.push(job);
                    }
                    Some((code, reason, message)) => {
                        tracing::info!(
                            queue = %job.queue,
                            job_type = %job.job_type,
                            code,
                            %message,
                            "enqueue rejected",
                        );
                        self.metrics.record_rejected(&job.queue, reason);
                        slots.push(Some(JobResult {
                            outcome: Some(job_result::Outcome::Error(ErrorDetails {
                                code: code.to_string(),
                                message,
                                context: HashMap::new(),
                            })),
                        }));
                    }
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
        .instrument(span.clone())
        .await;
        self.metrics
            .observe("enqueue_batch", started, &span, &result);
        result
    }

    async fn reserve(
        &self,
        request: Request<ReserveRequest>,
    ) -> Result<Response<ReserveResponse>, Status> {
        let span = tracing::info_span!(
            "sepp.reserve",
            job_ids = tracing::field::Empty,
            error = tracing::field::Empty,
            worker_id = request.get_ref().worker_id.as_deref().unwrap_or("<none>"),
        );
        telemetry::set_parent_from_metadata(&span, request.metadata());
        let started = StdInstant::now();
        let result = async move {
            let req = request.into_inner();
            self.validator
                .validate(&req)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            if req.queues.len() as u64 > self.server_limits.max_reserve_queues as u64 {
                return Err(Status::invalid_argument(format!(
                    "reserve exceeds max_reserve_queues ({})",
                    self.server_limits.max_reserve_queues
                )));
            }
            if let Some(q) = req
                .queues
                .iter()
                .find(|q| q.len() > self.server_limits.max_queue_name_bytes as usize)
            {
                return Err(Status::invalid_argument(format!(
                    "queue name {:?} exceeds max_queue_name_bytes ({})",
                    q, self.server_limits.max_queue_name_bytes
                )));
            }

            let registry = self.registry.load();
            if self.strict_queues {
                let unknown: Vec<&str> = req
                    .queues
                    .iter()
                    .filter(|q| !registry.is_declared(q))
                    .map(String::as_str)
                    .collect();
                if !unknown.is_empty() {
                    return Err(Status::invalid_argument(format!(
                        "queue(s) not declared (strict mode): {unknown:?}"
                    )));
                }
            }

            let max_lease = req
                .queues
                .iter()
                .map(|q| registry.effective(q).max_lease_duration_ms)
                .min()
                .unwrap_or(u64::MAX);
            let lease = req.lease_duration_ms.min(max_lease);
            let max_jobs = req
                .max_jobs
                .unwrap_or(1)
                .clamp(1, self.server_limits.max_reserve_batch) as usize;
            let wait = req
                .wait_timeout_ms
                .min(self.server_limits.max_wait_timeout_ms);
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
                    self.metrics.record_reserve_empty(&req.queues);
                    return Ok(Response::new(ReserveResponse { jobs: Vec::new() }));
                }

                tokio::select! {
                    _ = armed => {}
                    _ = sleep_until(deadline) => {
                        self.metrics.record_reserve_empty(&req.queues);
                        return Ok(Response::new(ReserveResponse { jobs: Vec::new() }));
                    }
                }
            }
        }
        .instrument(span.clone())
        .await;
        self.metrics.observe("reserve", started, &span, &result);
        result
    }

    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let span = tracing::info_span!(
            "sepp.ack",
            job_id = %request.get_ref().job_id,
            worker_id = request.get_ref().worker_id.as_deref().unwrap_or("<none>"),
            error = tracing::field::Empty
        );
        telemetry::set_parent_from_metadata(&span, request.metadata());
        let started = StdInstant::now();
        let result = async move {
            let req = request.into_inner();
            self.validator
                .validate(&req)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            self.storage.ack(req.job_id.clone(), req.attempt).await?;
            Ok(Response::new(AckResponse { job_id: req.job_id }))
        }
        .instrument(span.clone())
        .await;
        self.metrics.observe("ack", started, &span, &result);
        result
    }

    async fn nack(&self, request: Request<NackRequest>) -> Result<Response<NackResponse>, Status> {
        let span = tracing::info_span!(
            "sepp.nack",
            job_id = %request.get_ref().job_id,
            worker_id = request.get_ref().worker_id.as_deref().unwrap_or("<none>"),
            error = tracing::field::Empty
        );
        telemetry::set_parent_from_metadata(&span, request.metadata());
        let started = StdInstant::now();
        let result = async move {
            let req = request.into_inner();
            self.validator
                .validate(&req)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let job_id = req.job_id.clone();
            let dead_lettered = self.storage.nack(req).await?;
            if dead_lettered {
                tracing::info!(%job_id, "job dead-lettered via nack");
            }
            Ok(Response::new(NackResponse {
                job_id,
                dead_lettered,
            }))
        }
        .instrument(span.clone())
        .await;
        self.metrics.observe("nack", started, &span, &result);
        result
    }

    async fn extend(
        &self,
        request: Request<ExtendRequest>,
    ) -> Result<Response<ExtendResponse>, Status> {
        let span = tracing::info_span!(
            "sepp.extend",
            job_id = %request.get_ref().job_id,
            worker_id = request.get_ref().worker_id.as_deref().unwrap_or("<none>"),
            error = tracing::field::Empty
        );
        telemetry::set_parent_from_metadata(&span, request.metadata());
        let started = StdInstant::now();
        let result = async move {
            let req = request.into_inner();
            self.validator
                .validate(&req)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            // The per-queue lease ceiling is applied inside storage where the
            // job's queue is known via its Inflight record.
            let job_id = req.job_id.clone();
            let lease_expires_at = self.storage.extend(req).await?;
            Ok(Response::new(ExtendResponse {
                job_id,
                lease_expires_at,
            }))
        }
        .instrument(span.clone())
        .await;
        self.metrics.observe("extend", started, &span, &result);
        result
    }

    async fn get_server_info(
        &self,
        _request: Request<GetServerInfoRequest>,
    ) -> Result<Response<GetServerInfoResponse>, Status> {
        let defaults = self.registry.load().effective("");
        Ok(Response::new(GetServerInfoResponse {
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            supported_protocol_versions: vec!["1.0".to_string()],
            server_time_ms: now_ms(),
            restricts_encodings: defaults.allowed_encodings.is_some(),
            allowed_encodings: defaults.allowed_encodings.unwrap_or_default(),
            max_payload_bytes: defaults.max_payload_bytes,
            max_custom_entries: defaults.max_custom_entries,
            max_custom_total_bytes: defaults.max_custom_total_bytes,
            max_custom_key_bytes: defaults.max_custom_key_bytes,
        }))
    }
}
