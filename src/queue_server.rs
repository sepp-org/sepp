use crate::config::{Config, EffectiveLimits};
use crate::metrics::{self, Metrics};
use crate::pb::sepp::v1::{
    self as pb, AckRequest, AckResponse, EnqueueAtomicResponse, EnqueueBatchRequest,
    EnqueueBatchResponse, EnqueueRequest, ExtendRequest, ExtendResponse, GetServerInfoRequest,
    GetServerInfoResponse, JobResult, NackRequest, NackResponse, PrimitiveValue, ReserveRequest,
    ReserveResponse, enqueue_atomic_response, job_result, queue_service_server::QueueService,
};
use crate::queues::{QueueRegistry, SharedRegistry};
use crate::storage::{Storage, now_ms};
use crate::telemetry;
use std::{time::Duration, time::Instant as StdInstant};

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
        let metrics = Metrics::new(config.metrics.enabled || config.metrics.prometheus_enabled);
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
    ) -> Result<(), pb::JobRejection> {
        use pb::job_rejection::Reason;
        let s = &self.server_limits;
        if job.queue.len() > s.max_queue_name_bytes as usize {
            return Err(pb::JobRejection {
                reason: Some(Reason::QueueNameTooLong(pb::QueueNameTooLong {
                    limit: s.max_queue_name_bytes,
                    actual: job.queue.len() as u64,
                })),
            });
        }
        if job.job_type.len() > s.max_job_type_bytes as usize {
            return Err(pb::JobRejection {
                reason: Some(Reason::JobTypeNameTooLong(pb::JobTypeNameTooLong {
                    limit: s.max_job_type_bytes,
                    actual: job.job_type.len() as u64,
                })),
            });
        }
        if let Some(allowed) = &limits.allowed_job_types
            && !allowed.iter().any(|t| t == &job.job_type)
        {
            return Err(pb::JobRejection {
                reason: Some(Reason::JobTypeNotAllowed(pb::JobTypeNotAllowed {
                    job_type: job.job_type.clone(),
                    allowed: allowed.clone(),
                })),
            });
        }
        if let Some(key) = &job.idempotency_key
            && key.len() > s.max_idempotency_key_bytes as usize
        {
            return Err(pb::JobRejection {
                reason: Some(Reason::IdempotencyKeyTooLong(pb::IdempotencyKeyTooLong {
                    limit: s.max_idempotency_key_bytes,
                    actual: key.len() as u64,
                })),
            });
        }
        if let Some(payload) = &job.payload
            && payload.data.len() as u64 > limits.max_payload_bytes
        {
            return Err(pb::JobRejection {
                reason: Some(Reason::PayloadTooLarge(pb::PayloadTooLarge {
                    limit: limits.max_payload_bytes,
                    actual: payload.data.len() as u64,
                })),
            });
        }
        if let Some(payload) = &job.payload
            && let Some(allowed) = &limits.allowed_encodings
            && !allowed.iter().any(|e| e == &payload.encoding)
        {
            return Err(pb::JobRejection {
                reason: Some(Reason::EncodingNotAllowed(pb::EncodingNotAllowed {
                    encoding: payload.encoding.clone(),
                    allowed: allowed.clone(),
                })),
            });
        }
        if job.custom.len() as u64 > limits.max_custom_entries as u64 {
            return Err(pb::JobRejection {
                reason: Some(Reason::CustomEntriesTooMany(pb::CustomEntriesTooMany {
                    limit: limits.max_custom_entries,
                    actual: job.custom.len() as u32,
                })),
            });
        }
        let mut custom_bytes: u64 = 0;
        for (key, value) in &job.custom {
            if key.len() as u64 > limits.max_custom_key_bytes as u64 {
                return Err(pb::JobRejection {
                    reason: Some(Reason::CustomKeyTooLong(pb::CustomKeyTooLong {
                        key: key.clone(),
                        limit: limits.max_custom_key_bytes,
                        actual: key.len() as u64,
                    })),
                });
            }
            custom_bytes += key.len() as u64 + primitive_value_bytes(value);
        }
        if custom_bytes > limits.max_custom_total_bytes {
            return Err(pb::JobRejection {
                reason: Some(Reason::CustomMapTooLarge(pb::CustomMapTooLarge {
                    limit: limits.max_custom_total_bytes,
                    actual: custom_bytes,
                })),
            });
        }
        if let Some(at) = job.scheduled_at
            && at > now_ms().saturating_add(limits.max_schedule_horizon_ms as i64)
        {
            return Err(pb::JobRejection {
                reason: Some(Reason::ScheduledTooFar(pb::ScheduledTooFar {
                    horizon_ms: limits.max_schedule_horizon_ms,
                    actual_ms: at,
                })),
            });
        }
        Ok(())
    }

    fn classify_enqueue(
        &self,
        job: &EnqueueRequest,
        registry: &QueueRegistry,
    ) -> Result<(), pb::JobRejection> {
        use pb::job_rejection::Reason;
        if let Err(e) = self.validator.validate(job) {
            return Err(pb::JobRejection {
                reason: Some(Reason::InvalidRequest(pb::InvalidRequest {
                    message: e.to_string(),
                })),
            });
        }
        if self.strict_queues && !registry.is_declared(&job.queue) {
            return Err(pb::JobRejection {
                reason: Some(Reason::UnknownQueue(pb::UnknownQueue {
                    queue: job.queue.clone(),
                })),
            });
        }
        let limits = registry.effective(&job.queue);
        self.check_enqueue_limits(job, &limits)
    }

    async fn commit_validated(
        &self,
        mut valid: Vec<EnqueueRequest>,
    ) -> Result<Vec<pb::EnqueueResponse>, Status> {
        for job in &mut valid {
            if job.trace_context.is_none() {
                job.trace_context = telemetry::current_trace_context();
            }
        }
        let responses = self.storage.enqueue(valid).await?;
        let job_ids: Vec<&str> = responses.iter().map(|r| r.job_id.as_str()).collect();
        tracing::Span::current().record("job_ids", tracing::field::debug(&job_ids));
        Ok(responses)
    }
}

// For metrics
fn rejection_label(rejection: &pb::JobRejection) -> &'static str {
    use pb::job_rejection::Reason;
    match rejection
        .reason
        .as_ref()
        .expect("rejection.reason is always set at construction")
    {
        Reason::UnknownQueue(_) => "unknown_queue",
        Reason::PayloadTooLarge(_) => "payload_too_large",
        Reason::EncodingNotAllowed(_) => "encoding_not_allowed",
        Reason::JobTypeNotAllowed(_) => "job_type_not_allowed",
        Reason::CustomEntriesTooMany(_) => "custom_entries_too_many",
        Reason::CustomMapTooLarge(_) => "custom_map_too_large",
        Reason::CustomKeyTooLong(_) => "custom_key_too_long",
        Reason::QueueNameTooLong(_) => "queue_name_too_long",
        Reason::JobTypeNameTooLong(_) => "job_type_name_too_long",
        Reason::IdempotencyKeyTooLong(_) => "idempotency_key_too_long",
        Reason::ScheduledTooFar(_) => "scheduled_too_far",
        Reason::InvalidRequest(_) => "invalid_request",
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
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
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
                match self.classify_enqueue(&job, &registry) {
                    Ok(()) => {
                        slots.push(None);
                        valid.push(job);
                    }
                    Err(rejection) => {
                        let label = rejection_label(&rejection);
                        tracing::info!(
                            queue = %job.queue,
                            job_type = %job.job_type,
                            reason = label,
                            "enqueue rejected",
                        );
                        self.metrics.record_rejected(&job.queue, label);
                        slots.push(Some(JobResult {
                            outcome: Some(job_result::Outcome::Rejection(rejection)),
                        }));
                    }
                }
            }

            let mut enqueued = self.commit_validated(valid).await?.into_iter();
            let results = slots
                .into_iter()
                .map(|slot| {
                    slot.unwrap_or_else(|| JobResult {
                        outcome: Some(job_result::Outcome::Success(
                            enqueued.next().expect("one result per valid job"),
                        )),
                    })
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

    async fn enqueue_atomic(
        &self,
        request: Request<EnqueueBatchRequest>,
    ) -> Result<Response<EnqueueAtomicResponse>, Status> {
        let span = tracing::info_span!(
            "sepp.enqueue_atomic",
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
            job_ids = tracing::field::Empty,
            error = tracing::field::Empty,
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
            let mut errors: Vec<pb::JobValidationError> = Vec::new();
            let mut valid: Vec<EnqueueRequest> = Vec::with_capacity(req.jobs.len());
            for (index, job) in req.jobs.into_iter().enumerate() {
                match self.classify_enqueue(&job, &registry) {
                    Ok(()) => valid.push(job),
                    Err(rejection) => {
                        let label = rejection_label(&rejection);
                        tracing::info!(
                            queue = %job.queue,
                            job_type = %job.job_type,
                            index = index as u32,
                            reason = label,
                            "enqueue_atomic rejected job",
                        );
                        self.metrics.record_rejected(&job.queue, label);
                        errors.push(pb::JobValidationError {
                            index: index as u32,
                            rejection: Some(rejection),
                        });
                    }
                }
            }

            if !errors.is_empty() {
                return Ok(Response::new(EnqueueAtomicResponse {
                    outcome: Some(enqueue_atomic_response::Outcome::Rejection(
                        pb::BatchValidationFailure { errors },
                    )),
                }));
            }

            let responses = self.commit_validated(valid).await?;
            Ok(Response::new(EnqueueAtomicResponse {
                outcome: Some(enqueue_atomic_response::Outcome::Success(
                    pb::EnqueueAtomicSuccess { responses },
                )),
            }))
        }
        .instrument(span.clone())
        .await;
        self.metrics
            .observe("enqueue_atomic", started, &span, &result);
        result
    }

    async fn reserve(
        &self,
        request: Request<ReserveRequest>,
    ) -> Result<Response<ReserveResponse>, Status> {
        let span = tracing::info_span!(
            "sepp.reserve",
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
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
                    return Err(Status::failed_precondition(format!(
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

                let mut jobs = self
                    .storage
                    .reserve_once(req.queues.clone(), lease, max_jobs)
                    .await?;
                if !jobs.is_empty() {
                    let job_ids: Vec<&str> = jobs.iter().map(|j| j.id.as_str()).collect();
                    tracing::Span::current().record("job_ids", tracing::field::debug(&job_ids));

                    if telemetry::enabled() {
                        for job in &mut jobs {
                            let deliver = tracing::info_span!(
                                "sepp.deliver",
                                job_id = %job.id,
                                job_type = %job.job_type,
                                attempt = job.attempt,
                            );
                            telemetry::link_from_proto(&deliver, job.trace_context.as_ref());
                            if let Some(delivery_ctx) =
                                deliver.in_scope(telemetry::current_trace_context)
                            {
                                job.trace_context = Some(delivery_ctx);
                            }
                        }
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
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
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
            let trace_context = self.storage.ack(req.job_id.clone(), req.attempt).await?;
            telemetry::link_from_proto(&tracing::Span::current(), trace_context.as_ref());
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
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
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
            let (dead_lettered, trace_context) = self.storage.nack(req).await?;
            telemetry::link_from_proto(&tracing::Span::current(), trace_context.as_ref());
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
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
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
            let (lease_expires_at, trace_context) = self.storage.extend(req).await?;
            telemetry::link_from_proto(&tracing::Span::current(), trace_context.as_ref());
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
        let _span = tracing::info_span!("sepp.get_server_info", otel.kind = "server").entered();
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
