use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use serde::Deserialize;
use serde_json::{Value, json};
use toml_edit::DocumentMut;
use tonic::Status;

use crate::config::{Config, EffectiveLimits};
use crate::config_watch;
use crate::keys::DeadLetterKey;
use crate::pb::sepp::v1::{self as pb, EnqueueRequest, Payload, PrimitiveValue};
use crate::pb::{millis_to_timestamp, timestamp_to_millis};
use crate::queue_server::{ServerLimits, classify_enqueue, rejection_label};
use crate::storage::{AdminDeadLetter, AdminJob, AdminJobState, PeekState, now_ms};

use super::{AdminState, config_edit};

const INLINE_PAYLOAD_MAX: usize = 4096;
const DEAD_LETTER_KEYS_MAX: usize = 100;
const PURGE_CHUNK: usize = 1000;
// Must comfortably exceed the watcher's 500ms debounce.
const RELOAD_WAIT: Duration = Duration::from_secs(3);

// Mirrors config_watch::restart_only_changes.
const RESTART_ONLY: &[&str] = &[
    "server.listen_addr",
    "server.db_path",
    "server.tls_cert_path",
    "server.tls_key_path",
    "limits.max_message_bytes",
    "storage.persist_mode",
    "storage.sweep_interval_ms",
    "storage.sweep_limit",
    "storage.dead_letter_retention_ms",
    "storage.command_queue_capacity",
    "storage.cache_size_bytes",
    "storage.max_journaling_size_bytes",
    "storage.max_cached_files",
    "storage.worker_threads",
    "logging",
    "tracing",
    "metrics",
    "admin.enabled",
    "admin.listen_addr",
];

pub(super) struct ApiError {
    status: StatusCode,
    code: &'static str,
    error: String,
    rejection: Option<Value>,
}

type ApiResult<T> = Result<T, ApiError>;

impl ApiError {
    fn new(status: StatusCode, code: &'static str, error: impl Into<String>) -> Self {
        Self {
            status,
            code,
            error: error.into(),
            rejection: None,
        }
    }

    fn bad_request(code: &'static str, error: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, error)
    }

    fn not_found(error: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", error)
    }

    fn internal(error: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = json!({ "error": self.error, "code": self.code });
        if let Some(rejection) = self.rejection {
            body["rejection"] = rejection;
        }
        (self.status, Json(body)).into_response()
    }
}

impl From<Status> for ApiError {
    fn from(status: Status) -> Self {
        let (http, code) = match status.code() {
            tonic::Code::InvalidArgument => (StatusCode::BAD_REQUEST, "invalid_argument"),
            tonic::Code::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            tonic::Code::FailedPrecondition => (StatusCode::CONFLICT, "failed_precondition"),
            tonic::Code::ResourceExhausted => (StatusCode::CONFLICT, "resource_exhausted"),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };
        Self::new(http, code, status.message().to_string())
    }
}

fn decode_b64(field: &str, s: &str) -> Result<Vec<u8>, ApiError> {
    B64.decode(s)
        .map_err(|_| ApiError::bad_request("invalid_argument", format!("{field} is not valid base64")))
}

async fn resolve_blocking<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, ApiError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| ApiError::internal(format!("blocking read failed: {e}")))
}

// ---------------------------------------------------------------------------
// Overview and server info

pub(super) async fn overview(State(state): State<Arc<AdminState>>) -> Json<Value> {
    let config = state.config.load();
    let frame = state.latest_frame.load_full();
    let history = state.history.read().expect("history lock");

    Json(json!({
        "server": {
            "version": env!("CARGO_PKG_VERSION"),
            "started_at_ms": state.started_at_ms,
            "now_ms": now_ms(),
            "strict_queues": config.server.strict_queues,
            // Restart-only: the storage engine runs with the boot value even
            // after the on-disk config changed.
            "dead_letter_retention_ms": state.boot.storage.dead_letter_retention_ms,
            "command_queue_len": state.storage.command_queue_depth(),
        },
        "frame": &*frame,
        "history": &*history,
    }))
}

pub(super) async fn server_info(State(state): State<Arc<AdminState>>) -> Json<Value> {
    let config = state.config.load();
    // listen_addr, TLS, and db_path are restart-only: report the values the
    // server actually runs with, not whatever landed on disk since boot.
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "started_at_ms": state.started_at_ms,
        "listen_addr": state.boot.server.listen_addr.to_string(),
        "tls": state.boot.server.tls_enabled(),
        "auth_enforcing": config.auth.api_keys.is_some(),
        "strict_queues": config.server.strict_queues,
        "db_path": &state.boot.server.db_path,
    }))
}

// ---------------------------------------------------------------------------
// Queues

fn effective_json(e: &EffectiveLimits) -> Value {
    json!({
        "max_lease_duration_ms": e.max_lease_duration_ms,
        "default_max_attempts": e.default_max_attempts,
        "max_attempts_ceiling": e.max_attempts_ceiling,
        "default_priority": e.default_priority,
        "max_payload_bytes": e.max_payload_bytes,
        "allowed_encodings": e.allowed_encodings,
        "allowed_job_types": e.allowed_job_types,
        "max_schedule_horizon_ms": e.max_schedule_horizon_ms,
        "max_custom_entries": e.max_custom_entries,
        "max_custom_total_bytes": e.max_custom_total_bytes,
        "max_custom_key_bytes": e.max_custom_key_bytes,
        "dedup_window_ms": e.dedup_window_ms,
        "max_queue_depth": e.max_queue_depth,
    })
}

fn queue_json(state: &AdminState, config: &Config, name: &str) -> Value {
    let snapshot = state.stats.load_full();
    let depth = |m: &HashMap<String, u64>| m.get(name).copied().unwrap_or(0);
    let overrides = config.queues.iter().find(|q| q.name == name);

    json!({
        "name": name,
        "declared": overrides.is_some(),
        "depths": {
            "ready": depth(&snapshot.depths.ready),
            "scheduled": depth(&snapshot.depths.scheduled),
            "inflight": depth(&snapshot.depths.inflight),
            "dead_lettered": depth(&snapshot.depths.dead_letter),
        },
        "overrides": overrides,
        "effective": effective_json(&state.registry.load().effective(name)),
    })
}

pub(super) async fn list_queues(State(state): State<Arc<AdminState>>) -> Json<Value> {
    let config = state.config.load();
    let snapshot = state.stats.load_full();
    let mut names: std::collections::BTreeSet<String> = snapshot
        .depths
        .ready
        .keys()
        .chain(snapshot.depths.scheduled.keys())
        .chain(snapshot.depths.inflight.keys())
        .chain(snapshot.depths.dead_letter.keys())
        .chain(snapshot.totals.keys())
        .cloned()
        .collect();
    names.extend(config.queues.iter().map(|q| q.name.clone()));

    let queues: Vec<Value> = names
        .iter()
        .map(|name| queue_json(&state, &config, name))
        .collect();
    Json(Value::Array(queues))
}

pub(super) async fn get_queue(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
) -> Json<Value> {
    let config = state.config.load();
    Json(queue_json(&state, &config, &name))
}

#[derive(Deserialize)]
pub struct PutQueueBody {
    etag: String,
    overrides: serde_json::Map<String, Value>,
}

pub(super) async fn put_queue(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
    Json(body): Json<PutQueueBody>,
) -> ApiResult<Json<Value>> {
    let _guard = state.config_write_lock.lock().await;
    let current = read_config_file(&state)?;
    check_etag(&current, &body.etag)?;

    let mut doc = parse_doc(&current)?;
    // Declares the queue even when no overrides are sent; this is how the UI
    // creates queues.
    config_edit::ensure_queue(&mut doc, &name)
        .map_err(|e| ApiError::bad_request("invalid_change", e))?;
    for (field, value) in &body.overrides {
        config_edit::upsert_queue_field(&mut doc, &name, field, value)
            .map_err(|e| ApiError::bad_request("invalid_change", e))?;
    }

    let (applied, requires_restart, etag) = validate_and_write(&state, doc).await?;
    Ok(Json(json!({
        "applied": applied,
        "requires_restart": requires_restart,
        "etag": etag,
    })))
}

#[derive(Deserialize)]
pub struct DeleteQueueQuery {
    purge: Option<bool>,
}

// A truncated page counts as non-empty: entries may exist past the examine cap.
async fn peek_nonempty(
    state: &AdminState,
    queue: &str,
    peek: PeekState,
) -> Result<bool, ApiError> {
    let page = state
        .storage
        .peek_keys(peek, queue.to_string(), None, 1)
        .await?;
    Ok(!page.keys.is_empty() || page.truncated)
}

pub(super) async fn delete_queue(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
    Query(query): Query<DeleteQueueQuery>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let presented = headers
        .get(header::IF_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .ok_or_else(|| {
            ApiError::bad_request(
                "if_match_required",
                "If-Match header with the config etag is required",
            )
        })?;

    let _guard = state.config_write_lock.lock().await;
    let current = read_config_file(&state)?;
    check_etag(&current, &presented)?;

    // The stats snapshot can lag the committer by 250ms; peeks answer from
    // the live indexes, so a just-enqueued job cannot be purged silently.
    if peek_nonempty(&state, &name, PeekState::Inflight).await? {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "inflight",
            "queue has in-flight jobs",
        ));
    }
    if !query.purge.unwrap_or(false) {
        for peek in [PeekState::Ready, PeekState::Scheduled, PeekState::DeadLetter] {
            if peek_nonempty(&state, &name, peek).await? {
                return Err(ApiError::new(
                    StatusCode::CONFLICT,
                    "not_empty",
                    "queue holds jobs; pass purge=true to delete them",
                ));
            }
        }
    }

    // Chunked so normal traffic interleaves between committer cycles. Runs
    // even without purge=true to clear dedup leftovers that carry no depth.
    let mut purged = 0u64;
    loop {
        let outcome = state
            .storage
            .purge_queue_chunk(name.clone(), PURGE_CHUNK)
            .await
            .map_err(|status| match status.code() {
                tonic::Code::FailedPrecondition => ApiError::new(
                    StatusCode::CONFLICT,
                    "inflight",
                    status.message().to_string(),
                ),
                _ => status.into(),
            })?;
        purged += outcome.purged;
        if !outcome.remaining {
            break;
        }
    }

    let mut etag = config_edit::sha256_hex(current.as_bytes());
    let mut doc = parse_doc(&current)?;
    if config_edit::remove_queue(&mut doc, &name) {
        let (_, _, new_etag) = validate_and_write(&state, doc).await?;
        etag = new_etag;
    }

    Ok(Json(json!({ "purged": purged, "etag": etag })))
}

// ---------------------------------------------------------------------------
// Jobs

fn custom_json(custom: &HashMap<String, PrimitiveValue>) -> Value {
    use pb::primitive_value::Value as Pv;
    let map: serde_json::Map<String, Value> = custom
        .iter()
        .map(|(k, v)| {
            let value = match &v.value {
                Some(Pv::StringValue(s)) => Value::String(s.clone()),
                Some(Pv::DoubleValue(d)) => json!(d),
                Some(Pv::IntValue(i)) => json!(i),
                Some(Pv::BoolValue(b)) => json!(b),
                None => Value::Null,
            };
            (k.clone(), value)
        })
        .collect();
    Value::Object(map)
}

fn payload_json(payload: Option<&Payload>, full: bool) -> Value {
    match payload {
        Some(p) => {
            let mut v = json!({ "encoding": p.encoding, "size_bytes": p.data.len() });
            if full || p.data.len() <= INLINE_PAYLOAD_MAX {
                v["data_b64"] = Value::String(B64.encode(&p.data));
            }
            v
        }
        None if full => json!({ "encoding": "", "size_bytes": 0, "data_b64": "" }),
        None => json!({ "encoding": "", "size_bytes": 0 }),
    }
}

fn job_json(aj: &AdminJob, full: bool) -> Value {
    let job = &aj.job;
    let mut v = json!({
        "id": job.id,
        "key_b64": B64.encode(&aj.key),
        "job_type": job.job_type,
        "priority": job.priority,
        "attempt": job.attempt,
        "max_attempts": job.max_attempts,
        "enqueued_at_ms": job.enqueued_at.as_ref().map(timestamp_to_millis).unwrap_or(0),
        "custom": custom_json(&job.custom),
        "payload": payload_json(job.payload.as_ref(), full),
    });
    if let Some(at) = job.scheduled_at.as_ref().map(timestamp_to_millis) {
        v["scheduled_at_ms"] = json!(at);
    }
    if matches!(aj.state, AdminJobState::Inflight) {
        let lease = job
            .lease_expires_at
            .as_ref()
            .map(timestamp_to_millis)
            .unwrap_or(0);
        v["lease_expires_at_ms"] = json!(lease);
    }
    v
}

fn cause_label(cause: i32) -> &'static str {
    match pb::DeadLetterCause::try_from(cause) {
        Ok(pb::DeadLetterCause::AttemptsExhausted) => "attempts_exhausted",
        Ok(pb::DeadLetterCause::Rejected) => "rejected",
        Ok(pb::DeadLetterCause::LeaseExpired) => "lease_expired",
        Ok(pb::DeadLetterCause::Admin) => "admin",
        _ => "unspecified",
    }
}

fn dead_letter_json(d: &AdminDeadLetter, full: bool) -> Value {
    let job = d.record.job.clone().unwrap_or_default();
    let mut v = json!({
        "id": job.id,
        "key_b64": B64.encode(&d.key),
        "job_type": job.job_type,
        "priority": job.priority,
        "attempt": d.record.final_attempt,
        "max_attempts": job.max_attempts,
        "enqueued_at_ms": job.enqueued_at.as_ref().map(timestamp_to_millis).unwrap_or(0),
        "failed_at_ms": d.record.failed_at.as_ref().map(timestamp_to_millis).unwrap_or(0),
        "cause": cause_label(d.record.cause),
        "custom": custom_json(&job.custom),
        "payload": payload_json(job.payload.as_ref(), full),
    });
    if let Some(at) = job.scheduled_at.as_ref().map(timestamp_to_millis) {
        v["scheduled_at_ms"] = json!(at);
    }
    if let Some(reason) = &d.record.last_reason {
        v["last_reason"] = Value::String(reason.clone());
    }
    v
}

#[derive(Deserialize)]
pub struct JobsQuery {
    state: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
}

pub(super) async fn list_jobs(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
    Query(query): Query<JobsQuery>,
) -> ApiResult<Json<Value>> {
    let peek_state = match query.state.as_deref() {
        Some("ready") => PeekState::Ready,
        Some("scheduled") => PeekState::Scheduled,
        Some("inflight") => PeekState::Inflight,
        Some("dead_letter") => PeekState::DeadLetter,
        other => {
            return Err(ApiError::bad_request(
                "invalid_argument",
                format!(
                    "state must be ready|scheduled|inflight|dead_letter, got {:?}",
                    other.unwrap_or("")
                ),
            ));
        }
    };
    let cursor = query
        .cursor
        .as_deref()
        .map(|c| decode_b64("cursor", c))
        .transpose()?;
    let limit = query.limit.unwrap_or(50).clamp(1, 100);

    let page = state
        .storage
        .peek_keys(peek_state, name.clone(), cursor, limit)
        .await?;

    let read = state.read.clone();
    let keys = page.keys;
    let jobs: Vec<Value> = resolve_blocking(move || match peek_state {
        PeekState::Ready => read
            .resolve_ready(&keys)
            .iter()
            .map(|j| job_json(j, false))
            .collect(),
        PeekState::Scheduled => read
            .resolve_scheduled(&keys)
            .iter()
            .map(|j| job_json(j, false))
            .collect(),
        PeekState::Inflight => read
            .resolve_inflight(&keys)
            .iter()
            .map(|j| job_json(j, false))
            .collect(),
        PeekState::DeadLetter => read
            .resolve_dead_letters(&keys)
            .iter()
            .map(|d| dead_letter_json(d, false))
            .collect(),
    })
    .await?;

    Ok(Json(json!({
        "jobs": jobs,
        "next_cursor": page.next_cursor.map(|c| B64.encode(c)),
        "truncated": page.truncated,
    })))
}

pub(super) async fn get_job(
    State(state): State<Arc<AdminState>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let read = state.read.clone();
    let job = resolve_blocking(move || read.get_job(&id))
        .await?
        .ok_or_else(|| ApiError::not_found("job not found"))?;
    Ok(Json(job_json(&job, true)))
}

pub(super) async fn get_dead_letter(
    State(state): State<Arc<AdminState>>,
    Path((name, key_b64)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    let key = decode_b64("key", &key_b64)?;
    if DeadLetterKey::queue(&key) != Some(name.as_str()) {
        return Err(ApiError::not_found("dead-letter record not found"));
    }

    let read = state.read.clone();
    let record = resolve_blocking(move || read.resolve_dead_letters(&[key]).pop())
        .await?
        .ok_or_else(|| ApiError::not_found("dead-letter record not found"))?;
    Ok(Json(dead_letter_json(&record, true)))
}

// ---------------------------------------------------------------------------
// Enqueue

#[derive(Deserialize)]
pub struct EnqueuePayloadBody {
    encoding: String,
    data_b64: Option<String>,
    data_text: Option<String>,
}

#[derive(Deserialize)]
pub struct EnqueueBody {
    job_type: String,
    payload: Option<EnqueuePayloadBody>,
    priority: Option<u32>,
    max_attempts: Option<u32>,
    scheduled_at_ms: Option<i64>,
    idempotency_key: Option<String>,
    custom: Option<serde_json::Map<String, Value>>,
}

fn custom_from_json(
    custom: serde_json::Map<String, Value>,
) -> Result<HashMap<String, PrimitiveValue>, ApiError> {
    use pb::primitive_value::Value as Pv;
    custom
        .into_iter()
        .map(|(key, value)| {
            let value = match value {
                Value::String(s) => Pv::StringValue(s),
                Value::Bool(b) => Pv::BoolValue(b),
                Value::Number(n) => match n.as_i64() {
                    Some(i) => Pv::IntValue(i),
                    None => Pv::DoubleValue(n.as_f64().unwrap_or(0.0)),
                },
                _ => {
                    return Err(ApiError::bad_request(
                        "invalid_argument",
                        format!("custom[{key:?}] must be a string, number, or bool"),
                    ));
                }
            };
            Ok((key, PrimitiveValue { value: Some(value) }))
        })
        .collect()
}

fn rejection_detail(rejection: &pb::JobRejection) -> String {
    use pb::job_rejection::Reason;
    match rejection
        .reason
        .as_ref()
        .expect("rejection.reason is always set at construction")
    {
        Reason::UnknownQueue(r) => format!("queue {:?} is not declared (strict mode)", r.queue),
        Reason::PayloadTooLarge(r) => {
            format!("payload is {} bytes; the limit is {}", r.actual, r.limit)
        }
        Reason::EncodingNotAllowed(r) => format!(
            "encoding {:?} is not allowed; allowed: {:?}",
            r.encoding, r.allowed
        ),
        Reason::JobTypeNotAllowed(r) => format!(
            "job_type {:?} is not allowed; allowed: {:?}",
            r.job_type, r.allowed
        ),
        Reason::CustomEntriesTooMany(r) => format!(
            "custom map has {} entries; the limit is {}",
            r.actual, r.limit
        ),
        Reason::CustomMapTooLarge(r) => {
            format!("custom map is {} bytes; the limit is {}", r.actual, r.limit)
        }
        Reason::CustomKeyTooLong(r) => format!(
            "custom key {:?} is {} bytes; the limit is {}",
            r.key, r.actual, r.limit
        ),
        Reason::QueueNameTooLong(r) => {
            format!("queue name is {} bytes; the limit is {}", r.actual, r.limit)
        }
        Reason::JobTypeNameTooLong(r) => {
            format!("job_type is {} bytes; the limit is {}", r.actual, r.limit)
        }
        Reason::IdempotencyKeyTooLong(r) => format!(
            "idempotency_key is {} bytes; the limit is {}",
            r.actual, r.limit
        ),
        Reason::ScheduledTooFar(_) => "scheduled_at exceeds the schedule horizon".to_string(),
        Reason::InvalidRequest(r) => r.message.clone(),
    }
}

pub(super) async fn enqueue_job(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
    Json(body): Json<EnqueueBody>,
) -> ApiResult<Json<Value>> {
    let payload = body
        .payload
        .map(|p| -> Result<Payload, ApiError> {
            let data = match (p.data_b64, p.data_text) {
                (Some(b64), _) => decode_b64("payload.data_b64", &b64)?,
                (None, Some(text)) => text.into_bytes(),
                (None, None) => {
                    return Err(ApiError::bad_request(
                        "invalid_argument",
                        "payload requires data_b64 or data_text",
                    ));
                }
            };
            Ok(Payload {
                data,
                encoding: p.encoding,
            })
        })
        .transpose()?;

    let req = EnqueueRequest {
        queue: name,
        job_type: body.job_type,
        payload,
        idempotency_key: body.idempotency_key,
        priority: body.priority,
        max_attempts: body.max_attempts,
        trace_context: None,
        custom: custom_from_json(body.custom.unwrap_or_default())?,
        scheduled_at: body.scheduled_at_ms.map(millis_to_timestamp),
    };

    let (strict_queues, server_limits) = {
        let cfg = state.config.load();
        (cfg.server.strict_queues, ServerLimits::from_config(&cfg))
    };
    let registry = state.registry.load();
    if let Err(rejection) = classify_enqueue(&req, &registry, strict_queues, &server_limits) {
        let detail = rejection_detail(&rejection);
        return Err(ApiError {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: "rejected",
            error: format!("job rejected: {detail}"),
            rejection: Some(json!({
                "reason": rejection_label(&rejection),
                "detail": detail,
            })),
        });
    }

    let mut responses = state.storage.enqueue(vec![req]).await?;
    let job_id = responses.pop().map(|r| r.job_id).unwrap_or_default();
    Ok(Json(json!({ "job_id": job_id })))
}

// ---------------------------------------------------------------------------
// Dead-letter bulk operations

#[derive(Deserialize)]
pub struct DeadLetterKeysBody {
    keys_b64: Vec<String>,
}

fn decode_keys(keys_b64: &[String]) -> Result<Vec<Vec<u8>>, ApiError> {
    if keys_b64.len() > DEAD_LETTER_KEYS_MAX {
        return Err(ApiError::bad_request(
            "invalid_argument",
            format!("at most {DEAD_LETTER_KEYS_MAX} keys per request"),
        ));
    }
    keys_b64.iter().map(|k| decode_b64("keys_b64", k)).collect()
}

#[derive(Deserialize)]
pub struct DeadLetterJobsBody {
    state: String,
    keys_b64: Vec<String>,
    reason: Option<String>,
}

pub(super) async fn dead_letter_jobs(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
    Json(body): Json<DeadLetterJobsBody>,
) -> ApiResult<Json<Value>> {
    let peek_state = match body.state.as_str() {
        "ready" => PeekState::Ready,
        "scheduled" => PeekState::Scheduled,
        other => {
            return Err(ApiError::bad_request(
                "invalid_argument",
                format!("state must be ready or scheduled, got {other:?}"),
            ));
        }
    };
    let keys = decode_keys(&body.keys_b64)?;
    let outcome = state
        .storage
        .dead_letter_jobs(name, peek_state, keys, body.reason)
        .await?;
    Ok(Json(json!({
        "dead_lettered": outcome.dead_lettered,
        "missing": outcome.missing,
    })))
}

pub(super) async fn requeue_dead_letters(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
    Json(body): Json<DeadLetterKeysBody>,
) -> ApiResult<Json<Value>> {
    let keys = decode_keys(&body.keys_b64)?;
    let outcome = state.storage.requeue_dead_letters(name, keys).await?;
    Ok(Json(
        json!({ "requeued": outcome.requeued, "missing": outcome.missing }),
    ))
}

pub(super) async fn delete_dead_letters(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
    Json(body): Json<DeadLetterKeysBody>,
) -> ApiResult<Json<Value>> {
    let keys = decode_keys(&body.keys_b64)?;
    let outcome = state.storage.delete_dead_letters(name, keys).await?;
    Ok(Json(
        json!({ "deleted": outcome.deleted, "missing": outcome.missing }),
    ))
}

// ---------------------------------------------------------------------------
// Config

fn env_pinned() -> Vec<String> {
    let mut paths: Vec<String> = std::env::vars()
        .filter(|(k, _)| k != "SEPP_CONFIG")
        .filter_map(|(k, _)| {
            k.strip_prefix("SEPP_")
                .map(|rest| rest.to_lowercase().replace("__", "."))
        })
        .collect();
    paths.sort();
    paths
}

fn read_config_file(state: &AdminState) -> Result<String, ApiError> {
    config_edit::read_file(&state.config_path)
        .map_err(|e| ApiError::internal(format!("reading config file: {e}")))
}

fn check_etag(current: &str, presented: &str) -> Result<(), ApiError> {
    if config_edit::sha256_hex(current.as_bytes()) != presented {
        return Err(ApiError::new(
            StatusCode::PRECONDITION_FAILED,
            "etag_mismatch",
            "config file changed; reload and retry",
        ));
    }
    Ok(())
}

fn parse_doc(current: &str) -> Result<DocumentMut, ApiError> {
    current.parse().map_err(|e| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid",
            format!("config file is not valid TOML: {e}"),
        )
    })
}

// Validates the candidate, writes it atomically, and waits for the watcher to
// observe the change. Caller holds config_write_lock.
async fn validate_and_write(
    state: &AdminState,
    doc: DocumentMut,
) -> Result<(bool, Vec<&'static str>, String), ApiError> {
    let candidate_text = doc.to_string();
    let candidate = Config::from_toml_str(&candidate_text).map_err(|e| {
        ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "invalid", e.to_string())
    })?;
    let running = state.config.load_full();
    let requires_restart = config_watch::restart_only_changes(&running, &candidate);

    let mut rx = state.reload_seq.clone();
    let pre = *rx.borrow_and_update();
    config_edit::write_atomic(&state.config_path, &candidate_text)
        .map_err(|e| ApiError::internal(format!("writing config file: {e}")))?;

    // Bumped even for failed or no-op reloads, so this only times out when no
    // watcher is running (e.g. the file did not exist at startup).
    let applied = tokio::time::timeout(RELOAD_WAIT, async move {
        loop {
            if *rx.borrow_and_update() > pre {
                return true;
            }
            if rx.changed().await.is_err() {
                return false;
            }
        }
    })
    .await
    .unwrap_or(false);

    Ok((
        applied,
        requires_restart,
        config_edit::sha256_hex(candidate_text.as_bytes()),
    ))
}

pub(super) async fn get_config(State(state): State<Arc<AdminState>>) -> ApiResult<Json<Value>> {
    let config = state.config.load();
    let effective = serde_json::to_value(&**config)
        .map_err(|e| ApiError::internal(format!("serializing config: {e}")))?;
    let etag = config_edit::file_etag(&state.config_path)
        .map_err(|e| ApiError::internal(format!("hashing config file: {e}")))?;

    // Worker API keys are served unredacted: the admin plane is loopback-only
    // and PUT /config already lets any local caller rewrite them, so hiding
    // them on read protects nothing while blocking legitimate key management.
    Ok(Json(json!({
        "effective": effective,
        "etag": etag,
        "env_pinned": env_pinned(),
        "restart_only": RESTART_ONLY,
        // Restart-only fields whose on-disk value drifted from the running
        // (boot) value; they apply on the next restart.
        "pending_restart": config_watch::restart_only_changes(&state.boot, &config),
    })))
}

#[derive(Deserialize)]
pub struct ConfigChange {
    path: String,
    #[serde(default)]
    value: Value,
}

#[derive(Deserialize)]
pub struct PutConfigBody {
    etag: String,
    changes: Vec<ConfigChange>,
}

pub(super) async fn put_config(
    State(state): State<Arc<AdminState>>,
    Json(body): Json<PutConfigBody>,
) -> ApiResult<Json<Value>> {
    let _guard = state.config_write_lock.lock().await;
    let current = read_config_file(&state)?;
    check_etag(&current, &body.etag)?;

    let pinned = env_pinned();
    if let Some(change) = body.changes.iter().find(|c| pinned.contains(&c.path)) {
        return Err(ApiError::bad_request(
            "env_pinned",
            format!("{} is pinned by an environment variable", change.path),
        ));
    }

    let mut doc = parse_doc(&current)?;
    for change in &body.changes {
        config_edit::apply_change(&mut doc, &change.path, &change.value)
            .map_err(|e| ApiError::bad_request("invalid_change", e))?;
    }

    let (applied, requires_restart, etag) = validate_and_write(&state, doc).await?;
    Ok(Json(json!({
        "applied": applied,
        "requires_restart": requires_restart,
        "etag": etag,
    })))
}

