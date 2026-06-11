use prost_types::Duration;

use crate::pb::sepp::v1::{
    AckRequest, DrainDeadLettersRequest, EnqueueRequest, ExtendRequest, NackRequest, Payload,
    PrimitiveValue, ReserveRequest, TraceContext, nack_retry,
};

type Check = Result<(), String>;

pub fn enqueue_request(req: &EnqueueRequest) -> Check {
    queue_name("queue", &req.queue)?;
    min_len("job_type", &req.job_type)?;
    if let Some(p) = &req.payload {
        payload(p)?;
    }
    if let Some(k) = &req.idempotency_key {
        min_len("idempotency_key", k)?;
    }
    if let Some(p) = req.priority {
        lte("priority", p, 9)?;
    }
    if let Some(m) = req.max_attempts {
        gte("max_attempts", m, 1)?;
    }
    if let Some(tc) = &req.trace_context {
        trace_context(tc)?;
    }
    for (key, value) in &req.custom {
        min_len("custom key", key)?;
        primitive_value(key, value)?;
    }
    Ok(())
}

pub fn reserve_request(req: &ReserveRequest) -> Check {
    if req.queues.is_empty() {
        return Err("queues must contain at least 1 item".into());
    }
    for q in &req.queues {
        queue_name("queues item", q)?;
    }
    if let Some(d) = &req.wait_timeout {
        non_negative("wait_timeout", d)?;
    }
    match &req.lease_duration {
        Some(d) => positive("lease_duration", d)?,
        None => return Err("lease_duration is required".into()),
    }
    if let Some(w) = &req.worker_id {
        min_len("worker_id", w)?;
    }
    if let Some(m) = req.max_jobs {
        gte("max_jobs", m, 1)?;
    }
    Ok(())
}

pub fn ack_request(req: &AckRequest) -> Check {
    uuid("job_id", &req.job_id)?;
    gte("attempt", req.attempt, 1)?;
    if let Some(w) = &req.worker_id {
        min_len("worker_id", w)?;
    }
    Ok(())
}

pub fn nack_request(req: &NackRequest) -> Check {
    uuid("job_id", &req.job_id)?;
    gte("attempt", req.attempt, 1)?;
    if let Some(retry) = &req.retry {
        match &retry.strategy {
            None => return Err("retry strategy is required".into()),
            Some(nack_retry::Strategy::Delay(d)) => non_negative("retry delay", d)?,
            Some(_) => {}
        }
    }
    if let Some(w) = &req.worker_id {
        min_len("worker_id", w)?;
    }
    Ok(())
}

pub fn extend_request(req: &ExtendRequest) -> Check {
    uuid("job_id", &req.job_id)?;
    gte("attempt", req.attempt, 1)?;
    match &req.lease_duration {
        Some(d) => positive("lease_duration", d)?,
        None => return Err("lease_duration is required".into()),
    }
    if let Some(w) = &req.worker_id {
        min_len("worker_id", w)?;
    }
    Ok(())
}

pub fn drain_dead_letters_request(req: &DrainDeadLettersRequest) -> Check {
    if let Some(q) = &req.queue {
        min_len("queue", q)?;
    }
    if let Some(m) = req.max {
        gte("max", m, 1)?;
    }
    Ok(())
}

fn payload(p: &Payload) -> Check {
    if p.data.is_empty() {
        return Err("payload.data must not be empty".into());
    }
    min_len("payload.encoding", &p.encoding)
}

fn trace_context(tc: &TraceContext) -> Check {
    min_len("trace_context.traceparent", &tc.traceparent)
}

// The map's value type carries `oneof value { ... } [required]`.
fn primitive_value(key: &str, v: &PrimitiveValue) -> Check {
    if v.value.is_none() {
        return Err(format!("custom[{key}] must set a value"));
    }
    Ok(())
}

fn min_len(field: &str, s: &str) -> Check {
    if s.is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    Ok(())
}

// Shared queue-name validity, enforced on both the gRPC request path (here) and
// in config validation, so a name the gRPC plane auto-creates is always one the
// admin REST API and operators can address. Minimal reject set: empty, "."/"..",
// any name containing '/' (breaks the admin REST path), or control characters.
// Length is bounded separately (max_queue_name_bytes -> QueueNameTooLong).
pub(crate) fn queue_name_error(name: &str) -> Option<&'static str> {
    if name.is_empty() {
        Some("must not be empty")
    } else if name == "." || name == ".." {
        Some("must not be \".\" or \"..\"")
    } else if name.contains('/') {
        Some("must not contain '/'")
    } else if name.chars().any(char::is_control) {
        Some("must not contain control characters")
    } else {
        None
    }
}

fn queue_name(field: &str, name: &str) -> Check {
    match queue_name_error(name) {
        Some(why) => Err(format!("{field} {why}")),
        None => Ok(()),
    }
}

fn lte(field: &str, value: u32, max: u32) -> Check {
    if value > max {
        return Err(format!("{field} must be <= {max}"));
    }
    Ok(())
}

fn gte(field: &str, value: u32, min: u32) -> Check {
    if value < min {
        return Err(format!("{field} must be >= {min}"));
    }
    Ok(())
}

// `duration.gte = {}`: not negative.
fn non_negative(field: &str, d: &Duration) -> Check {
    if total_nanos(d) < 0 {
        return Err(format!("{field} must not be negative"));
    }
    Ok(())
}

// `duration.gt = {}`: strictly positive.
fn positive(field: &str, d: &Duration) -> Check {
    if total_nanos(d) <= 0 {
        return Err(format!("{field} must be greater than 0"));
    }
    Ok(())
}

fn total_nanos(d: &Duration) -> i128 {
    d.seconds as i128 * 1_000_000_000 + d.nanos as i128
}

fn uuid(field: &str, s: &str) -> Check {
    if !is_uuid(s) {
        return Err(format!("{field} must be a UUID"));
    }
    Ok(())
}

// Canonical hyphenated form, case-insensitive (matches protovalidate's `string.uuid`).
fn is_uuid(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 36 {
        return false;
    }
    b.iter().enumerate().all(|(i, c)| match i {
        8 | 13 | 18 | 23 => *c == b'-',
        _ => c.is_ascii_hexdigit(),
    })
}
