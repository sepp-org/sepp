use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
};

use prost::Message;
use tonic::Status;
use tracing::{debug, error, warn};

use crate::config::RetryBackoff;
use crate::keys::{
    AUDIT_SEQ_KEY, AuditValue, DeadLetterKey, DedupKey, DedupTimerKey, DedupValue, Inflight,
    JobValue, ReadyKey, TimerKey, closing_key, queue_prefix,
};
use crate::op::{Op, PreparedJob};
use crate::pb::sepp::storage::v1::AuditRecord;
use crate::pb::sepp::v1::{
    DeadLetterCause, DeadLetterRecord, EnqueueRequest, EnqueueResponse, ExtendRequest, Job,
    JobRejection, NackRequest, Payload, QueueClosing, QueueFull, job_rejection, nack_retry,
};
use crate::pb::{millis_to_timestamp, timestamp_to_millis};
use crate::queues::RetryPolicy;
use crate::telemetry;

use super::*;

pub(crate) fn apply_op(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    op: Op,
) -> Result<OpOutcome, Status> {
    Ok(match op {
        Op::Enqueue { jobs, now_ms } => {
            OpOutcome::Enqueue(apply_enqueue(store, indexes, tx, cycle, jobs, now_ms)?)
        }
        Op::EnqueueAtomic { jobs, now_ms } => OpOutcome::EnqueueAtomic(apply_enqueue_atomic(
            store, indexes, tx, cycle, jobs, now_ms,
        )?),
        Op::Reserve {
            queues,
            lease_ms,
            max_jobs,
            now_ms,
        } => OpOutcome::Reserve(apply_reserve(
            store, indexes, tx, cycle, &queues, lease_ms, max_jobs, now_ms,
        )?),
        Op::Ack { job_id, attempt } => {
            OpOutcome::Ack(apply_ack(store, indexes, tx, cycle, &job_id, attempt)?)
        }
        Op::Nack {
            req,
            retry_delay_ms,
            dead_letter_enabled,
            now_ms,
        } => OpOutcome::Nack(apply_nack(
            store,
            indexes,
            tx,
            cycle,
            req,
            retry_delay_ms,
            dead_letter_enabled,
            now_ms,
        )?),
        Op::Extend {
            req,
            lease_ms,
            now_ms,
        } => OpOutcome::Extend(apply_extend(
            store, indexes, tx, cycle, req, lease_ms, now_ms,
        )?),
        Op::DrainDeadLetters {
            queue,
            max,
            scan_cap,
        } => OpOutcome::DrainDeadLetters(apply_drain(
            store, indexes, tx, cycle, queue, max, scan_cap,
        )?),
        Op::CloseQueue {
            queue,
            now_ms,
            grace_ms,
        } => {
            apply_close_queue(store, indexes, tx, cycle, queue, now_ms, grace_ms);
            OpOutcome::CloseQueue
        }
        Op::OpenQueue { queue } => {
            apply_open_queue(store, indexes, tx, cycle, &queue);
            OpOutcome::OpenQueue
        }
        Op::RequeueDeadLetters {
            queue,
            keys,
            now_ms,
        } => OpOutcome::RequeueDeadLetters(apply_requeue_dead_letters(
            store, indexes, tx, cycle, &queue, keys, now_ms,
        )?),
        Op::DeadLetterJobs {
            queue,
            state,
            keys,
            reason,
            dead_letter_enabled,
            now_ms,
        } => OpOutcome::DeadLetterJobs(apply_dead_letter_jobs(
            store,
            indexes,
            tx,
            cycle,
            &queue,
            state,
            keys,
            reason,
            dead_letter_enabled,
            now_ms,
        )?),
        Op::DeleteDeadLetters { queue, keys } => OpOutcome::DeleteDeadLetters(
            apply_delete_dead_letters(store, indexes, tx, cycle, &queue, keys),
        ),
        Op::PurgeQueueChunk { queue, max } => OpOutcome::PurgeQueueChunk(apply_purge_queue_chunk(
            store, indexes, tx, cycle, &queue, max,
        )?),
        Op::Sweep {
            now_ms,
            budget,
            retention_cutoff_ms,
            dead_letter_enabled,
        } => OpOutcome::Sweep(apply_sweep(
            store,
            indexes,
            tx,
            cycle,
            now_ms,
            budget,
            retention_cutoff_ms,
            dead_letter_enabled,
        )?),
        Op::AuditAppend { record, now_ms } => {
            let seq = apply_audit_append(store, tx, cycle, &record, now_ms)?;
            OpOutcome::AuditAppend { seq, ts_ms: now_ms }
        }
    })
}
pub(crate) fn queue_full(queue: &str, cap: u64) -> JobRejection {
    JobRejection {
        reason: Some(job_rejection::Reason::QueueFull(QueueFull {
            queue: queue.to_string(),
            limit: cap,
        })),
    }
}

pub(crate) fn queue_closing(queue: &str) -> JobRejection {
    JobRejection {
        reason: Some(job_rejection::Reason::QueueClosing(QueueClosing {
            queue: queue.to_string(),
        })),
    }
}

pub(crate) enum DedupCheck {
    Hit(EnqueueResponse),
    Miss { stale_timer: Option<Vec<u8>> },
}

pub(crate) fn check_dedup(
    store: &Store,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    req: &EnqueueRequest,
    now: i64,
) -> Result<DedupCheck, Status> {
    let mut stale_timer = None;

    if let Some(key) = &req.idempotency_key {
        let dkey = DedupKey {
            queue: &req.queue,
            idempotency_key: key,
        }
        .encode();

        if let Some(existing) = tx.get(&store.dedup, &dkey).map_err(stg_err)? {
            match DedupValue::decode(&existing) {
                Some(dv) if now < dv.deadline => {
                    cycle.deduplicated(&req.queue);
                    return Ok(DedupCheck::Hit(EnqueueResponse {
                        job_id: dv.job_id.to_owned(),
                        deduplicated: true,
                    }));
                }
                Some(dv) => {
                    stale_timer = Some(
                        DedupTimerKey {
                            deadline: dv.deadline,
                            dedup_key: &dkey,
                        }
                        .encode(),
                    );
                }
                None => {}
            }
        }
    }

    Ok(DedupCheck::Miss { stale_timer })
}

pub(crate) fn apply_enqueue(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    jobs: Vec<PreparedJob>,
    now: i64,
) -> Result<Vec<EnqueueResult>, Status> {
    let mut results = Vec::with_capacity(jobs.len());

    for job in jobs {
        let queue = &job.req.queue;
        // Checked before dedup so a hit can't hand back a job_id that is about
        // to be purged.
        if indexes
            .closing
            .get(queue)
            .is_some_and(|&deadline| deadline > now)
        {
            results.push(Err(queue_closing(queue)));
            continue;
        }

        match check_dedup(store, tx, cycle, &job.req, now)? {
            DedupCheck::Hit(resp) => results.push(Ok(resp)),
            DedupCheck::Miss { stale_timer } => {
                // live_depth is read from the in-memory indexes, which
                // insert_job bumps immediately, so jobs admitted earlier in
                // this batch already count against the cap. Dedup hits never
                // get here and so never count.
                if let Some(cap) = job.limits.max_queue_depth
                    && indexes.live_depth(queue) >= cap
                {
                    results.push(Err(queue_full(queue, cap)));
                    continue;
                }
                results.push(Ok(insert_job(
                    store,
                    indexes,
                    tx,
                    cycle,
                    job,
                    now,
                    stale_timer,
                )));
            }
        }
    }

    Ok(results)
}

pub(crate) fn apply_enqueue_atomic(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    jobs: Vec<PreparedJob>,
    now: i64,
) -> Result<AtomicEnqueueOutcome, Status> {
    // Atomic = all-or-nothing: if any job targets a queue being deleted, reject
    // the whole batch (mirrors the per-job QueueClosing in best-effort enqueue).
    let closing: Vec<(u32, JobRejection)> = jobs
        .iter()
        .enumerate()
        .filter(|(_, job)| {
            indexes
                .closing
                .get(&job.req.queue)
                .is_some_and(|&d| d > now)
        })
        .map(|(index, job)| (index as u32, queue_closing(&job.req.queue)))
        .collect();
    if !closing.is_empty() {
        return Ok(AtomicEnqueueOutcome::Rejected(closing));
    }

    // Capacity is checked for the whole batch up front so a full queue commits
    // nothing. Dedup hits are conservatively counted as new jobs here. Jobs in
    // the same queue carry the same cap (resolved under one registry snapshot
    // at propose time), so the first job seen speaks for its queue.
    let mut wanted: HashMap<&str, (u64, Option<u64>)> = HashMap::new();
    for job in &jobs {
        wanted
            .entry(job.req.queue.as_str())
            .or_insert((0, job.limits.max_queue_depth))
            .0 += 1;
    }

    let mut full: HashMap<&str, u64> = HashMap::new();
    for (queue, (count, cap)) in wanted {
        if let Some(cap) = cap
            && indexes.live_depth(queue) + count > cap
        {
            full.insert(queue, cap);
        }
    }

    if !full.is_empty() {
        let errors = jobs
            .iter()
            .enumerate()
            .filter_map(|(index, job)| {
                full.get(job.req.queue.as_str())
                    .map(|cap| (index as u32, queue_full(&job.req.queue, *cap)))
            })
            .collect();
        return Ok(AtomicEnqueueOutcome::Rejected(errors));
    }

    let mut responses = Vec::with_capacity(jobs.len());
    for job in jobs {
        responses.push(match check_dedup(store, tx, cycle, &job.req, now)? {
            DedupCheck::Hit(resp) => resp,
            DedupCheck::Miss { stale_timer } => {
                insert_job(store, indexes, tx, cycle, job, now, stale_timer)
            }
        });
    }

    Ok(AtomicEnqueueOutcome::Committed(responses))
}

pub(crate) fn insert_job(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    job: PreparedJob,
    now: i64,
    stale_dedup_timer: Option<Vec<u8>>,
) -> EnqueueResponse {
    let PreparedJob { id, req, limits } = job;
    let queue = req.queue;
    let payload = req.payload;

    let scheduled_at_ms = req.scheduled_at.as_ref().map(timestamp_to_millis);
    let job = Job {
        id: id.clone(),
        job_type: req.job_type,
        payload: None,
        priority: limits.priority,
        trace_context: req.trace_context,
        enqueued_at: Some(millis_to_timestamp(now)),
        attempt: 1,
        max_attempts: limits.max_attempts,
        // Not yet leased; a real lease is stamped at reserve time.
        lease_expires_at: Some(millis_to_timestamp(0)),
        custom: req.custom,
        scheduled_at: req.scheduled_at,
        queue: String::new(),
    };

    tx.insert(
        &store.jobs,
        id.clone().into_bytes(),
        JobValue {
            queue: &queue,
            job: &job,
        }
        .encode(),
    );

    if let Some(payload) = payload {
        tx.insert(
            &store.payloads,
            id.clone().into_bytes(),
            payload.encode_to_vec(),
        );
    }

    match scheduled_at_ms {
        Some(at) if at > now => {
            let tk = TimerKey {
                deadline: at,
                job_id: &id,
            }
            .encode();
            tx.insert(
                &store.scheduled,
                tk.clone(),
                job.attempt.to_be_bytes().to_vec(),
            );
            indexes.scheduled.insert(tk, &queue);
        }
        _ => {
            let rk = ReadyKey {
                queue: &queue,
                priority: job.priority,
                enqueued_at: now,
                job_id: &id,
            }
            .encode();

            tx.insert(&store.ready, rk.clone(), job.attempt.to_be_bytes().to_vec());
            indexes.ready.insert(rk, job.attempt);
            cycle.new_ready.insert(queue.clone());
        }
    }

    if let Some(key) = &req.idempotency_key {
        let dkey = DedupKey {
            queue: &queue,
            idempotency_key: key,
        }
        .encode();
        if let Some(old_timer) = stale_dedup_timer {
            tx.remove(&store.dedup_timers, old_timer.clone());
            indexes.dedup_timers.remove(&old_timer);
        }

        let deadline = now.saturating_add(limits.dedup_window_ms);
        let dtk = DedupTimerKey {
            deadline,
            dedup_key: &dkey,
        }
        .encode();
        tx.insert(&store.dedup_timers, dtk.clone(), Vec::new());
        indexes.dedup_timers.insert(dtk, &queue);
        tx.insert(
            &store.dedup,
            dkey,
            DedupValue {
                enqueued_at: now,
                deadline,
                job_id: &id,
            }
            .encode(),
        );
    }

    cycle.enqueued(&queue);
    cycle.dirty = true;

    EnqueueResponse {
        job_id: id,
        deduplicated: false,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_reserve(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    queues: &[String],
    lease_ms: u64,
    max_jobs: usize,
    now: i64,
) -> Result<Vec<Job>, Status> {
    let lease_expires_at = now.saturating_add(i64::try_from(lease_ms).unwrap_or(i64::MAX));
    let mut jobs = Vec::new();

    for queue in queues {
        if jobs.len() >= max_jobs {
            break;
        }

        let prefix = queue_prefix(queue);
        while jobs.len() < max_jobs {
            let Some((ready_k, attempt)) = indexes.ready.pop_front(&prefix) else {
                break;
            };

            let job_id: String = match ReadyKey::decode(&ready_k) {
                Some(rk) => rk.job_id.to_owned(),
                None => {
                    error!(queue = %queue, "corrupt ready-index entry; removing it");
                    tx.remove(&store.ready, ready_k);
                    cycle.dirty = true;
                    continue;
                }
            };

            let stored = match tx.get(&store.jobs, job_id.as_bytes()) {
                Ok(Some(stored)) => stored,
                Ok(None) => {
                    tx.remove(&store.ready, ready_k);
                    cycle.dirty = true;
                    continue;
                }
                // No partial batch on a read error: the op's committed effects
                // must be a function of state and op, not of transient I/O
                // failures. Poisoning the cycle rolls everything back.
                Err(e) => return Err(stg_err(e)),
            };

            let (job_queue, mut job) = match JobValue::decode(&stored) {
                Ok(decoded) => decoded,
                Err(e) => {
                    error!(
                        job_id = %job_id,
                        queue = %queue,
                        error = %e,
                        "corrupt job record encountered during reserve; deleting it PERMANENTLY",
                    );
                    tx.remove(&store.ready, ready_k);
                    tx.remove(&store.jobs, job_id.as_bytes().to_vec());
                    tx.remove(&store.payloads, job_id.as_bytes().to_vec());
                    cycle.dirty = true;
                    continue;
                }
            };
            job.queue = job_queue.clone();

            match tx.get(&store.payloads, job_id.as_bytes()) {
                Ok(Some(bytes)) => match Payload::decode(&*bytes) {
                    Ok(payload) => job.payload = Some(payload),
                    Err(e) => {
                        error!(
                            job_id = %job_id,
                            queue = %job_queue,
                            error = %e,
                            "corrupt payload encountered during reserve; deleting the job PERMANENTLY",
                        );
                        tx.remove(&store.ready, ready_k);
                        tx.remove(&store.jobs, job_id.as_bytes().to_vec());
                        tx.remove(&store.payloads, job_id.as_bytes().to_vec());
                        cycle.dirty = true;
                        continue;
                    }
                },
                Ok(None) => {}
                Err(e) => return Err(stg_err(e)),
            }

            let enqueued_at_ms = job
                .enqueued_at
                .as_ref()
                .map(timestamp_to_millis)
                .unwrap_or(0);
            job.attempt = attempt;
            job.lease_expires_at = Some(millis_to_timestamp(lease_expires_at));

            let inflight = Inflight {
                attempt,
                lease_expires_at,
                enqueued_at: enqueued_at_ms,
                priority: job.priority,
                max_attempts: job.max_attempts,
                queue: job_queue,
                trace_context: job.trace_context.clone(),
            };
            tx.remove(&store.ready, ready_k);
            tx.insert(
                &store.inflight,
                job.id.clone().into_bytes(),
                inflight.encode(),
            );

            let lease_timer = TimerKey {
                deadline: lease_expires_at,
                job_id: &job.id,
            }
            .encode();
            tx.insert(&store.leases, lease_timer.clone(), Vec::new());
            indexes.leases.insert(lease_timer, &inflight.queue);
            cycle.reserved(&inflight.queue);
            cycle.dirty = true;
            jobs.push(job);
        }
    }

    Ok(jobs)
}

pub(crate) fn apply_ack(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    job_id: &str,
    attempt: u32,
) -> Result<AckOutcome, Status> {
    let stored = tx
        .get(&store.inflight, job_id.as_bytes())
        .map_err(stg_err)?
        .ok_or_else(|| Status::not_found("job not found"))?;

    let inflight = Inflight::decode(&stored)?;
    if inflight.attempt != attempt {
        return Err(Status::failed_precondition("attempt mismatch"));
    }

    tx.remove(&store.jobs, job_id.as_bytes().to_vec());
    tx.remove(&store.payloads, job_id.as_bytes().to_vec());
    tx.remove(&store.inflight, job_id.as_bytes().to_vec());

    let lease_timer = TimerKey {
        deadline: inflight.lease_expires_at,
        job_id,
    }
    .encode();
    tx.remove(&store.leases, lease_timer.clone());
    indexes.leases.remove(&lease_timer);
    cycle.acked(&inflight.queue);
    cycle.dirty = true;

    Ok(AckOutcome {
        queue: inflight.queue,
        trace_context: inflight.trace_context,
    })
}

pub(crate) fn read_dead_letter_job(
    store: &Store,
    tx: &mut ApplyTx<'_>,
    job_id: &[u8],
) -> Result<Option<Job>, Status> {
    let Some(stored) = tx.get(&store.jobs, job_id).map_err(stg_err)? else {
        return Ok(None);
    };

    let (queue, mut job) = match JobValue::decode(&stored) {
        Ok(decoded) => decoded,
        Err(e) => {
            warn!(error = %e, "dead-letter: skipping record for corrupt job");
            return Ok(None);
        }
    };

    if let Some(bytes) = tx.get(&store.payloads, job_id).map_err(stg_err)? {
        match Payload::decode(&*bytes) {
            Ok(payload) => job.payload = Some(payload),
            Err(e) => warn!(error = %e, "dead-letter: dropping corrupt payload from record"),
        }
    }

    job.queue = queue;
    Ok(Some(job))
}

pub(crate) struct DeadLetterMeta {
    cause: DeadLetterCause,
    failed_at: i64,
    attempt: u32,
    last_reason: Option<String>,
}

// Stores the job in the DLQ, or drops it when the op says retention is
// disabled.
pub(crate) fn maybe_store_dead_letter(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    job_id: &[u8],
    meta: DeadLetterMeta,
    dead_letter_enabled: bool,
) -> Result<(), Status> {
    if !dead_letter_enabled {
        return Ok(());
    }

    let Some(mut job) = read_dead_letter_job(store, tx, job_id)? else {
        return Ok(());
    };

    job.attempt = meta.attempt;
    let key = DeadLetterKey {
        failed_at: meta.failed_at,
        queue: &job.queue,
        job_id,
    }
    .encode();
    indexes.dead_letter.insert(key.clone(), &job.queue);

    let record = DeadLetterRecord {
        job: Some(job),
        cause: meta.cause as i32,
        failed_at: Some(millis_to_timestamp(meta.failed_at)),
        final_attempt: meta.attempt,
        last_reason: meta.last_reason,
    };

    tx.insert(&store.dead_letter, key, record.encode_to_vec());
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_drain(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    queue: Option<String>,
    max: usize,
    scan_cap: usize,
) -> Result<Vec<DeadLetterRecord>, Status> {
    let mut chosen: Vec<Vec<u8>> = Vec::new();
    for (examined, (key, q)) in indexes.dead_letter.iter_oldest().enumerate() {
        if chosen.len() >= max || examined >= scan_cap {
            break;
        }

        if queue.as_deref().is_some_and(|want| want != q) {
            continue;
        }

        chosen.push(key.to_vec());
    }

    let mut records = Vec::with_capacity(chosen.len());
    for key in &chosen {
        match tx.get(&store.dead_letter, key).map_err(stg_err)? {
            Some(value) => match DeadLetterRecord::decode(&*value) {
                Ok(record) => {
                    records.push(record);
                    indexes.dead_letter.remove(key);
                    tx.remove(&store.dead_letter, key.clone());
                }
                Err(e) => {
                    warn!(error = %e, "drain leaving corrupt dead-letter record for retention");
                }
            },
            None => {
                indexes.dead_letter.remove(key);
            }
        }
    }

    if !records.is_empty() {
        cycle.dead_letter_drained(records.len() as u64);
        cycle.dirty = true;
    }

    Ok(records)
}

pub(crate) fn peek_keys(
    indexes: &Indexes,
    state: PeekState,
    queue: &str,
    cursor: Option<Vec<u8>>,
    limit: usize,
) -> PeekPage {
    use std::ops::Bound::{Excluded, Included, Unbounded};

    let limit = limit.clamp(1, PEEK_LIMIT_MAX);

    // Ready keys embed the queue, so a prefix range lands exactly on the
    // queue's entries and never needs an examined cap.
    if let PeekState::Ready = state {
        let prefix = queue_prefix(queue);
        let start = match cursor {
            Some(c) => Excluded(c),
            None => Included(prefix.clone()),
        };
        let mut range = indexes
            .ready
            .keys
            .range((start, Unbounded))
            .map(|(k, _)| k)
            .take_while(|k| k.starts_with(&prefix));
        let keys: Vec<Vec<u8>> = range.by_ref().take(limit).cloned().collect();
        let next_cursor = (keys.len() == limit && range.next().is_some())
            .then(|| keys.last().cloned())
            .flatten();

        return PeekPage {
            keys,
            next_cursor,
            truncated: false,
        };
    }

    let index = match state {
        PeekState::Scheduled => &indexes.scheduled,
        PeekState::Inflight => &indexes.leases,
        PeekState::DeadLetter => &indexes.dead_letter,
        PeekState::Ready => unreachable!("handled above"),
    };
    let start = match cursor {
        Some(c) => Excluded(c),
        None => Unbounded,
    };

    let mut keys = Vec::new();
    let mut last_examined: Option<&Vec<u8>> = None;
    for (examined, (key, owner)) in index.keys.range((start, Unbounded)).enumerate() {
        if examined == PEEK_EXAMINE_CAP {
            return PeekPage {
                keys,
                next_cursor: last_examined.cloned(),
                truncated: true,
            };
        }

        if owner == queue {
            keys.push(key.clone());
            if keys.len() == limit {
                return PeekPage {
                    next_cursor: Some(key.clone()),
                    keys,
                    truncated: false,
                };
            }
        }
        last_examined = Some(key);
    }

    PeekPage {
        keys,
        next_cursor: None,
        truncated: false,
    }
}

pub(crate) fn apply_requeue_dead_letters(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    queue: &str,
    keys: Vec<Vec<u8>>,
    now: i64,
) -> Result<RequeueOutcome, Status> {
    let mut requeued = 0u64;
    let mut missing = 0u64;
    let mut job_ids = Vec::new();

    for key in keys {
        if DeadLetterKey::queue(&key) != Some(queue) {
            missing += 1;
            continue;
        }

        let Some(stored) = tx.get(&store.dead_letter, &key).map_err(stg_err)? else {
            indexes.dead_letter.remove(&key);
            missing += 1;
            continue;
        };

        let record = match DeadLetterRecord::decode(&*stored) {
            Ok(record) => record,
            Err(e) => {
                warn!(error = %e, "requeue leaving corrupt dead-letter record in place");
                missing += 1;
                continue;
            }
        };
        let Some(mut job) = record.job else {
            warn!("requeue leaving dead-letter record without a job in place");
            missing += 1;
            continue;
        };

        // Mirror apply_enqueue: the record's job carries queue and payload
        // inline (see maybe_store_dead_letter), but jobs/payloads store them
        // separately and the persisted Job leaves both fields empty.
        let job_queue = std::mem::take(&mut job.queue);
        let payload = job.payload.take();
        job.attempt = 1;
        job.enqueued_at = Some(millis_to_timestamp(now));
        job.lease_expires_at = Some(millis_to_timestamp(0));
        job.scheduled_at = None;

        tx.insert(
            &store.jobs,
            job.id.clone().into_bytes(),
            JobValue {
                queue: &job_queue,
                job: &job,
            }
            .encode(),
        );
        if let Some(payload) = payload {
            tx.insert(
                &store.payloads,
                job.id.clone().into_bytes(),
                payload.encode_to_vec(),
            );
        }

        let rk = ReadyKey {
            queue: &job_queue,
            priority: job.priority,
            enqueued_at: now,
            job_id: &job.id,
        }
        .encode();
        tx.insert(&store.ready, rk.clone(), job.attempt.to_be_bytes().to_vec());
        indexes.ready.insert(rk, job.attempt);

        tx.remove(&store.dead_letter, key.clone());
        indexes.dead_letter.remove(&key);
        cycle.new_ready.insert(job_queue);
        cycle.dirty = true;
        requeued += 1;
        job_ids.push(job.id);
    }

    Ok(RequeueOutcome {
        requeued,
        missing,
        job_ids,
    })
}

// The reverse of apply_requeue_dead_letters: moves live ready/scheduled jobs
// into the dead-letter queue (or drops them when retention is disabled),
// exactly as a nack with the dead_letter strategy would.
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_dead_letter_jobs(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    queue: &str,
    state: PeekState,
    keys: Vec<Vec<u8>>,
    reason: Option<String>,
    dead_letter_enabled: bool,
    now: i64,
) -> Result<DeadLetterJobsOutcome, Status> {
    if !matches!(state, PeekState::Ready | PeekState::Scheduled) {
        return Err(Status::invalid_argument(
            "only ready or scheduled jobs can be dead-lettered",
        ));
    }

    let mut dead_lettered = 0u64;
    let mut missing = 0u64;
    let mut job_ids = Vec::new();

    for key in keys {
        // Peeked keys can be consumed (reserved, promoted, purged) between
        // peek and this command; gone keys count as missing, not errors.
        let job_id: Vec<u8> = match state {
            PeekState::Ready => {
                let Some(rk) = ReadyKey::decode(&key) else {
                    missing += 1;
                    continue;
                };
                if rk.queue != queue {
                    missing += 1;
                    continue;
                }
                let id = rk.job_id.as_bytes().to_vec();
                if tx.get(&store.ready, &key).map_err(stg_err)?.is_none() {
                    indexes.ready.remove(&key);
                    missing += 1;
                    continue;
                }
                id
            }
            PeekState::Scheduled => {
                let Some(id) = TimerKey::job_id(&key) else {
                    missing += 1;
                    continue;
                };
                let id = id.to_vec();
                if tx.get(&store.scheduled, &key).map_err(stg_err)?.is_none() {
                    indexes.scheduled.remove(&key);
                    missing += 1;
                    continue;
                }
                id
            }
            _ => unreachable!("state validated above"),
        };

        // Timer keys carry no queue, so the job record is the queue authority
        // for both states; a mismatch means the key belongs to another queue.
        let Some(stored) = tx.get(&store.jobs, &job_id).map_err(stg_err)? else {
            missing += 1;
            continue;
        };
        let job_queue = match JobValue::decode(&stored) {
            Ok((q, _)) => q,
            Err(e) => {
                warn!(error = %e, "dead-letter: skipping corrupt job record");
                missing += 1;
                continue;
            }
        };
        if job_queue != queue {
            missing += 1;
            continue;
        }

        // Jobs that never reached a worker are on their stored attempt
        // (1 for fresh enqueues, higher for nack-rescheduled ones).
        let attempt = match state {
            PeekState::Ready => indexes.ready.attempt(&key).unwrap_or(1),
            _ => tx
                .get(&store.scheduled, &key)
                .map_err(stg_err)?
                .and_then(|v| v.as_ref().try_into().ok().map(u32::from_be_bytes))
                .unwrap_or(1),
        };

        maybe_store_dead_letter(
            store,
            indexes,
            tx,
            &job_id,
            DeadLetterMeta {
                cause: DeadLetterCause::Admin,
                failed_at: now,
                attempt,
                last_reason: reason.clone(),
            },
            dead_letter_enabled,
        )?;

        match state {
            PeekState::Ready => {
                tx.remove(&store.ready, key.clone());
                indexes.ready.remove(&key);
            }
            PeekState::Scheduled => {
                tx.remove(&store.scheduled, key.clone());
                indexes.scheduled.remove(&key);
            }
            _ => unreachable!("state validated above"),
        }
        tx.remove(&store.jobs, job_id.clone());
        job_ids.push(String::from_utf8_lossy(&job_id).into_owned());
        tx.remove(&store.payloads, job_id);

        cycle.dead_lettered(queue, "admin");
        cycle.dirty = true;
        dead_lettered += 1;
    }

    Ok(DeadLetterJobsOutcome {
        dead_lettered,
        missing,
        job_ids,
    })
}

pub(crate) fn apply_delete_dead_letters(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    queue: &str,
    keys: Vec<Vec<u8>>,
) -> DeleteOutcome {
    let mut deleted = 0u64;
    let mut missing = 0u64;
    let mut job_ids = Vec::new();

    for key in keys {
        match indexes.dead_letter.keys.get(&key) {
            Some(owner) if owner == queue => {
                indexes.dead_letter.remove(&key);
                if let Some(id) = DeadLetterKey::job_id(&key) {
                    job_ids.push(id.to_string());
                }
                tx.remove(&store.dead_letter, key);
                cycle.dirty = true;
                deleted += 1;
            }
            _ => missing += 1,
        }
    }

    DeleteOutcome {
        deleted,
        missing,
        job_ids,
    }
}

pub(crate) fn apply_close_queue(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    queue: String,
    now: i64,
    grace_ms: i64,
) {
    let deadline = now.saturating_add(grace_ms);
    tx.insert(
        &store.meta,
        closing_key(&queue),
        deadline.to_be_bytes().to_vec(),
    );
    indexes.closing.insert(queue, deadline);
    cycle.dirty = true;
}

pub(crate) fn apply_open_queue(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    queue: &str,
) {
    // `closing` mirrors the meta rows exactly, so a missing entry means there
    // is no row to delete.
    if indexes.closing.remove(queue).is_some() {
        tx.remove(&store.meta, closing_key(queue));
        cycle.dirty = true;
    }
}

pub(crate) fn apply_purge_queue_chunk(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    queue: &str,
    max: usize,
) -> Result<PurgeOutcome, Status> {
    if indexes.leases.by_queue.get(queue).copied().unwrap_or(0) > 0 {
        return Err(Status::failed_precondition("queue has in-flight jobs"));
    }

    let mut purged = 0usize;

    let prefix = queue_prefix(queue);
    while purged < max {
        let Some((ready_k, _)) = indexes.ready.pop_front(&prefix) else {
            break;
        };
        if let Some(rk) = ReadyKey::decode(&ready_k) {
            tx.remove(&store.jobs, rk.job_id.as_bytes().to_vec());
            tx.remove(&store.payloads, rk.job_id.as_bytes().to_vec());
        }
        tx.remove(&store.ready, ready_k);
        cycle.dirty = true;
        purged += 1;
    }

    let chosen: Vec<Vec<u8>> = indexes
        .scheduled
        .keys
        .iter()
        .filter(|(_, q)| q.as_str() == queue)
        .take(max - purged)
        .map(|(k, _)| k.clone())
        .collect();
    for timer_k in chosen {
        if let Some(job_id) = TimerKey::job_id(&timer_k) {
            tx.remove(&store.jobs, job_id.to_vec());
            tx.remove(&store.payloads, job_id.to_vec());
        }
        tx.remove(&store.scheduled, timer_k.clone());
        indexes.scheduled.remove(&timer_k);
        cycle.dirty = true;
        purged += 1;
    }

    let chosen: Vec<Vec<u8>> = indexes
        .dead_letter
        .keys
        .iter()
        .filter(|(_, q)| q.as_str() == queue)
        .take(max - purged)
        .map(|(k, _)| k.clone())
        .collect();
    for key in chosen {
        tx.remove(&store.dead_letter, key.clone());
        indexes.dead_letter.remove(&key);
        cycle.dirty = true;
        purged += 1;
    }

    let chosen: Vec<Vec<u8>> = indexes
        .dedup_timers
        .keys
        .iter()
        .filter(|(_, q)| q.as_str() == queue)
        .take(max - purged)
        .map(|(k, _)| k.clone())
        .collect();
    for timer_k in chosen {
        if let Some(dedup_k) = DedupTimerKey::dedup_key(&timer_k) {
            tx.remove(&store.dedup, dedup_k.to_vec());
        }
        tx.remove(&store.dedup_timers, timer_k.clone());
        indexes.dedup_timers.remove(&timer_k);
        cycle.dirty = true;
        purged += 1;
    }

    let remaining = indexes.ready.by_queue.contains_key(queue)
        || indexes.scheduled.by_queue.contains_key(queue)
        || indexes.dead_letter.by_queue.contains_key(queue)
        || indexes.dedup_timers.by_queue.contains_key(queue);
    if !remaining {
        cycle.queue_purged(queue);
    }

    Ok(PurgeOutcome {
        purged: purged as u64,
        remaining,
    })
}

// An unkeyed hash instead of an RNG keeps op construction reproducible.
pub(crate) fn retry_jitter_hash(job_id: &str, attempt: u32) -> u64 {
    let mut h = DefaultHasher::new();
    job_id.hash(&mut h);
    attempt.hash(&mut h);
    h.finish()
}

pub(crate) fn policy_retry_delay_ms(policy: &RetryPolicy, attempt: u32, job_id: &str) -> u64 {
    let base = policy.retry_delay_ms;
    if base == 0 {
        return 0;
    }

    let grown = match policy.retry_backoff {
        RetryBackoff::None => base,
        RetryBackoff::Exponential => {
            base.saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1)))
        }
    };
    let cap = policy
        .retry_delay_max_ms
        .min(policy.max_schedule_horizon_ms);
    let capped = grown.min(cap);

    // Subtract the jitter so that for capped delay there is still
    // some variance.
    capped - retry_jitter_hash(job_id, attempt) % (capped / 4 + 1)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_nack(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    req: NackRequest,
    retry_delay_ms: u64,
    dead_letter_enabled: bool,
    now: i64,
) -> Result<NackOutcome, Status> {
    let stored = tx
        .get(&store.inflight, req.job_id.as_bytes())
        .map_err(stg_err)?
        .ok_or_else(|| Status::not_found("job not found"))?;

    let inflight = Inflight::decode(&stored)?;
    if inflight.attempt != req.attempt {
        return Err(Status::failed_precondition("attempt mismatch"));
    }

    let lease_timer = TimerKey {
        deadline: inflight.lease_expires_at,
        job_id: &req.job_id,
    }
    .encode();
    let strategy = req.retry.as_ref().and_then(|r| r.strategy.as_ref());
    let force_dead_letter = matches!(strategy, Some(nack_retry::Strategy::DeadLetter(_)));

    if force_dead_letter || inflight.attempt >= inflight.max_attempts {
        let (cause_label, cause) = if force_dead_letter {
            ("rejected", DeadLetterCause::Rejected)
        } else {
            ("attempts_exhausted", DeadLetterCause::AttemptsExhausted)
        };

        maybe_store_dead_letter(
            store,
            indexes,
            tx,
            req.job_id.as_bytes(),
            DeadLetterMeta {
                cause,
                failed_at: now,
                attempt: inflight.attempt,
                last_reason: req.reason.clone(),
            },
            dead_letter_enabled,
        )?;

        tx.remove(&store.jobs, req.job_id.as_bytes().to_vec());
        tx.remove(&store.payloads, req.job_id.as_bytes().to_vec());
        tx.remove(&store.inflight, req.job_id.into_bytes());
        tx.remove(&store.leases, lease_timer.clone());

        indexes.leases.remove(&lease_timer);
        cycle.nacked(&inflight.queue);
        cycle.dead_lettered(&inflight.queue, cause_label);
        cycle.dirty = true;

        return Ok(NackOutcome {
            queue: inflight.queue,
            dead_lettered: true,
            retry_delay_ms: 0,
            trace_context: inflight.trace_context,
        });
    }

    let attempt = inflight.attempt + 1;
    if retry_delay_ms > 0 {
        let deadline = now.saturating_add(i64::try_from(retry_delay_ms).unwrap_or(i64::MAX));
        let tk = TimerKey {
            deadline,
            job_id: &req.job_id,
        }
        .encode();
        tx.insert(&store.scheduled, tk.clone(), attempt.to_be_bytes().to_vec());
        indexes.scheduled.insert(tk, &inflight.queue);
    } else {
        let rk = ReadyKey {
            queue: &inflight.queue,
            priority: inflight.priority,
            enqueued_at: inflight.enqueued_at,
            job_id: &req.job_id,
        }
        .encode();

        tx.insert(&store.ready, rk.clone(), attempt.to_be_bytes().to_vec());
        indexes.ready.insert(rk, attempt);
        cycle.new_ready.insert(inflight.queue.clone());
    }

    tx.remove(&store.inflight, req.job_id.into_bytes());
    tx.remove(&store.leases, lease_timer.clone());
    indexes.leases.remove(&lease_timer);

    cycle.nacked(&inflight.queue);
    cycle.dirty = true;

    Ok(NackOutcome {
        queue: inflight.queue,
        dead_lettered: false,
        retry_delay_ms,
        trace_context: inflight.trace_context,
    })
}

pub(crate) fn apply_extend(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    req: ExtendRequest,
    lease_ms: u64,
    now: i64,
) -> Result<ExtendOutcome, Status> {
    let stored = tx
        .get(&store.inflight, req.job_id.as_bytes())
        .map_err(stg_err)?
        .ok_or_else(|| Status::not_found("job not found"))?;

    let mut inflight = Inflight::decode(&stored)?;
    if inflight.attempt != req.attempt {
        return Err(Status::failed_precondition("attempt mismatch"));
    }

    let old_timer = TimerKey {
        deadline: inflight.lease_expires_at,
        job_id: &req.job_id,
    }
    .encode();
    // The max-lease clamp happened at propose time (see resolve_extend_lease).
    // Floor at 1ms so a zero in the log cannot grant an already-expired lease.
    let lease_ms = lease_ms.max(1);
    let lease_expires_at = now.saturating_add(i64::try_from(lease_ms).unwrap_or(i64::MAX));
    inflight.lease_expires_at = lease_expires_at;

    tx.insert(
        &store.inflight,
        req.job_id.clone().into_bytes(),
        inflight.encode(),
    );

    tx.remove(&store.leases, old_timer.clone());
    indexes.leases.remove(&old_timer);

    let new_timer = TimerKey {
        deadline: lease_expires_at,
        job_id: &req.job_id,
    }
    .encode();
    tx.insert(&store.leases, new_timer.clone(), Vec::new());
    indexes.leases.insert(new_timer, &inflight.queue);
    cycle.dirty = true;

    Ok(ExtendOutcome {
        queue: inflight.queue,
        lease_expires_at,
        trace_context: inflight.trace_context,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_sweep(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    now: i64,
    budget: usize,
    retention_cutoff_ms: Option<i64>,
    dead_letter_enabled: bool,
) -> Result<usize, Status> {
    let mut processed = 0usize;

    // Drop close tombstones whose delete handler died without clearing them.
    indexes.closing.retain(|queue, deadline| {
        if *deadline > now {
            return true;
        }
        tx.remove(&store.meta, closing_key(queue));
        cycle.dirty = true;
        false
    });

    // Each phase gets its own budget so a backlog of one timer kind cannot
    // starve another — most importantly, scheduled promotions must not crowd
    // out lease-expiry redelivery.
    let mut remaining = budget;
    while remaining > 0 {
        let Some((timer_k, _)) = indexes.scheduled.pop_due(now) else {
            break;
        };

        remaining -= 1;
        processed += 1;
        let attempt_hint = tx
            .get(&store.scheduled, &timer_k)
            .map_err(stg_err)?
            .and_then(|v| v.first_chunk::<4>().map(|b| u32::from_be_bytes(*b)));

        tx.remove(&store.scheduled, timer_k.clone());
        cycle.dirty = true;

        let Some(job_id) = TimerKey::job_id(&timer_k) else {
            continue;
        };

        let Some(stored) = tx.get(&store.jobs, job_id).map_err(stg_err)? else {
            continue;
        };

        let (queue, job) = match JobValue::decode(&stored) {
            Ok(decoded) => decoded,
            Err(e) => {
                warn!(error = %e, "sweep skipping corrupt job");
                continue;
            }
        };

        let enqueued_at_ms = job
            .enqueued_at
            .as_ref()
            .map(timestamp_to_millis)
            .unwrap_or(0);
        let scheduled_at_ms = job.scheduled_at.as_ref().map(timestamp_to_millis);
        let attempt = attempt_hint.unwrap_or(job.attempt);
        let _span = telemetry::enabled().then(|| {
            let span = tracing::info_span!(
                "sepp.promote",
                job_id = %job.id,
                queue = %queue,
                job_type = %job.job_type,
                attempt,
                priority = job.priority,
                scheduled_at = scheduled_at_ms,
            );
            telemetry::link_from_proto(&span, job.trace_context.as_ref());
            span.entered()
        });

        debug!(
            job_id = %job.id,
            queue = %queue,
            attempt,
            "scheduled job promoted to ready",
        );

        let rk = ReadyKey {
            queue: &queue,
            priority: job.priority,
            enqueued_at: enqueued_at_ms,
            job_id: &job.id,
        }
        .encode();

        tx.insert(&store.ready, rk.clone(), attempt.to_be_bytes().to_vec());
        indexes.ready.insert(rk, attempt);
        cycle.sweep_promotion(&queue);
        cycle.new_ready.insert(queue);
    }

    let mut remaining = budget;
    while remaining > 0 {
        let Some((timer_k, _)) = indexes.leases.pop_due(now) else {
            break;
        };

        remaining -= 1;
        processed += 1;
        tx.remove(&store.leases, timer_k.clone());
        cycle.dirty = true;

        let Some(job_id) = TimerKey::job_id(&timer_k) else {
            continue;
        };

        let Some(stored) = tx.get(&store.inflight, job_id).map_err(stg_err)? else {
            continue;
        };

        let inflight = match Inflight::decode(&stored) {
            Ok(decoded) => decoded,
            Err(e) => {
                warn!(error = %e, "sweep skipping corrupt inflight record");
                continue;
            }
        };

        if inflight.attempt >= inflight.max_attempts {
            let job_id_str = String::from_utf8_lossy(job_id);
            let _span = telemetry::enabled().then(|| {
                let span = tracing::info_span!(
                    "sepp.dead_letter",
                    job_id = %job_id_str,
                    queue = %inflight.queue,
                    attempt = inflight.attempt,
                    max_attempts = inflight.max_attempts,
                    cause = "lease_expired",
                );
                telemetry::link_from_proto(&span, inflight.trace_context.as_ref());
                span.entered()
            });

            warn!(
                job_id = %job_id_str,
                attempt = inflight.attempt,
                "job dead-lettered: lease expired with attempts exhausted"
            );

            maybe_store_dead_letter(
                store,
                indexes,
                tx,
                job_id,
                DeadLetterMeta {
                    cause: DeadLetterCause::LeaseExpired,
                    failed_at: now,
                    attempt: inflight.attempt,
                    last_reason: None,
                },
                dead_letter_enabled,
            )?;

            tx.remove(&store.jobs, job_id.to_vec());
            tx.remove(&store.payloads, job_id.to_vec());
            tx.remove(&store.inflight, job_id.to_vec());
            cycle.dead_lettered(&inflight.queue, "lease_expired");
        } else {
            let Ok(job_id_str) = std::str::from_utf8(job_id) else {
                continue;
            };

            let attempt = inflight.attempt + 1;
            let _span = telemetry::enabled().then(|| {
                let span = tracing::info_span!(
                    "sepp.redeliver",
                    job_id = %job_id_str,
                    queue = %inflight.queue,
                    attempt,
                    max_attempts = inflight.max_attempts,
                    reason = "lease_expired",
                );
                telemetry::link_from_proto(&span, inflight.trace_context.as_ref());
                span.entered()
            });

            debug!(
                job_id = %job_id_str,
                queue = %inflight.queue,
                attempt,
                "lease expired; requeueing job",
            );

            let rk = ReadyKey {
                queue: &inflight.queue,
                priority: inflight.priority,
                enqueued_at: inflight.enqueued_at,
                job_id: job_id_str,
            }
            .encode();

            tx.insert(&store.ready, rk.clone(), attempt.to_be_bytes().to_vec());
            indexes.ready.insert(rk, attempt);
            tx.remove(&store.inflight, job_id.to_vec());
            cycle.sweep_lease_redelivery(&inflight.queue);
            cycle.new_ready.insert(inflight.queue);
        }
    }

    let mut remaining = budget;
    while remaining > 0 {
        let Some((timer_k, queue)) = indexes.dedup_timers.pop_due(now) else {
            break;
        };

        remaining -= 1;
        processed += 1;
        if let Some(dedup_k) = DedupTimerKey::dedup_key(&timer_k) {
            let record_deadline = tx
                .get(&store.dedup, dedup_k)
                .map_err(stg_err)?
                .and_then(|v| DedupValue::decode(&v).map(|dv| dv.deadline));
            if record_deadline.is_some_and(|d| d <= now) {
                tx.remove(&store.dedup, dedup_k.to_vec());
            }
        }

        tx.remove(&store.dedup_timers, timer_k.clone());
        cycle.sweep_dedup_expiration(&queue);
        cycle.dirty = true;
    }

    if let Some(cutoff) = retention_cutoff_ms {
        let mut remaining = budget;
        let mut expired = 0u64;

        while remaining > 0 {
            let Some((key, _queue)) = indexes.dead_letter.pop_due(cutoff) else {
                break;
            };
            remaining -= 1;
            processed += 1;
            expired += 1;
            tx.remove(&store.dead_letter, key);
            cycle.dirty = true;
        }

        if expired > 0 {
            cycle.dead_letter_expired(expired);
        }
    }

    Ok(processed)
}

pub(crate) fn apply_audit_append(
    store: &Store,
    tx: &mut ApplyTx<'_>,
    cycle: &mut Cycle,
    record: &AuditRecord,
    now_ms: i64,
) -> Result<u64, Status> {
    let seq = match tx.get(&store.meta, AUDIT_SEQ_KEY).map_err(stg_err)? {
        Some(v) => <[u8; 8]>::try_from(v.as_ref())
            .ok()
            .and_then(|b| u64::from_be_bytes(b).checked_add(1))
            .ok_or_else(|| Status::internal("corrupt audit_seq row"))?,
        None => 1,
    };

    tx.insert(
        &store.meta,
        AUDIT_SEQ_KEY.to_vec(),
        seq.to_be_bytes().to_vec(),
    );
    tx.insert(
        &store.audit,
        seq.to_be_bytes().to_vec(),
        AuditValue {
            ts_ms: now_ms,
            record,
        }
        .encode(),
    );
    cycle.dirty = true;

    Ok(seq)
}
