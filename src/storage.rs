use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fjall::{Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode};
use prost::Message;
use tokio::sync::{Notify, futures::Notified, oneshot};
use tonic::Status;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::pb::sepp::v1::{
    EnqueueRequest, EnqueueResponse, ErrorDetails, ExtendRequest, Job, JobResult, NackRequest,
    job_result, nack_retry,
};

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

struct StorageParams {
    persist_mode: PersistMode,
    sweep_limit: usize,
    dedup_window_ms: i64,
    default_max_attempts: u32,
    default_priority: u32,
    max_attempts_ceiling: u32,
    max_schedule_horizon_ms: u64,
}

struct Store {
    db: Database,
    payloads: Keyspace,
    inflight: Keyspace,
    dead_letters: Keyspace,
    ready: Keyspace,
    dedup: Keyspace,
    dedup_timers: Keyspace,
    scheduled: Keyspace,
    leases: Keyspace,
    params: StorageParams,
}

fn read_u32(bytes: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
}

fn read_i64(bytes: &[u8], at: usize) -> Option<i64> {
    Some(i64::from_be_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

fn queue_prefix(queue: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + queue.len());
    k.extend_from_slice(&(queue.len() as u16).to_be_bytes());
    k.extend_from_slice(queue.as_bytes());
    k
}

fn ready_key(queue: &str, priority: u32, enqueued_at: i64, job_id: &str) -> Vec<u8> {
    let mut k = queue_prefix(queue);
    k.push(9u8.saturating_sub(priority.min(9) as u8));
    k.extend_from_slice(&enqueued_at.to_be_bytes());
    k.extend_from_slice(job_id.as_bytes());
    k
}

fn ready_key_id_offset(queue: &str) -> usize {
    2 + queue.len() + 1 + 8
}

fn dedup_key(queue: &str, idempotency_key: &str) -> Vec<u8> {
    let mut k = queue_prefix(queue);
    k.extend_from_slice(idempotency_key.as_bytes());
    k
}

fn encode_dedup(enqueued_at: i64, job_id: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + job_id.len());
    v.extend_from_slice(&enqueued_at.to_be_bytes());
    v.extend_from_slice(job_id.as_bytes());
    v
}

fn decode_dedup(bytes: &[u8]) -> Option<(i64, &str)> {
    let enqueued_at = i64::from_be_bytes(bytes.first_chunk::<8>().copied()?);
    let job_id = std::str::from_utf8(bytes.get(8..)?).ok()?;
    Some((enqueued_at, job_id))
}

fn timer_key(deadline: i64, job_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(8 + job_id.len());
    k.extend_from_slice(&deadline.to_be_bytes());
    k.extend_from_slice(job_id.as_bytes());
    k
}

fn deadline_of(key: &[u8]) -> i64 {
    i64::from_be_bytes(key.first_chunk::<8>().copied().unwrap_or([0; 8]))
}

fn dedup_timer_key(deadline: i64, dedup_key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(8 + dedup_key.len());
    k.extend_from_slice(&deadline.to_be_bytes());
    k.extend_from_slice(dedup_key);
    k
}

fn encode_job(queue: &str, job: &Job) -> Vec<u8> {
    let mut v = queue_prefix(queue);
    job.encode(&mut v)
        .expect("Vec buffer never runs out of space");
    v
}

fn decode_job(bytes: &[u8]) -> Result<(String, Job), Status> {
    let corrupt = || Status::internal("corrupt job record");
    let qlen = 2 + u16::from_be_bytes(*bytes.first_chunk::<2>().ok_or_else(corrupt)?) as usize;
    let queue = std::str::from_utf8(bytes.get(2..qlen).ok_or_else(corrupt)?)
        .map_err(|_| corrupt())?
        .to_owned();
    let job = Job::decode(&bytes[qlen..]).map_err(|_| corrupt())?;
    Ok((queue, job))
}

struct Inflight {
    attempt: u32,
    lease_expires_at: i64,
    enqueued_at: i64,
    priority: u32,
    max_attempts: u32,
    queue: String,
}

fn encode_inflight(s: &Inflight) -> Vec<u8> {
    let mut v = Vec::with_capacity(28 + s.queue.len());
    v.extend_from_slice(&s.attempt.to_be_bytes());
    v.extend_from_slice(&s.lease_expires_at.to_be_bytes());
    v.extend_from_slice(&s.enqueued_at.to_be_bytes());
    v.extend_from_slice(&s.priority.to_be_bytes());
    v.extend_from_slice(&s.max_attempts.to_be_bytes());
    v.extend_from_slice(s.queue.as_bytes());
    v
}

fn decode_inflight(bytes: &[u8]) -> Result<Inflight, Status> {
    let corrupt = || Status::internal("corrupt inflight record");
    let attempt = read_u32(bytes, 0).ok_or_else(corrupt)?;
    let lease_expires_at = read_i64(bytes, 4).ok_or_else(corrupt)?;
    let enqueued_at = read_i64(bytes, 12).ok_or_else(corrupt)?;
    let priority = read_u32(bytes, 20).ok_or_else(corrupt)?;
    let max_attempts = read_u32(bytes, 24).ok_or_else(corrupt)?;
    let queue = std::str::from_utf8(bytes.get(28..).ok_or_else(corrupt)?)
        .map_err(|_| corrupt())?
        .to_owned();
    Ok(Inflight {
        attempt,
        lease_expires_at,
        enqueued_at,
        priority,
        max_attempts,
        queue,
    })
}

#[derive(Clone, Copy)]
enum DeadLetterCause {
    Rejected = 0,
    AttemptsExhausted = 1,
    LeaseExpired = 2,
}

fn encode_dead_letter(dead_lettered_at: i64, cause: DeadLetterCause) -> Vec<u8> {
    let mut v = Vec::with_capacity(9);
    v.extend_from_slice(&dead_lettered_at.to_be_bytes());
    v.push(cause as u8);
    v
}

fn stg_err(e: fjall::Error) -> Status {
    Status::internal(format!("storage error: {e}"))
}

fn status_to_error(s: &Status) -> ErrorDetails {
    ErrorDetails {
        code: format!("{:?}", s.code()),
        message: s.message().to_string(),
        context: HashMap::new(),
    }
}

#[derive(Default)]
struct ReadyIndex {
    keys: BTreeMap<Vec<u8>, u32>,
}

impl ReadyIndex {
    fn insert(&mut self, ready_key: Vec<u8>, attempt: u32) {
        self.keys.insert(ready_key, attempt);
    }

    fn pop_front(&mut self, queue_prefix: &[u8]) -> Option<(Vec<u8>, u32)> {
        let key = self
            .keys
            .range(queue_prefix.to_vec()..)
            .next()
            .filter(|(k, _)| k.starts_with(queue_prefix))
            .map(|(k, _)| k.clone())?;
        let attempt = self.keys.remove(&key)?;
        Some((key, attempt))
    }
}

#[derive(Default)]
struct TimerIndex {
    keys: BTreeSet<Vec<u8>>,
}

impl TimerIndex {
    fn insert(&mut self, key: Vec<u8>) {
        self.keys.insert(key);
    }

    fn remove(&mut self, key: &[u8]) {
        self.keys.remove(key);
    }

    fn pop_due(&mut self, now: i64) -> Option<Vec<u8>> {
        let first = self.keys.iter().next()?;
        if deadline_of(first) > now {
            return None;
        }
        let key = first.clone();
        self.keys.remove(&key);
        Some(key)
    }

    fn earliest(&self) -> Option<i64> {
        self.keys.iter().next().map(|k| deadline_of(k))
    }
}

#[derive(Default)]
struct Indexes {
    ready: ReadyIndex,
    scheduled: TimerIndex,
    leases: TimerIndex,
    dedup_timers: TimerIndex,
}

fn rebuild_indexes(store: &Store) -> Result<Indexes, fjall::Error> {
    let mut indexes = Indexes::default();
    for guard in store.ready.iter() {
        let (key, value) = guard.into_inner()?;
        let attempt = read_u32(&value, 0).unwrap_or(1);
        indexes.ready.insert(key.to_vec(), attempt);
    }
    for guard in store.scheduled.iter() {
        let (key, _) = guard.into_inner()?;
        indexes.scheduled.insert(key.to_vec());
    }
    for guard in store.leases.iter() {
        let (key, _) = guard.into_inner()?;
        indexes.leases.insert(key.to_vec());
    }
    for guard in store.dedup_timers.iter() {
        let (key, _) = guard.into_inner()?;
        indexes.dedup_timers.insert(key.to_vec());
    }
    Ok(indexes)
}

fn resync(store: &Store, indexes: &mut Indexes) {
    match rebuild_indexes(store) {
        Ok(fresh) => *indexes = fresh,
        Err(e) => error!(error = %e, "could not re-sync the in-memory indexes"),
    }
}

enum PerJob {
    Settled(JobResult),
    Pending(EnqueueResponse),
}

impl PerJob {
    fn resolve(self, outcome: &Result<(), Status>) -> JobResult {
        match self {
            PerJob::Settled(result) => result,
            PerJob::Pending(resp) => match outcome {
                Ok(()) => job_ok(resp),
                Err(s) => job_err(s),
            },
        }
    }
}

fn job_ok(resp: EnqueueResponse) -> JobResult {
    JobResult {
        outcome: Some(job_result::Outcome::Success(resp)),
    }
}

fn job_err(s: &Status) -> JobResult {
    JobResult {
        outcome: Some(job_result::Outcome::Error(status_to_error(s))),
    }
}

enum Command {
    Enqueue {
        jobs: Vec<EnqueueRequest>,
        resp: oneshot::Sender<Vec<JobResult>>,
    },
    Reserve {
        queues: Vec<String>,
        lease_ms: u64,
        max_jobs: usize,
        resp: oneshot::Sender<Result<Vec<Job>, Status>>,
    },
    Ack {
        job_id: String,
        attempt: u32,
        resp: oneshot::Sender<Result<(), Status>>,
    },
    Nack {
        req: NackRequest,
        resp: oneshot::Sender<Result<bool, Status>>,
    },
    Extend {
        req: ExtendRequest,
        resp: oneshot::Sender<Result<i64, Status>>,
    },
}

type Responder = Box<dyn FnOnce(&Result<(), Status>) + Send>;

#[derive(Default)]
struct Cycle {
    dirty: bool,
    new_ready: HashSet<String>,
    dedup_seen: HashMap<(String, String), String>,
}

fn next_deadline(indexes: &Indexes) -> Option<i64> {
    [
        indexes.scheduled.earliest(),
        indexes.leases.earliest(),
        indexes.dedup_timers.earliest(),
    ]
    .into_iter()
    .flatten()
    .min()
}

fn run_committer(
    store: Store,
    mut indexes: Indexes,
    rx: flume::Receiver<Command>,
    notifiers: QueueNotifiers,
    max_sweep_interval: Duration,
) {
    loop {
        let sweep_due = next_deadline(&indexes).is_some_and(|d| d <= now_ms());
        if sweep_due {
            run_sweep_cycle(&store, &mut indexes, &notifiers);
        }

        let first = if sweep_due {
            match rx.try_recv() {
                Ok(c) => Some(c),
                Err(flume::TryRecvError::Empty) => None,
                Err(flume::TryRecvError::Disconnected) => break,
            }
        } else {
            let wait = match next_deadline(&indexes) {
                Some(deadline) => Duration::from_millis((deadline - now_ms()).max(0) as u64)
                    .min(max_sweep_interval),
                None => max_sweep_interval,
            };
            match rx.recv_timeout(wait) {
                Ok(c) => Some(c),
                Err(flume::RecvTimeoutError::Timeout) => None,
                Err(flume::RecvTimeoutError::Disconnected) => break,
            }
        };

        if let Some(first) = first {
            let mut rpcs = vec![first];
            while let Ok(c) = rx.try_recv() {
                rpcs.push(c);
            }
            run_rpc_cycle(&store, &mut indexes, &notifiers, rpcs);
        }
    }
}

fn run_rpc_cycle(
    store: &Store,
    indexes: &mut Indexes,
    notifiers: &QueueNotifiers,
    rpcs: Vec<Command>,
) {
    let mut batch = store.db.batch();
    let mut cycle = Cycle::default();
    let mut responders: Vec<Responder> = Vec::new();

    for cmd in rpcs {
        match cmd {
            Command::Enqueue { jobs, resp } => {
                let per_jobs = apply_enqueue(store, indexes, &mut batch, &mut cycle, jobs);
                responders.push(Box::new(move |o| {
                    let _ = resp.send(per_jobs.into_iter().map(|pj| pj.resolve(o)).collect());
                }));
            }
            Command::Reserve {
                queues,
                lease_ms,
                max_jobs,
                resp,
            } => match apply_reserve(
                store, indexes, &mut batch, &mut cycle, &queues, lease_ms, max_jobs,
            ) {
                Ok(jobs) => responders.push(Box::new(move |o| {
                    let _ = resp.send(o.clone().map(|()| jobs));
                })),
                Err(e) => {
                    let _ = resp.send(Err(e));
                }
            },
            Command::Ack {
                job_id,
                attempt,
                resp,
            } => match apply_ack(store, indexes, &mut batch, &mut cycle, &job_id, attempt) {
                Ok(()) => responders.push(Box::new(move |o| {
                    let _ = resp.send(o.clone());
                })),
                Err(e) => {
                    let _ = resp.send(Err(e));
                }
            },
            Command::Nack { req, resp } => {
                match apply_nack(store, indexes, &mut batch, &mut cycle, req) {
                    Ok(dead_lettered) => responders.push(Box::new(move |o| {
                        let _ = resp.send(o.clone().map(|()| dead_lettered));
                    })),
                    Err(e) => {
                        let _ = resp.send(Err(e));
                    }
                }
            }
            Command::Extend { req, resp } => {
                match apply_extend(store, indexes, &mut batch, &mut cycle, req) {
                    Ok(lease_expires_at) => responders.push(Box::new(move |o| {
                        let _ = resp.send(o.clone().map(|()| lease_expires_at));
                    })),
                    Err(e) => {
                        let _ = resp.send(Err(e));
                    }
                }
            }
        }
    }

    let outcome = if cycle.dirty {
        commit_and_persist(store, batch)
    } else {
        Ok(())
    };
    if outcome.is_err() {
        resync(store, indexes);
    }

    for responder in responders {
        responder(&outcome);
    }
    if outcome.is_ok() {
        for queue in &cycle.new_ready {
            notifiers.wake(queue);
        }
    }
}

fn run_sweep_cycle(store: &Store, indexes: &mut Indexes, notifiers: &QueueNotifiers) {
    let started = std::time::Instant::now();
    let mut batch = store.db.batch();
    let mut cycle = Cycle::default();

    let processed = match apply_sweep(store, indexes, &mut batch, &mut cycle) {
        Ok(processed) => processed,
        Err(e) => {
            warn!(error = %e, "timer sweep aborted");
            resync(store, indexes);
            return;
        }
    };
    let outcome = if cycle.dirty {
        commit_and_persist(store, batch)
    } else {
        Ok(())
    };
    if outcome.is_err() {
        resync(store, indexes);
        return;
    }
    for queue in &cycle.new_ready {
        notifiers.wake(queue);
    }

    let elapsed = started.elapsed();
    if elapsed >= Duration::from_millis(100) {
        warn!(?elapsed, processed, "slow timer sweep");
    } else {
        debug!(?elapsed, processed, "timer sweep");
    }
}

fn commit_and_persist(store: &Store, batch: OwnedWriteBatch) -> Result<(), Status> {
    match batch
        .commit()
        .and_then(|()| store.db.persist(store.params.persist_mode))
    {
        Ok(()) => Ok(()),
        Err(e) => {
            error!(error = %e, "storage commit failed");
            Err(Status::internal("storage commit failed"))
        }
    }
}

fn apply_enqueue(
    store: &Store,
    indexes: &mut Indexes,
    batch: &mut OwnedWriteBatch,
    cycle: &mut Cycle,
    jobs: Vec<EnqueueRequest>,
) -> Vec<PerJob> {
    let now = now_ms();
    let mut results = Vec::with_capacity(jobs.len());

    for req in jobs {
        let mut stale_dedup_timer: Option<Vec<u8>> = None;

        if let Some(key) = &req.idempotency_key {
            let dk = (req.queue.clone(), key.clone());
            if let Some(existing) = cycle.dedup_seen.get(&dk) {
                results.push(PerJob::Settled(job_ok(EnqueueResponse {
                    job_id: existing.clone(),
                    deduplicated: true,
                })));
                continue;
            }
            let dkey = dedup_key(&req.queue, key);
            match store.dedup.get(&dkey) {
                Ok(Some(existing)) => match decode_dedup(&existing) {
                    Some((ts, job_id)) if now - ts < store.params.dedup_window_ms => {
                        results.push(PerJob::Settled(job_ok(EnqueueResponse {
                            job_id: job_id.to_owned(),
                            deduplicated: true,
                        })));
                        continue;
                    }
                    Some((ts, _)) => {
                        stale_dedup_timer =
                            Some(dedup_timer_key(ts + store.params.dedup_window_ms, &dkey));
                    }
                    None => {}
                },
                Ok(None) => {}
                Err(e) => {
                    results.push(PerJob::Settled(job_err(&stg_err(e))));
                    continue;
                }
            }
        }

        let id = Uuid::new_v4().to_string();
        let queue = req.queue;
        let job = Job {
            id: id.clone(),
            job_type: req.job_type,
            payload: req.payload,
            priority: req.priority.unwrap_or(store.params.default_priority),
            trace_context: req.trace_context,
            enqueued_at: now,
            attempt: 1,
            max_attempts: req
                .max_attempts
                .unwrap_or(store.params.default_max_attempts)
                .min(store.params.max_attempts_ceiling),
            lease_expires_at: 0,
            custom: req.custom,
            scheduled_at: req.scheduled_at,
        };

        batch.insert(
            &store.payloads,
            id.clone().into_bytes(),
            encode_job(&queue, &job),
        );
        match job.scheduled_at {
            Some(at) if at > now => {
                let tk = timer_key(at, &id);
                batch.insert(
                    &store.scheduled,
                    tk.clone(),
                    job.attempt.to_be_bytes().to_vec(),
                );
                indexes.scheduled.insert(tk);
            }
            _ => {
                let rk = ready_key(&queue, job.priority, job.enqueued_at, &id);
                batch.insert(&store.ready, rk.clone(), job.attempt.to_be_bytes().to_vec());
                indexes.ready.insert(rk, job.attempt);
                cycle.new_ready.insert(queue.clone());
            }
        }
        if let Some(key) = &req.idempotency_key {
            let dkey = dedup_key(&queue, key);
            if let Some(old_timer) = stale_dedup_timer {
                batch.remove(&store.dedup_timers, old_timer.clone());
                indexes.dedup_timers.remove(&old_timer);
            }
            let dtk = dedup_timer_key(now + store.params.dedup_window_ms, &dkey);
            batch.insert(&store.dedup_timers, dtk.clone(), Vec::new());
            indexes.dedup_timers.insert(dtk);
            batch.insert(&store.dedup, dkey, encode_dedup(now, &id));
            cycle.dedup_seen.insert((queue, key.clone()), id.clone());
        }
        cycle.dirty = true;

        results.push(PerJob::Pending(EnqueueResponse {
            job_id: id,
            deduplicated: false,
        }));
    }

    results
}

fn apply_reserve(
    store: &Store,
    indexes: &mut Indexes,
    batch: &mut OwnedWriteBatch,
    cycle: &mut Cycle,
    queues: &[String],
    lease_ms: u64,
    max_jobs: usize,
) -> Result<Vec<Job>, Status> {
    let mut jobs = Vec::new();
    for queue in queues {
        if jobs.len() >= max_jobs {
            break;
        }
        let prefix = queue_prefix(queue);
        let id_offset = ready_key_id_offset(queue);

        while jobs.len() < max_jobs {
            let Some((ready_k, attempt)) = indexes.ready.pop_front(&prefix) else {
                break;
            };
            let job_id: String = match ready_k.get(id_offset..).map(std::str::from_utf8) {
                Some(Ok(id)) => id.to_owned(),
                _ => {
                    batch.remove(&store.ready, ready_k);
                    cycle.dirty = true;
                    continue;
                }
            };

            let stored = match store.payloads.get(job_id.as_bytes()) {
                Ok(Some(stored)) => stored,
                Ok(None) => {
                    batch.remove(&store.ready, ready_k);
                    cycle.dirty = true;
                    continue;
                }
                Err(e) => {
                    indexes.ready.insert(ready_k, attempt);
                    if jobs.is_empty() {
                        return Err(stg_err(e));
                    }
                    return Ok(jobs);
                }
            };

            let (job_queue, mut job) = match decode_job(&stored) {
                Ok(decoded) => decoded,
                Err(e) => {
                    warn!(error = %e, "reserve skipping corrupt job");
                    batch.remove(&store.ready, ready_k);
                    cycle.dirty = true;
                    continue;
                }
            };

            let lease_expires_at = now_ms() + lease_ms as i64;
            job.attempt = attempt;
            job.lease_expires_at = lease_expires_at;

            let inflight = Inflight {
                attempt,
                lease_expires_at,
                enqueued_at: job.enqueued_at,
                priority: job.priority,
                max_attempts: job.max_attempts,
                queue: job_queue,
            };
            batch.remove(&store.ready, ready_k);
            batch.insert(
                &store.inflight,
                job.id.clone().into_bytes(),
                encode_inflight(&inflight),
            );
            let lease_timer = timer_key(lease_expires_at, &job.id);
            batch.insert(&store.leases, lease_timer.clone(), Vec::new());
            indexes.leases.insert(lease_timer);
            cycle.dirty = true;
            jobs.push(job);
        }
    }

    Ok(jobs)
}

fn apply_ack(
    store: &Store,
    indexes: &mut Indexes,
    batch: &mut OwnedWriteBatch,
    cycle: &mut Cycle,
    job_id: &str,
    attempt: u32,
) -> Result<(), Status> {
    let stored = store
        .inflight
        .get(job_id.as_bytes())
        .map_err(stg_err)?
        .ok_or_else(|| Status::not_found("job not found"))?;
    let inflight = decode_inflight(&stored)?;
    if inflight.attempt != attempt {
        return Err(Status::failed_precondition("attempt mismatch"));
    }
    batch.remove(&store.payloads, job_id.as_bytes().to_vec());
    batch.remove(&store.inflight, job_id.as_bytes().to_vec());
    let lease_timer = timer_key(inflight.lease_expires_at, job_id);
    batch.remove(&store.leases, lease_timer.clone());
    indexes.leases.remove(&lease_timer);
    cycle.dirty = true;
    Ok(())
}

fn apply_nack(
    store: &Store,
    indexes: &mut Indexes,
    batch: &mut OwnedWriteBatch,
    cycle: &mut Cycle,
    req: NackRequest,
) -> Result<bool, Status> {
    let stored = store
        .inflight
        .get(req.job_id.as_bytes())
        .map_err(stg_err)?
        .ok_or_else(|| Status::not_found("job not found"))?;
    let inflight = decode_inflight(&stored)?;
    if inflight.attempt != req.attempt {
        return Err(Status::failed_precondition("attempt mismatch"));
    }
    let lease_timer = timer_key(inflight.lease_expires_at, &req.job_id);

    let strategy = req.retry.as_ref().and_then(|r| r.strategy.as_ref());
    let force_dead_letter = matches!(strategy, Some(nack_retry::Strategy::DeadLetter(_)));
    let retry_delay_ms = match strategy {
        Some(nack_retry::Strategy::DelayMs(ms)) => (*ms).min(store.params.max_schedule_horizon_ms),
        _ => 0,
    };
    if force_dead_letter || inflight.attempt >= inflight.max_attempts {
        let cause = if force_dead_letter {
            DeadLetterCause::Rejected
        } else {
            DeadLetterCause::AttemptsExhausted
        };
        batch.insert(
            &store.dead_letters,
            req.job_id.clone().into_bytes(),
            encode_dead_letter(now_ms(), cause),
        );
        batch.remove(&store.inflight, req.job_id.into_bytes());
        batch.remove(&store.leases, lease_timer.clone());
        indexes.leases.remove(&lease_timer);
        cycle.dirty = true;
        return Ok(true);
    }

    let attempt = inflight.attempt + 1;
    if retry_delay_ms > 0 {
        let deadline = now_ms().saturating_add(i64::try_from(retry_delay_ms).unwrap_or(i64::MAX));
        let tk = timer_key(deadline, &req.job_id);
        batch.insert(&store.scheduled, tk.clone(), attempt.to_be_bytes().to_vec());
        indexes.scheduled.insert(tk);
    } else {
        let rk = ready_key(
            &inflight.queue,
            inflight.priority,
            inflight.enqueued_at,
            &req.job_id,
        );
        batch.insert(&store.ready, rk.clone(), attempt.to_be_bytes().to_vec());
        indexes.ready.insert(rk, attempt);
        cycle.new_ready.insert(inflight.queue);
    }
    batch.remove(&store.inflight, req.job_id.into_bytes());
    batch.remove(&store.leases, lease_timer.clone());
    indexes.leases.remove(&lease_timer);
    cycle.dirty = true;
    Ok(false)
}

fn apply_extend(
    store: &Store,
    indexes: &mut Indexes,
    batch: &mut OwnedWriteBatch,
    cycle: &mut Cycle,
    req: ExtendRequest,
) -> Result<i64, Status> {
    let stored = store
        .inflight
        .get(req.job_id.as_bytes())
        .map_err(stg_err)?
        .ok_or_else(|| Status::not_found("job not found"))?;
    let mut inflight = decode_inflight(&stored)?;
    if inflight.attempt != req.attempt {
        return Err(Status::failed_precondition("attempt mismatch"));
    }
    let old_timer = timer_key(inflight.lease_expires_at, &req.job_id);
    let lease_expires_at = now_ms() + req.lease_duration_ms as i64;
    inflight.lease_expires_at = lease_expires_at;

    batch.insert(
        &store.inflight,
        req.job_id.clone().into_bytes(),
        encode_inflight(&inflight),
    );
    batch.remove(&store.leases, old_timer.clone());
    indexes.leases.remove(&old_timer);
    let new_timer = timer_key(lease_expires_at, &req.job_id);
    batch.insert(&store.leases, new_timer.clone(), Vec::new());
    indexes.leases.insert(new_timer);
    cycle.dirty = true;
    Ok(lease_expires_at)
}

fn apply_sweep(
    store: &Store,
    indexes: &mut Indexes,
    batch: &mut OwnedWriteBatch,
    cycle: &mut Cycle,
) -> Result<usize, Status> {
    let now = now_ms();
    let mut processed = 0usize;

    // Each phase gets its own budget so a backlog of one timer kind cannot
    // starve another — most importantly, scheduled promotions must not crowd
    // out lease-expiry redelivery.
    let mut budget = store.params.sweep_limit;
    while budget > 0 {
        let Some(timer_k) = indexes.scheduled.pop_due(now) else {
            break;
        };
        budget -= 1;
        processed += 1;
        let attempt_hint = store
            .scheduled
            .get(&timer_k)
            .map_err(stg_err)?
            .and_then(|v| read_u32(&v, 0));
        batch.remove(&store.scheduled, timer_k.clone());
        cycle.dirty = true;

        let Some(job_id) = timer_k.get(8..) else {
            continue;
        };
        let Some(stored) = store.payloads.get(job_id).map_err(stg_err)? else {
            continue;
        };
        let (queue, job) = match decode_job(&stored) {
            Ok(decoded) => decoded,
            Err(e) => {
                warn!(error = %e, "sweep skipping corrupt job");
                continue;
            }
        };
        let attempt = attempt_hint.unwrap_or(job.attempt);
        let rk = ready_key(&queue, job.priority, job.enqueued_at, &job.id);
        batch.insert(&store.ready, rk.clone(), attempt.to_be_bytes().to_vec());
        indexes.ready.insert(rk, attempt);
        cycle.new_ready.insert(queue);
    }

    let mut budget = store.params.sweep_limit;
    while budget > 0 {
        let Some(timer_k) = indexes.leases.pop_due(now) else {
            break;
        };
        budget -= 1;
        processed += 1;
        batch.remove(&store.leases, timer_k.clone());
        cycle.dirty = true;

        let Some(job_id) = timer_k.get(8..) else {
            continue;
        };
        let Some(stored) = store.inflight.get(job_id).map_err(stg_err)? else {
            continue;
        };
        let inflight = match decode_inflight(&stored) {
            Ok(decoded) => decoded,
            Err(e) => {
                warn!(error = %e, "sweep skipping corrupt inflight record");
                continue;
            }
        };

        if inflight.attempt >= inflight.max_attempts {
            batch.insert(
                &store.dead_letters,
                job_id.to_vec(),
                encode_dead_letter(now, DeadLetterCause::LeaseExpired),
            );
            batch.remove(&store.inflight, job_id.to_vec());
        } else {
            let Ok(job_id_str) = std::str::from_utf8(job_id) else {
                continue;
            };
            let attempt = inflight.attempt + 1;
            let rk = ready_key(
                &inflight.queue,
                inflight.priority,
                inflight.enqueued_at,
                job_id_str,
            );
            batch.insert(&store.ready, rk.clone(), attempt.to_be_bytes().to_vec());
            indexes.ready.insert(rk, attempt);
            batch.remove(&store.inflight, job_id.to_vec());
            cycle.new_ready.insert(inflight.queue);
        }
    }

    let mut budget = store.params.sweep_limit;
    while budget > 0 {
        let Some(timer_k) = indexes.dedup_timers.pop_due(now) else {
            break;
        };
        budget -= 1;
        processed += 1;
        if let Some(dedup_k) = timer_k.get(8..) {
            batch.remove(&store.dedup, dedup_k.to_vec());
        }
        batch.remove(&store.dedup_timers, timer_k.clone());
        cycle.dirty = true;
    }

    Ok(processed)
}

#[derive(Clone, Default)]
struct QueueNotifiers {
    map: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
}

impl QueueNotifiers {
    fn get(&self, queue: &str) -> Arc<Notify> {
        Arc::clone(
            self.map
                .lock()
                .unwrap()
                .entry(queue.to_owned())
                .or_insert_with(|| Arc::new(Notify::new())),
        )
    }

    fn wake(&self, queue: &str) {
        if let Some(notify) = self.map.lock().unwrap().get(queue) {
            notify.notify_waiters();
        }
    }
}

pub struct JobWaiter {
    notifies: Vec<Arc<Notify>>,
}

impl JobWaiter {
    pub fn arm(&self) -> Armed<'_> {
        let waiters = self
            .notifies
            .iter()
            .map(|notify| {
                let mut waiter = Box::pin(notify.notified());
                waiter.as_mut().enable();
                waiter
            })
            .collect();
        Armed { waiters }
    }
}

pub struct Armed<'a> {
    waiters: Vec<Pin<Box<Notified<'a>>>>,
}

impl Future for Armed<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        for waiter in &mut self.get_mut().waiters {
            if waiter.as_mut().poll(cx).is_ready() {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    }
}

#[derive(Clone)]
pub struct Storage {
    tx: flume::Sender<Command>,
    notifiers: QueueNotifiers,
}

impl Storage {
    pub fn open(config: &Config) -> Result<Self, fjall::Error> {
        let mut builder = Database::builder(config.server.db_path.as_str());
        if let Some(bytes) = config.storage.cache_size_bytes {
            builder = builder.cache_size(bytes);
        }
        if let Some(bytes) = config.storage.max_journaling_size_bytes {
            builder = builder.max_journaling_size(bytes);
        }
        let db = builder.open()?;
        let params = StorageParams {
            persist_mode: match config.storage.persist_mode {
                crate::config::PersistMode::SyncAll => PersistMode::SyncAll,
                crate::config::PersistMode::SyncData => PersistMode::SyncData,
            },
            sweep_limit: config.storage.sweep_limit,
            dedup_window_ms: config.storage.dedup_window_ms,
            default_max_attempts: config.limits.default_max_attempts,
            default_priority: config.limits.default_priority,
            max_attempts_ceiling: config.limits.max_attempts_ceiling,
            max_schedule_horizon_ms: config.limits.max_schedule_horizon_ms,
        };
        let store = Store {
            payloads: db.keyspace("payloads", KeyspaceCreateOptions::default)?,
            inflight: db.keyspace("inflight", KeyspaceCreateOptions::default)?,
            dead_letters: db.keyspace("dead_letters", KeyspaceCreateOptions::default)?,
            ready: db.keyspace("ready", KeyspaceCreateOptions::default)?,
            dedup: db.keyspace("dedup", KeyspaceCreateOptions::default)?,
            dedup_timers: db.keyspace("dedup_timers", KeyspaceCreateOptions::default)?,
            scheduled: db.keyspace("scheduled", KeyspaceCreateOptions::default)?,
            leases: db.keyspace("leases", KeyspaceCreateOptions::default)?,
            db,
            params,
        };
        let indexes = rebuild_indexes(&store)?;

        let (tx, rx) = flume::bounded(config.storage.command_queue_capacity);
        let notifiers = QueueNotifiers::default();
        let max_sweep_interval = Duration::from_millis(config.storage.sweep_interval_ms);
        std::thread::Builder::new()
            .name("sepp-committer".to_string())
            .spawn({
                let notifiers = notifiers.clone();
                move || run_committer(store, indexes, rx, notifiers, max_sweep_interval)
            })
            .expect("failed to spawn committer thread");

        Ok(Self { tx, notifiers })
    }

    pub fn job_waiter(&self, queues: &[String]) -> JobWaiter {
        JobWaiter {
            notifies: queues.iter().map(|q| self.notifiers.get(q)).collect(),
        }
    }

    async fn send<T>(&self, make: impl FnOnce(oneshot::Sender<T>) -> Command) -> Result<T, Status> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send_async(make(resp_tx))
            .await
            .map_err(|_| Status::internal("storage unavailable"))?;
        resp_rx
            .await
            .map_err(|_| Status::internal("storage unavailable"))
    }

    pub async fn enqueue(&self, jobs: Vec<EnqueueRequest>) -> Result<Vec<JobResult>, Status> {
        self.send(|resp| Command::Enqueue { jobs, resp }).await
    }

    pub async fn reserve_once(
        &self,
        queues: Vec<String>,
        lease_ms: u64,
        max_jobs: usize,
    ) -> Result<Vec<Job>, Status> {
        self.send(|resp| Command::Reserve {
            queues,
            lease_ms,
            max_jobs,
            resp,
        })
        .await?
    }

    pub async fn ack(&self, job_id: String, attempt: u32) -> Result<(), Status> {
        self.send(|resp| Command::Ack {
            job_id,
            attempt,
            resp,
        })
        .await?
    }

    pub async fn nack(&self, req: NackRequest) -> Result<bool, Status> {
        self.send(|resp| Command::Nack { req, resp }).await?
    }

    pub async fn extend(&self, req: ExtendRequest) -> Result<i64, Status> {
        self.send(|resp| Command::Extend { req, resp }).await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_job(id: &str) -> Job {
        Job {
            id: id.to_string(),
            job_type: "unit-test".to_string(),
            enqueued_at: 1_700_000_000_000,
            priority: 5,
            attempt: 1,
            max_attempts: 3,
            ..Default::default()
        }
    }

    fn job_id_of<'a>(queue: &str, ready_k: &'a [u8]) -> &'a str {
        std::str::from_utf8(&ready_k[ready_key_id_offset(queue)..]).unwrap()
    }

    #[test]
    fn queue_prefix_is_length_prefixed() {
        assert_eq!(queue_prefix("ab"), vec![0, 2, b'a', b'b']);
        assert_eq!(queue_prefix(""), vec![0, 0]);
    }

    #[test]
    fn ready_key_id_offset_locates_the_job_id() {
        let key = ready_key("orders", 3, 55, "the-job-id");
        assert_eq!(job_id_of("orders", &key), "the-job-id");
    }

    #[test]
    fn ready_key_priority_clamps_above_nine() {
        let prio_offset = 2 + "q".len();
        let nine = ready_key("q", 9, 0, "j");
        let huge = ready_key("q", 1000, 0, "j");
        assert_eq!(nine[prio_offset], huge[prio_offset]);
    }

    #[test]
    fn timer_key_round_trips_through_deadline_of() {
        assert_eq!(deadline_of(&timer_key(12345, "x")), 12345);
        assert_eq!(deadline_of(&timer_key(0, "x")), 0);
    }

    #[test]
    fn dedup_timer_key_carries_deadline_and_key() {
        let k = dedup_timer_key(777, b"the-dedup-key");
        assert_eq!(deadline_of(&k), 777);
        assert_eq!(&k[8..], b"the-dedup-key");
    }

    #[test]
    fn dedup_encoding_round_trips() {
        let bytes = encode_dedup(42, "job-7");
        assert_eq!(decode_dedup(&bytes), Some((42, "job-7")));
    }

    #[test]
    fn decode_dedup_rejects_short_and_invalid_input() {
        assert_eq!(decode_dedup(&[]), None);
        assert_eq!(decode_dedup(&[0, 0, 0, 1]), None);
        let mut bad = 1i64.to_be_bytes().to_vec();
        bad.extend_from_slice(&[0xff, 0xff]);
        assert_eq!(decode_dedup(&bad), None);
    }

    #[test]
    fn job_encoding_round_trips_with_queue() {
        let job = sample_job("job-42");
        let (queue, decoded) = decode_job(&encode_job("orders", &job)).expect("decodes");
        assert_eq!(queue, "orders");
        assert_eq!(decoded, job);
    }

    #[test]
    fn decode_job_rejects_corrupt_input() {
        assert!(decode_job(&[]).is_err());
        assert!(decode_job(&[0]).is_err());
        assert!(decode_job(&[0, 5, 1, 2]).is_err());
    }

    #[test]
    fn inflight_encoding_round_trips() {
        let s = Inflight {
            attempt: 4,
            lease_expires_at: 1_700_000_999_000,
            enqueued_at: 1_700_000_000_000,
            priority: 7,
            max_attempts: 10,
            queue: "my-queue".to_string(),
        };
        let d = decode_inflight(&encode_inflight(&s)).expect("decodes");
        assert_eq!(d.attempt, s.attempt);
        assert_eq!(d.lease_expires_at, s.lease_expires_at);
        assert_eq!(d.enqueued_at, s.enqueued_at);
        assert_eq!(d.priority, s.priority);
        assert_eq!(d.max_attempts, s.max_attempts);
        assert_eq!(d.queue, s.queue);
    }

    #[test]
    fn decode_inflight_rejects_truncated_input() {
        assert!(decode_inflight(&[]).is_err());
        assert!(decode_inflight(&[0u8; 20]).is_err());
    }

    #[test]
    fn decode_inflight_rejects_invalid_queue_utf8() {
        let mut bytes = encode_inflight(&Inflight {
            attempt: 1,
            lease_expires_at: 0,
            enqueued_at: 0,
            priority: 0,
            max_attempts: 1,
            queue: String::new(),
        });
        bytes.extend_from_slice(&[0xff, 0xff]);
        assert!(decode_inflight(&bytes).is_err());
    }

    #[test]
    fn dead_letter_marker_carries_timestamp_and_cause() {
        let v = encode_dead_letter(123_456, DeadLetterCause::LeaseExpired);
        assert_eq!(v.len(), 9);
        assert_eq!(i64::from_be_bytes(v[..8].try_into().unwrap()), 123_456);
        assert_eq!(v[8], 2);
        assert_eq!(encode_dead_letter(0, DeadLetterCause::Rejected)[8], 0);
        assert_eq!(
            encode_dead_letter(0, DeadLetterCause::AttemptsExhausted)[8],
            1
        );
    }

    #[test]
    fn ready_index_pops_highest_priority_first() {
        let mut idx = ReadyIndex::default();
        idx.insert(ready_key("q", 0, 100, "low"), 1);
        idx.insert(ready_key("q", 9, 100, "high"), 1);
        idx.insert(ready_key("q", 5, 100, "mid"), 1);

        let prefix = queue_prefix("q");
        let (k, _) = idx.pop_front(&prefix).unwrap();
        assert_eq!(job_id_of("q", &k), "high");
        let (k, _) = idx.pop_front(&prefix).unwrap();
        assert_eq!(job_id_of("q", &k), "mid");
        let (k, _) = idx.pop_front(&prefix).unwrap();
        assert_eq!(job_id_of("q", &k), "low");
        assert!(idx.pop_front(&prefix).is_none());
    }

    #[test]
    fn ready_index_is_fifo_within_a_priority() {
        let mut idx = ReadyIndex::default();
        idx.insert(ready_key("q", 5, 200, "second"), 1);
        idx.insert(ready_key("q", 5, 100, "first"), 1);

        let prefix = queue_prefix("q");
        let (k, _) = idx.pop_front(&prefix).unwrap();
        assert_eq!(job_id_of("q", &k), "first");
        let (k, _) = idx.pop_front(&prefix).unwrap();
        assert_eq!(job_id_of("q", &k), "second");
    }

    #[test]
    fn ready_index_isolates_queues() {
        let mut idx = ReadyIndex::default();
        idx.insert(ready_key("qa", 5, 100, "a-job"), 1);
        idx.insert(ready_key("qb", 5, 100, "b-job"), 1);

        let (k, _) = idx.pop_front(&queue_prefix("qa")).unwrap();
        assert_eq!(job_id_of("qa", &k), "a-job");
        assert!(idx.pop_front(&queue_prefix("qa")).is_none());

        let (k, _) = idx.pop_front(&queue_prefix("qb")).unwrap();
        assert_eq!(job_id_of("qb", &k), "b-job");
    }

    #[test]
    fn ready_index_does_not_leak_across_queue_name_prefixes() {
        let mut idx = ReadyIndex::default();
        idx.insert(ready_key("aa", 5, 100, "aa-job"), 1);

        assert!(idx.pop_front(&queue_prefix("a")).is_none());
        let (k, _) = idx.pop_front(&queue_prefix("aa")).unwrap();
        assert_eq!(job_id_of("aa", &k), "aa-job");
    }

    #[test]
    fn ready_index_preserves_the_attempt() {
        let mut idx = ReadyIndex::default();
        idx.insert(ready_key("q", 5, 100, "j"), 7);
        let (_, attempt) = idx.pop_front(&queue_prefix("q")).unwrap();
        assert_eq!(attempt, 7);
    }

    #[test]
    fn timer_index_pops_in_deadline_order() {
        let mut idx = TimerIndex::default();
        idx.insert(timer_key(300, "c"));
        idx.insert(timer_key(100, "a"));
        idx.insert(timer_key(200, "b"));

        assert_eq!(idx.pop_due(i64::MAX), Some(timer_key(100, "a")));
        assert_eq!(idx.pop_due(i64::MAX), Some(timer_key(200, "b")));
        assert_eq!(idx.pop_due(i64::MAX), Some(timer_key(300, "c")));
        assert_eq!(idx.pop_due(i64::MAX), None);
    }

    #[test]
    fn timer_index_pop_due_respects_the_now_boundary() {
        let mut idx = TimerIndex::default();
        idx.insert(timer_key(100, "a"));

        assert_eq!(idx.pop_due(99), None);
        assert_eq!(idx.pop_due(100), Some(timer_key(100, "a")));
    }

    #[test]
    fn timer_index_only_yields_due_entries() {
        let mut idx = TimerIndex::default();
        idx.insert(timer_key(100, "a"));
        idx.insert(timer_key(500, "b"));

        assert_eq!(idx.pop_due(200), Some(timer_key(100, "a")));
        assert_eq!(idx.pop_due(200), None);
        assert_eq!(idx.pop_due(500), Some(timer_key(500, "b")));
    }

    #[test]
    fn timer_index_earliest_reports_the_lowest_deadline() {
        let mut idx = TimerIndex::default();
        assert_eq!(idx.earliest(), None);

        idx.insert(timer_key(300, "c"));
        idx.insert(timer_key(100, "a"));
        idx.insert(timer_key(200, "b"));
        assert_eq!(idx.earliest(), Some(100));

        idx.remove(&timer_key(100, "a"));
        assert_eq!(idx.earliest(), Some(200));
    }

    #[test]
    fn next_deadline_is_the_minimum_across_every_timer_index() {
        let mut indexes = Indexes::default();
        assert_eq!(next_deadline(&indexes), None);

        indexes.scheduled.insert(timer_key(500, "s"));
        indexes.leases.insert(timer_key(200, "l"));
        indexes.dedup_timers.insert(dedup_timer_key(800, b"d"));
        assert_eq!(next_deadline(&indexes), Some(200));

        indexes.leases.remove(&timer_key(200, "l"));
        assert_eq!(next_deadline(&indexes), Some(500));
    }

    #[test]
    fn timer_index_remove_drops_the_entry() {
        let mut idx = TimerIndex::default();
        idx.insert(timer_key(100, "a"));
        idx.remove(&timer_key(100, "a"));
        assert_eq!(idx.pop_due(i64::MAX), None);
    }
}
