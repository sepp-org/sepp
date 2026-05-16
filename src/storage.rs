use std::{
    collections::{BTreeSet, HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fjall::{Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode};
use prost::Message;
use tokio::sync::{Notify, futures::Notified, mpsc, oneshot};
use tonic::Status;
use tracing::{error, warn};
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
}

struct Store {
    db: Database,
    jobs: Keyspace,
    dead_letters: Keyspace,
    ready: Keyspace,
    dedup: Keyspace,
    dedup_timers: Keyspace,
    scheduled: Keyspace,
    leases: Keyspace,
    params: StorageParams,
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

#[derive(Clone, Copy)]
enum DeadLetterCause {
    Rejected = 0,
    AttemptsExhausted = 1,
    LeaseExpired = 2,
}

fn encode_dead_letter(
    dead_lettered_at: i64,
    cause: DeadLetterCause,
    queue: &str,
    job: &Job,
) -> Vec<u8> {
    let mut v = Vec::with_capacity(9 + 2 + queue.len());
    v.extend_from_slice(&dead_lettered_at.to_be_bytes());
    v.push(cause as u8);
    v.extend_from_slice(&encode_job(queue, job));
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
    keys: BTreeSet<Vec<u8>>,
}

impl ReadyIndex {
    fn insert(&mut self, ready_key: Vec<u8>) {
        self.keys.insert(ready_key);
    }

    fn pop_front(&mut self, queue_prefix: &[u8]) -> Option<Vec<u8>> {
        let key = self
            .keys
            .range(queue_prefix.to_vec()..)
            .next()
            .filter(|k| k.starts_with(queue_prefix))
            .cloned()?;
        self.keys.remove(&key);
        Some(key)
    }
}

fn rebuild_ready_index(store: &Store) -> Result<ReadyIndex, fjall::Error> {
    let mut index = ReadyIndex::default();
    for guard in store.ready.iter() {
        let (key, _value) = guard.into_inner()?;
        index.insert(key.to_vec());
    }
    Ok(index)
}

fn resync_ready_index(store: &Store, ready: &mut ReadyIndex) {
    match rebuild_ready_index(store) {
        Ok(fresh) => *ready = fresh,
        Err(e) => error!(error = %e, "could not re-sync the ready index"),
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
    Sweep,
}

type Responder = Box<dyn FnOnce(&Result<(), Status>) + Send>;

#[derive(Default)]
struct Cycle {
    dirty: bool,
    new_ready: HashSet<String>,
    dedup_seen: HashMap<(String, String), String>,
}

fn run_committer(
    store: Store,
    mut ready: ReadyIndex,
    mut rx: mpsc::UnboundedReceiver<Command>,
    notifiers: QueueNotifiers,
) {
    while let Some(first) = rx.blocking_recv() {
        let mut rpcs = vec![first];
        let mut sweep_due = false;
        while let Ok(c) = rx.try_recv() {
            rpcs.push(c);
        }
        rpcs.retain(|c| {
            if matches!(c, Command::Sweep) {
                sweep_due = true;
                false
            } else {
                true
            }
        });

        if !rpcs.is_empty() {
            run_rpc_cycle(&store, &mut ready, &notifiers, rpcs);
        }
        if sweep_due {
            run_sweep_cycle(&store, &mut ready, &notifiers);
        }
    }
}

fn run_rpc_cycle(
    store: &Store,
    ready: &mut ReadyIndex,
    notifiers: &QueueNotifiers,
    rpcs: Vec<Command>,
) {
    let mut batch = store.db.batch();
    let mut cycle = Cycle::default();
    let mut responders: Vec<Responder> = Vec::new();

    for cmd in rpcs {
        match cmd {
            Command::Enqueue { jobs, resp } => {
                let per_jobs = apply_enqueue(store, ready, &mut batch, &mut cycle, jobs);
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
                store, ready, &mut batch, &mut cycle, &queues, lease_ms, max_jobs,
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
            } => match apply_ack(store, &mut batch, &mut cycle, &job_id, attempt) {
                Ok(()) => responders.push(Box::new(move |o| {
                    let _ = resp.send(o.clone());
                })),
                Err(e) => {
                    let _ = resp.send(Err(e));
                }
            },
            Command::Nack { req, resp } => {
                match apply_nack(store, ready, &mut batch, &mut cycle, req) {
                    Ok(dead_lettered) => responders.push(Box::new(move |o| {
                        let _ = resp.send(o.clone().map(|()| dead_lettered));
                    })),
                    Err(e) => {
                        let _ = resp.send(Err(e));
                    }
                }
            }
            Command::Extend { req, resp } => {
                match apply_extend(store, &mut batch, &mut cycle, req) {
                    Ok(lease_expires_at) => responders.push(Box::new(move |o| {
                        let _ = resp.send(o.clone().map(|()| lease_expires_at));
                    })),
                    Err(e) => {
                        let _ = resp.send(Err(e));
                    }
                }
            }
            Command::Sweep => unreachable!("sweep pokes are partitioned out"),
        }
    }

    let outcome = if cycle.dirty {
        commit_and_persist(store, batch)
    } else {
        Ok(())
    };
    if outcome.is_err() {
        resync_ready_index(store, ready);
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

fn run_sweep_cycle(store: &Store, ready: &mut ReadyIndex, notifiers: &QueueNotifiers) {
    let mut batch = store.db.batch();
    let mut cycle = Cycle::default();

    if let Err(e) = apply_sweep(store, ready, &mut batch, &mut cycle) {
        warn!(error = %e, "timer sweep aborted");
        resync_ready_index(store, ready);
        return;
    }
    let outcome = if cycle.dirty {
        commit_and_persist(store, batch)
    } else {
        Ok(())
    };
    if outcome.is_err() {
        resync_ready_index(store, ready);
        return;
    }
    for queue in &cycle.new_ready {
        notifiers.wake(queue);
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
    ready: &mut ReadyIndex,
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
            priority: req.priority.unwrap_or(0),
            trace_context: req.trace_context,
            enqueued_at: now,
            attempt: 1,
            max_attempts: req
                .max_attempts
                .unwrap_or(store.params.default_max_attempts),
            lease_expires_at: 0,
            custom: req.custom,
            scheduled_at: req.scheduled_at,
        };

        batch.insert(
            &store.jobs,
            id.clone().into_bytes(),
            encode_job(&queue, &job),
        );
        match job.scheduled_at {
            Some(at) if at > now => {
                batch.insert(
                    &store.scheduled,
                    timer_key(at, &id),
                    id.clone().into_bytes(),
                );
            }
            _ => {
                let rk = ready_key(&queue, job.priority, job.enqueued_at, &id);
                batch.insert(&store.ready, rk.clone(), Vec::new());
                ready.insert(rk);
                cycle.new_ready.insert(queue.clone());
            }
        }
        if let Some(key) = &req.idempotency_key {
            let dkey = dedup_key(&queue, key);
            if let Some(old_timer) = stale_dedup_timer {
                batch.remove(&store.dedup_timers, old_timer);
            }
            batch.insert(
                &store.dedup_timers,
                dedup_timer_key(now + store.params.dedup_window_ms, &dkey),
                Vec::new(),
            );
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
    ready: &mut ReadyIndex,
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
            let Some(ready_k) = ready.pop_front(&prefix) else {
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

            let stored = match store.jobs.get(job_id.as_bytes()) {
                Ok(Some(stored)) => stored,
                Ok(None) => {
                    batch.remove(&store.ready, ready_k);
                    cycle.dirty = true;
                    continue;
                }
                Err(e) => {
                    ready.insert(ready_k);
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

            job.lease_expires_at = now_ms() + lease_ms as i64;
            batch.remove(&store.ready, ready_k);
            batch.insert(
                &store.jobs,
                job.id.clone().into_bytes(),
                encode_job(&job_queue, &job),
            );
            batch.insert(
                &store.leases,
                timer_key(job.lease_expires_at, &job.id),
                job.id.clone().into_bytes(),
            );
            cycle.dirty = true;
            jobs.push(job);
        }
    }

    Ok(jobs)
}

fn apply_ack(
    store: &Store,
    batch: &mut OwnedWriteBatch,
    cycle: &mut Cycle,
    job_id: &str,
    attempt: u32,
) -> Result<(), Status> {
    let stored = store
        .jobs
        .get(job_id.as_bytes())
        .map_err(stg_err)?
        .ok_or_else(|| Status::not_found("job not found"))?;
    let (_queue, job) = decode_job(&stored)?;
    if job.attempt != attempt {
        return Err(Status::failed_precondition("attempt mismatch"));
    }
    batch.remove(&store.jobs, job_id.as_bytes().to_vec());
    batch.remove(&store.leases, timer_key(job.lease_expires_at, job_id));
    cycle.dirty = true;
    Ok(())
}

fn apply_nack(
    store: &Store,
    ready: &mut ReadyIndex,
    batch: &mut OwnedWriteBatch,
    cycle: &mut Cycle,
    req: NackRequest,
) -> Result<bool, Status> {
    let stored = store
        .jobs
        .get(req.job_id.as_bytes())
        .map_err(stg_err)?
        .ok_or_else(|| Status::not_found("job not found"))?;
    let (queue, mut job) = decode_job(&stored)?;
    if job.attempt != req.attempt {
        return Err(Status::failed_precondition("attempt mismatch"));
    }
    let lease_timer = timer_key(job.lease_expires_at, &req.job_id);

    let force_dead_letter = matches!(
        req.retry.as_ref().and_then(|r| r.strategy.as_ref()),
        Some(nack_retry::Strategy::DeadLetter(_))
    );
    if force_dead_letter || job.attempt >= job.max_attempts {
        let cause = if force_dead_letter {
            DeadLetterCause::Rejected
        } else {
            DeadLetterCause::AttemptsExhausted
        };
        batch.insert(
            &store.dead_letters,
            req.job_id.clone().into_bytes(),
            encode_dead_letter(now_ms(), cause, &queue, &job),
        );
        batch.remove(&store.jobs, req.job_id.into_bytes());
        batch.remove(&store.leases, lease_timer);
        cycle.dirty = true;
        return Ok(true);
    }

    job.attempt += 1;
    job.lease_expires_at = 0;
    let rk = ready_key(&queue, job.priority, job.enqueued_at, &req.job_id);
    batch.insert(
        &store.jobs,
        req.job_id.clone().into_bytes(),
        encode_job(&queue, &job),
    );
    batch.insert(&store.ready, rk.clone(), Vec::new());
    ready.insert(rk);
    batch.remove(&store.leases, lease_timer);
    cycle.dirty = true;
    cycle.new_ready.insert(queue);
    Ok(false)
}

fn apply_extend(
    store: &Store,
    batch: &mut OwnedWriteBatch,
    cycle: &mut Cycle,
    req: ExtendRequest,
) -> Result<i64, Status> {
    let stored = store
        .jobs
        .get(req.job_id.as_bytes())
        .map_err(stg_err)?
        .ok_or_else(|| Status::not_found("job not found"))?;
    let (queue, mut job) = decode_job(&stored)?;
    if job.attempt != req.attempt {
        return Err(Status::failed_precondition("attempt mismatch"));
    }
    let old_timer = timer_key(job.lease_expires_at, &req.job_id);
    let lease_expires_at = now_ms() + req.lease_duration_ms as i64;
    job.lease_expires_at = lease_expires_at;

    batch.insert(
        &store.jobs,
        req.job_id.clone().into_bytes(),
        encode_job(&queue, &job),
    );
    batch.remove(&store.leases, old_timer);
    batch.insert(
        &store.leases,
        timer_key(lease_expires_at, &req.job_id),
        req.job_id.into_bytes(),
    );
    cycle.dirty = true;
    Ok(lease_expires_at)
}

fn apply_sweep(
    store: &Store,
    ready: &mut ReadyIndex,
    batch: &mut OwnedWriteBatch,
    cycle: &mut Cycle,
) -> Result<(), Status> {
    let now = now_ms();
    let mut budget = store.params.sweep_limit;

    for guard in store.scheduled.iter() {
        if budget == 0 {
            break;
        }
        let (timer_k, job_id) = guard.into_inner().map_err(stg_err)?;
        if deadline_of(&timer_k) > now {
            break;
        }
        budget -= 1;
        batch.remove(&store.scheduled, timer_k.to_vec());
        cycle.dirty = true;

        let Some(stored) = store.jobs.get(&job_id).map_err(stg_err)? else {
            continue;
        };
        let (queue, job) = match decode_job(&stored) {
            Ok(decoded) => decoded,
            Err(e) => {
                warn!(error = %e, "sweep skipping corrupt job");
                continue;
            }
        };
        let rk = ready_key(&queue, job.priority, job.enqueued_at, &job.id);
        batch.insert(&store.ready, rk.clone(), Vec::new());
        ready.insert(rk);
        cycle.new_ready.insert(queue);
    }

    for guard in store.leases.iter() {
        if budget == 0 {
            break;
        }
        let (timer_k, job_id) = guard.into_inner().map_err(stg_err)?;
        if deadline_of(&timer_k) > now {
            break;
        }
        budget -= 1;
        batch.remove(&store.leases, timer_k.to_vec());
        cycle.dirty = true;

        let Some(stored) = store.jobs.get(&job_id).map_err(stg_err)? else {
            continue;
        };
        let (queue, mut job) = match decode_job(&stored) {
            Ok(decoded) => decoded,
            Err(e) => {
                warn!(error = %e, "sweep skipping corrupt job");
                continue;
            }
        };

        if job.attempt >= job.max_attempts {
            batch.insert(
                &store.dead_letters,
                job_id.to_vec(),
                encode_dead_letter(now, DeadLetterCause::LeaseExpired, &queue, &job),
            );
            batch.remove(&store.jobs, job_id.to_vec());
        } else {
            job.attempt += 1;
            job.lease_expires_at = 0;
            batch.insert(&store.jobs, job_id.to_vec(), encode_job(&queue, &job));
            let rk = ready_key(&queue, job.priority, job.enqueued_at, &job.id);
            batch.insert(&store.ready, rk.clone(), Vec::new());
            ready.insert(rk);
            cycle.new_ready.insert(queue);
        }
    }

    for guard in store.dedup_timers.iter() {
        if budget == 0 {
            break;
        }
        let (timer_k, _) = guard.into_inner().map_err(stg_err)?;
        if deadline_of(&timer_k) > now {
            break;
        }
        budget -= 1;
        if let Some(dedup_k) = timer_k.get(8..) {
            batch.remove(&store.dedup, dedup_k.to_vec());
        }
        batch.remove(&store.dedup_timers, timer_k.to_vec());
        cycle.dirty = true;
    }

    Ok(())
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
    tx: mpsc::UnboundedSender<Command>,
    notifiers: QueueNotifiers,
}

impl Storage {
    pub fn open(config: &Config) -> Result<Self, fjall::Error> {
        let db = Database::builder(config.server.db_path.as_str()).open()?;
        let params = StorageParams {
            persist_mode: match config.storage.persist_mode {
                crate::config::PersistMode::SyncAll => PersistMode::SyncAll,
                crate::config::PersistMode::SyncData => PersistMode::SyncData,
            },
            sweep_limit: config.storage.sweep_limit,
            dedup_window_ms: config.storage.dedup_window_ms,
            default_max_attempts: config.limits.default_max_attempts,
        };
        let store = Store {
            jobs: db.keyspace("jobs", KeyspaceCreateOptions::default)?,
            dead_letters: db.keyspace("dead_letters", KeyspaceCreateOptions::default)?,
            ready: db.keyspace("ready", KeyspaceCreateOptions::default)?,
            dedup: db.keyspace("dedup", KeyspaceCreateOptions::default)?,
            dedup_timers: db.keyspace("dedup_timers", KeyspaceCreateOptions::default)?,
            scheduled: db.keyspace("scheduled", KeyspaceCreateOptions::default)?,
            leases: db.keyspace("leases", KeyspaceCreateOptions::default)?,
            db,
            params,
        };
        let ready_index = rebuild_ready_index(&store)?;

        let (tx, rx) = mpsc::unbounded_channel();
        let notifiers = QueueNotifiers::default();
        std::thread::Builder::new()
            .name("sepp-committer".to_string())
            .spawn({
                let notifiers = notifiers.clone();
                move || run_committer(store, ready_index, rx, notifiers)
            })
            .expect("failed to spawn committer thread");

        let sweep_interval = Duration::from_millis(config.storage.sweep_interval_ms);
        std::thread::Builder::new()
            .name("sepp-sweep-ticker".to_string())
            .spawn({
                let tx = tx.clone();
                move || {
                    loop {
                        std::thread::sleep(sweep_interval);
                        if tx.send(Command::Sweep).is_err() {
                            break;
                        }
                    }
                }
            })
            .expect("failed to spawn sweep ticker thread");

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
            .send(make(resp_tx))
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
