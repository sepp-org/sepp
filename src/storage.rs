use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fjall::{
    KeyspaceCreateOptions, KvSeparationOptions, PersistMode, Readable,
    SingleWriterTxDatabase as TxDatabase, SingleWriterTxKeyspace as TxKeyspace,
    SingleWriterWriteTx as WriteTransaction,
};
use prost::Message;
use tokio::sync::{Notify, futures::Notified, oneshot};
use tonic::Status;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::metrics::{CycleMetrics, Metrics, QueueDepthSnapshot};
use crate::pb::sepp::v1::{
    EnqueueRequest, EnqueueResponse, ExtendRequest, Job, NackRequest, Payload, TraceContext,
    nack_retry,
};
use crate::queues::SharedRegistry;
use crate::telemetry;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as i64
}

struct StorageParams {
    persist_mode: PersistMode,
    sweep_limit: usize,
}

struct Store {
    db: TxDatabase,
    jobs: TxKeyspace,
    payloads: TxKeyspace,
    inflight: TxKeyspace,
    ready: TxKeyspace,
    dedup: TxKeyspace,
    dedup_timers: TxKeyspace,
    scheduled: TxKeyspace,
    leases: TxKeyspace,
    params: StorageParams,
    registry: SharedRegistry,
    metrics: Metrics,
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

fn read_queue(bytes: &[u8]) -> Option<&str> {
    let len = u16::from_be_bytes(*bytes.first_chunk::<2>()?) as usize;
    std::str::from_utf8(bytes.get(2..2 + len)?).ok()
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
    trace_context: Option<TraceContext>,
}

fn encode_inflight(s: &Inflight) -> Vec<u8> {
    let tc_bytes = s
        .trace_context
        .as_ref()
        .map(Message::encode_to_vec)
        .unwrap_or_default();
    let mut v = Vec::with_capacity(30 + s.queue.len() + tc_bytes.len());
    v.extend_from_slice(&s.attempt.to_be_bytes());
    v.extend_from_slice(&s.lease_expires_at.to_be_bytes());
    v.extend_from_slice(&s.enqueued_at.to_be_bytes());
    v.extend_from_slice(&s.priority.to_be_bytes());
    v.extend_from_slice(&s.max_attempts.to_be_bytes());
    v.extend_from_slice(&(s.queue.len() as u16).to_be_bytes());
    v.extend_from_slice(s.queue.as_bytes());
    v.extend_from_slice(&tc_bytes);
    v
}

fn decode_inflight(bytes: &[u8]) -> Result<Inflight, Status> {
    let corrupt = || Status::internal("corrupt inflight record");
    let parse = || -> Option<Inflight> {
        let attempt = read_u32(bytes, 0)?;
        let lease_expires_at = read_i64(bytes, 4)?;
        let enqueued_at = read_i64(bytes, 12)?;
        let priority = read_u32(bytes, 20)?;
        let max_attempts = read_u32(bytes, 24)?;
        let queue_len = u16::from_be_bytes(bytes.get(28..30)?.try_into().ok()?) as usize;
        let queue = std::str::from_utf8(bytes.get(30..30 + queue_len)?)
            .ok()?
            .to_owned();
        let tc_bytes = bytes.get(30 + queue_len..)?;
        let trace_context = if tc_bytes.is_empty() {
            None
        } else {
            Some(TraceContext::decode(tc_bytes).ok()?)
        };
        Some(Inflight {
            attempt,
            lease_expires_at,
            enqueued_at,
            priority,
            max_attempts,
            queue,
            trace_context,
        })
    };
    parse().ok_or_else(corrupt)
}

fn stg_err(e: fjall::Error) -> Status {
    Status::internal(format!("storage error: {e}"))
}

fn bump_queue(map: &mut HashMap<String, u64>, queue: &str) {
    *map.entry(queue.to_string()).or_default() += 1;
}

fn drop_queue(map: &mut HashMap<String, u64>, queue: &str) {
    if let Some(n) = map.get_mut(queue) {
        *n -= 1;
        if *n == 0 {
            map.remove(queue);
        }
    }
}

#[derive(Default)]
struct ReadyIndex {
    keys: BTreeMap<Vec<u8>, u32>,
    by_queue: HashMap<String, u64>,
}

impl ReadyIndex {
    fn insert(&mut self, ready_key: Vec<u8>, attempt: u32) {
        if !self.keys.contains_key(&ready_key)
            && let Some(queue) = read_queue(&ready_key)
        {
            bump_queue(&mut self.by_queue, queue);
        }
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
        if let Some(queue) = read_queue(&key) {
            drop_queue(&mut self.by_queue, queue);
        }
        Some((key, attempt))
    }
}

// Timer keys are `deadline | job_id` and carry no queue, so each key stores
// its owning queue as the map value. Caller passes it on insert; pop_due /
// remove return it so we can keep the by_queue counter in sync without an
// extra DB lookup.
#[derive(Default)]
struct TimerIndex {
    keys: BTreeMap<Vec<u8>, String>,
    by_queue: HashMap<String, u64>,
}

impl TimerIndex {
    fn insert(&mut self, key: Vec<u8>, queue: &str) {
        if !self.keys.contains_key(&key) {
            bump_queue(&mut self.by_queue, queue);
        }
        self.keys.insert(key, queue.to_string());
    }

    fn remove(&mut self, key: &[u8]) -> Option<String> {
        let queue = self.keys.remove(key)?;
        drop_queue(&mut self.by_queue, &queue);
        Some(queue)
    }

    fn pop_due(&mut self, now: i64) -> Option<(Vec<u8>, String)> {
        let (key, _) = self.keys.iter().next()?;
        if deadline_of(key) > now {
            return None;
        }
        let key = key.clone();
        let queue = self.keys.remove(&key)?;
        drop_queue(&mut self.by_queue, &queue);
        Some((key, queue))
    }

    fn earliest(&self) -> Option<i64> {
        self.keys.keys().next().map(|k| deadline_of(k))
    }
}

#[derive(Default)]
struct Indexes {
    ready: ReadyIndex,
    scheduled: TimerIndex,
    leases: TimerIndex,
    dedup_timers: TimerIndex,
}

impl Indexes {
    fn snapshot(&self) -> QueueDepthSnapshot {
        QueueDepthSnapshot {
            ready: self.ready.by_queue.clone(),
            scheduled: self.scheduled.by_queue.clone(),
            inflight: self.leases.by_queue.clone(),
        }
    }
}

fn warn_on_undeclared_persisted_queues(store: &Store) {
    let registry = store.registry.load();
    let mut undeclared: BTreeSet<String> = BTreeSet::new();
    let snap = store.db.read_tx();
    for guard in snap.iter(&store.jobs) {
        let Ok((_, value)) = guard.into_inner() else {
            continue;
        };
        let Some(queue) = read_queue(&value) else {
            continue;
        };
        if !registry.is_declared(queue) {
            undeclared.insert(queue.to_owned());
        }
    }
    if !undeclared.is_empty() {
        warn!(
            queues = ?undeclared,
            "strict mode is on but the database holds jobs in queues that are not declared; \
             new enqueues/reserves on these queues will be rejected"
        );
    }
}

fn rebuild_indexes(store: &Store) -> Result<Indexes, fjall::Error> {
    let mut indexes = Indexes::default();
    let snap = store.db.read_tx();
    for guard in snap.iter(&store.ready) {
        let (key, value) = guard.into_inner()?;
        let attempt = read_u32(&value, 0).unwrap_or(1);
        indexes.ready.insert(key.to_vec(), attempt);
    }
    for guard in snap.iter(&store.scheduled) {
        let (key, _) = guard.into_inner()?;
        let queue = key
            .get(8..)
            .and_then(|job_id| snap.get(&store.jobs, job_id).ok().flatten())
            .and_then(|stored| read_queue(&stored).map(str::to_owned))
            .unwrap_or_default();
        indexes.scheduled.insert(key.to_vec(), &queue);
    }
    for guard in snap.iter(&store.leases) {
        let (key, _) = guard.into_inner()?;
        let queue = key
            .get(8..)
            .and_then(|job_id| snap.get(&store.inflight, job_id).ok().flatten())
            .and_then(|stored| decode_inflight(&stored).ok().map(|i| i.queue))
            .unwrap_or_default();
        indexes.leases.insert(key.to_vec(), &queue);
    }
    for guard in snap.iter(&store.dedup_timers) {
        let (key, _) = guard.into_inner()?;
        let queue = key.get(8..).and_then(read_queue).unwrap_or("").to_string();
        indexes.dedup_timers.insert(key.to_vec(), &queue);
    }
    Ok(indexes)
}

fn resync(store: &Store, indexes: &mut Indexes) {
    match rebuild_indexes(store) {
        Ok(fresh) => *indexes = fresh,
        Err(e) => error!(error = %e, "could not re-sync the in-memory indexes"),
    }
}

enum Command {
    Enqueue {
        jobs: Vec<EnqueueRequest>,
        resp: oneshot::Sender<Result<Vec<EnqueueResponse>, Status>>,
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
        resp: oneshot::Sender<Result<Option<TraceContext>, Status>>,
    },
    Nack {
        req: NackRequest,
        resp: oneshot::Sender<Result<(bool, Option<TraceContext>), Status>>,
    },
    Extend {
        req: ExtendRequest,
        resp: oneshot::Sender<Result<(i64, Option<TraceContext>), Status>>,
    },
}

type Responder = Box<dyn FnOnce(&Result<(), Status>) + Send>;

struct Cycle {
    dirty: bool,
    new_ready: HashSet<String>,
    // `None` when metrics are disabled — every recorder method becomes a no-op
    // and we skip allocating into nine HashMaps that would never be flushed.
    metrics: Option<CycleMetrics>,
}

impl Cycle {
    fn new(metrics_enabled: bool) -> Self {
        Self {
            dirty: false,
            new_ready: HashSet::new(),
            metrics: metrics_enabled.then(CycleMetrics::default),
        }
    }

    fn enqueued(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.enqueued_by_queue, queue);
        }
    }

    fn reserved(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.reserved_by_queue, queue);
        }
    }

    fn acked(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.acked_by_queue, queue);
        }
    }

    fn nacked(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.nacked_by_queue, queue);
        }
    }

    fn deduplicated(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.deduplicated_by_queue, queue);
        }
    }

    fn dead_lettered(&mut self, queue: &str, cause: &'static str) {
        if let Some(m) = self.metrics.as_mut() {
            *m.dead_lettered_by_queue_cause
                .entry((queue.to_string(), cause))
                .or_default() += 1;
        }
    }

    fn sweep_promotion(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.sweep_promotions_by_queue, queue);
        }
    }

    fn sweep_lease_redelivery(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.sweep_lease_redeliveries_by_queue, queue);
        }
    }

    fn sweep_dedup_expiration(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.sweep_dedup_expirations_by_queue, queue);
        }
    }
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
                // Channel closed only when every Storage handle has dropped,
                // which means the gRPC server has already stopped accepting
                // requests.
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

        if store.metrics.is_enabled() {
            store.metrics.set_queue_depths(indexes.snapshot());
        }
    }
    info!("committer thread stopped; storage is no longer accepting commands");
}

fn run_rpc_cycle(
    store: &Store,
    indexes: &mut Indexes,
    notifiers: &QueueNotifiers,
    rpcs: Vec<Command>,
) {
    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(store.metrics.is_enabled());
    let mut responders: Vec<Responder> = Vec::new();

    for cmd in rpcs {
        match cmd {
            Command::Enqueue { jobs, resp } => {
                match apply_enqueue(store, indexes, &mut tx, &mut cycle, jobs) {
                    Ok(enqueued) => responders.push(Box::new(move |o| {
                        let _ = resp.send(o.clone().map(|()| enqueued));
                    })),
                    Err(e) => {
                        let _ = resp.send(Err(e));
                    }
                }
            }
            Command::Reserve {
                queues,
                lease_ms,
                max_jobs,
                resp,
            } => match apply_reserve(
                store, indexes, &mut tx, &mut cycle, &queues, lease_ms, max_jobs,
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
            } => match apply_ack(store, indexes, &mut tx, &mut cycle, &job_id, attempt) {
                Ok(trace_context) => responders.push(Box::new(move |o| {
                    let _ = resp.send(o.clone().map(|()| trace_context));
                })),
                Err(e) => {
                    let _ = resp.send(Err(e));
                }
            },
            Command::Nack { req, resp } => {
                match apply_nack(store, indexes, &mut tx, &mut cycle, req) {
                    Ok(outcome) => responders.push(Box::new(move |o| {
                        let _ = resp.send(o.clone().map(|()| outcome));
                    })),
                    Err(e) => {
                        let _ = resp.send(Err(e));
                    }
                }
            }
            Command::Extend { req, resp } => {
                match apply_extend(store, indexes, &mut tx, &mut cycle, req) {
                    Ok(outcome) => responders.push(Box::new(move |o| {
                        let _ = resp.send(o.clone().map(|()| outcome));
                    })),
                    Err(e) => {
                        let _ = resp.send(Err(e));
                    }
                }
            }
        }
    }

    let outcome = if cycle.dirty {
        commit_and_persist(store, tx)
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
        if let Some(m) = &cycle.metrics {
            store.metrics.flush_cycle(m);
        }
        // Must wake *after* the commit lands. tokio::Notify only delivers to
        // waiters armed before the wake, so a wake issued pre-commit could be
        // followed by an arm+reserve that then fails to see the new ready
        // entry if the commit ultimately fails or is rolled back.
        for queue in &cycle.new_ready {
            notifiers.wake(queue);
        }
    }
}

fn run_sweep_cycle(store: &Store, indexes: &mut Indexes, notifiers: &QueueNotifiers) {
    let started = std::time::Instant::now();
    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(store.metrics.is_enabled());

    let processed = match apply_sweep(store, indexes, &mut tx, &mut cycle) {
        Ok(processed) => processed,
        Err(e) => {
            warn!(error = %e, "timer sweep aborted");
            resync(store, indexes);
            return;
        }
    };
    let outcome = if cycle.dirty {
        commit_and_persist(store, tx)
    } else {
        Ok(())
    };
    if outcome.is_err() {
        resync(store, indexes);
        return;
    }
    if let Some(m) = &cycle.metrics {
        store.metrics.flush_cycle(m);
    }
    // Post-commit, same reason as in run_rpc_cycle.
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

fn commit_and_persist(store: &Store, tx: WriteTransaction<'_>) -> Result<(), Status> {
    let started = std::time::Instant::now();
    let result = tx
        .commit()
        .and_then(|()| store.db.persist(store.params.persist_mode));
    store.metrics.record_commit(started.elapsed());
    match result {
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
    tx: &mut WriteTransaction<'_>,
    cycle: &mut Cycle,
    jobs: Vec<EnqueueRequest>,
) -> Result<Vec<EnqueueResponse>, Status> {
    let now = now_ms();
    let registry = store.registry.load();
    let mut results = Vec::with_capacity(jobs.len());

    for req in jobs {
        let limits = registry.effective(&req.queue);
        let mut stale_dedup_timer: Option<Vec<u8>> = None;

        if let Some(key) = &req.idempotency_key {
            let dkey = dedup_key(&req.queue, key);
            if let Some(existing) = tx.get(&store.dedup, &dkey).map_err(stg_err)? {
                match decode_dedup(&existing) {
                    Some((ts, job_id)) if now - ts < limits.dedup_window_ms => {
                        cycle.deduplicated(&req.queue);
                        results.push(EnqueueResponse {
                            job_id: job_id.to_owned(),
                            deduplicated: true,
                        });
                        continue;
                    }
                    Some((ts, _)) => {
                        stale_dedup_timer =
                            Some(dedup_timer_key(ts + limits.dedup_window_ms, &dkey));
                    }
                    None => {}
                }
            }
        }

        let id = Uuid::new_v4().to_string();
        let queue = req.queue;
        let payload = req.payload;
        let job = Job {
            id: id.clone(),
            job_type: req.job_type,
            payload: None,
            priority: req.priority.unwrap_or(limits.default_priority),
            trace_context: req.trace_context,
            enqueued_at: now,
            attempt: 1,
            max_attempts: req
                .max_attempts
                .unwrap_or(limits.default_max_attempts)
                .min(limits.max_attempts_ceiling),
            lease_expires_at: 0,
            custom: req.custom,
            scheduled_at: req.scheduled_at,
        };

        tx.insert(
            &store.jobs,
            id.clone().into_bytes(),
            encode_job(&queue, &job),
        );
        if let Some(payload) = payload {
            tx.insert(
                &store.payloads,
                id.clone().into_bytes(),
                payload.encode_to_vec(),
            );
        }
        match job.scheduled_at {
            Some(at) if at > now => {
                let tk = timer_key(at, &id);
                tx.insert(
                    &store.scheduled,
                    tk.clone(),
                    job.attempt.to_be_bytes().to_vec(),
                );
                indexes.scheduled.insert(tk, &queue);
            }
            _ => {
                let rk = ready_key(&queue, job.priority, job.enqueued_at, &id);
                tx.insert(&store.ready, rk.clone(), job.attempt.to_be_bytes().to_vec());
                indexes.ready.insert(rk, job.attempt);
                cycle.new_ready.insert(queue.clone());
            }
        }
        if let Some(key) = &req.idempotency_key {
            let dkey = dedup_key(&queue, key);
            if let Some(old_timer) = stale_dedup_timer {
                tx.remove(&store.dedup_timers, old_timer.clone());
                indexes.dedup_timers.remove(&old_timer);
            }
            let dtk = dedup_timer_key(now + limits.dedup_window_ms, &dkey);
            tx.insert(&store.dedup_timers, dtk.clone(), Vec::new());
            indexes.dedup_timers.insert(dtk, &queue);
            tx.insert(&store.dedup, dkey, encode_dedup(now, &id));
        }
        cycle.enqueued(&queue);
        cycle.dirty = true;

        results.push(EnqueueResponse {
            job_id: id,
            deduplicated: false,
        });
    }

    Ok(results)
}

fn apply_reserve(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
    cycle: &mut Cycle,
    queues: &[String],
    lease_ms: u64,
    max_jobs: usize,
) -> Result<Vec<Job>, Status> {
    let now = now_ms();
    let lease_expires_at = now.saturating_add(i64::try_from(lease_ms).unwrap_or(i64::MAX));
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
                    warn!(error = %e, "reserve dropping corrupt job");
                    tx.remove(&store.ready, ready_k);
                    tx.remove(&store.jobs, job_id.as_bytes().to_vec());
                    tx.remove(&store.payloads, job_id.as_bytes().to_vec());
                    cycle.dirty = true;
                    continue;
                }
            };

            match tx.get(&store.payloads, job_id.as_bytes()) {
                Ok(Some(bytes)) => match Payload::decode(&*bytes) {
                    Ok(payload) => job.payload = Some(payload),
                    Err(e) => {
                        warn!(error = %e, "reserve dropping job with corrupt payload");
                        tx.remove(&store.ready, ready_k);
                        tx.remove(&store.jobs, job_id.as_bytes().to_vec());
                        tx.remove(&store.payloads, job_id.as_bytes().to_vec());
                        cycle.dirty = true;
                        continue;
                    }
                },
                Ok(None) => {}
                Err(e) => {
                    indexes.ready.insert(ready_k, attempt);
                    if jobs.is_empty() {
                        return Err(stg_err(e));
                    }
                    return Ok(jobs);
                }
            }

            job.attempt = attempt;
            job.lease_expires_at = lease_expires_at;

            let inflight = Inflight {
                attempt,
                lease_expires_at,
                enqueued_at: job.enqueued_at,
                priority: job.priority,
                max_attempts: job.max_attempts,
                queue: job_queue,
                trace_context: job.trace_context.clone(),
            };
            tx.remove(&store.ready, ready_k);
            tx.insert(
                &store.inflight,
                job.id.clone().into_bytes(),
                encode_inflight(&inflight),
            );
            let lease_timer = timer_key(lease_expires_at, &job.id);
            tx.insert(&store.leases, lease_timer.clone(), Vec::new());
            indexes.leases.insert(lease_timer, &inflight.queue);
            cycle.reserved(&inflight.queue);
            cycle.dirty = true;
            jobs.push(job);
        }
    }

    Ok(jobs)
}

fn apply_ack(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
    cycle: &mut Cycle,
    job_id: &str,
    attempt: u32,
) -> Result<Option<TraceContext>, Status> {
    let stored = tx
        .get(&store.inflight, job_id.as_bytes())
        .map_err(stg_err)?
        .ok_or_else(|| Status::not_found("job not found"))?;
    let inflight = decode_inflight(&stored)?;
    if inflight.attempt != attempt {
        return Err(Status::failed_precondition("attempt mismatch"));
    }
    tx.remove(&store.jobs, job_id.as_bytes().to_vec());
    tx.remove(&store.payloads, job_id.as_bytes().to_vec());
    tx.remove(&store.inflight, job_id.as_bytes().to_vec());
    let lease_timer = timer_key(inflight.lease_expires_at, job_id);
    tx.remove(&store.leases, lease_timer.clone());
    indexes.leases.remove(&lease_timer);
    cycle.acked(&inflight.queue);
    cycle.dirty = true;
    Ok(inflight.trace_context)
}

fn apply_nack(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
    cycle: &mut Cycle,
    req: NackRequest,
) -> Result<(bool, Option<TraceContext>), Status> {
    let now = now_ms();
    let stored = tx
        .get(&store.inflight, req.job_id.as_bytes())
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
        Some(nack_retry::Strategy::DelayMs(ms)) => {
            let max = store
                .registry
                .load()
                .effective(&inflight.queue)
                .max_schedule_horizon_ms;
            (*ms).min(max)
        }
        _ => 0,
    };
    if force_dead_letter || inflight.attempt >= inflight.max_attempts {
        let cause_label = if force_dead_letter {
            "rejected"
        } else {
            "attempts_exhausted"
        };
        tx.remove(&store.jobs, req.job_id.as_bytes().to_vec());
        tx.remove(&store.payloads, req.job_id.as_bytes().to_vec());
        tx.remove(&store.inflight, req.job_id.into_bytes());
        tx.remove(&store.leases, lease_timer.clone());
        indexes.leases.remove(&lease_timer);
        cycle.nacked(&inflight.queue);
        cycle.dead_lettered(&inflight.queue, cause_label);
        cycle.dirty = true;
        return Ok((true, inflight.trace_context));
    }

    let attempt = inflight.attempt + 1;
    if retry_delay_ms > 0 {
        let deadline = now.saturating_add(i64::try_from(retry_delay_ms).unwrap_or(i64::MAX));
        let tk = timer_key(deadline, &req.job_id);
        tx.insert(&store.scheduled, tk.clone(), attempt.to_be_bytes().to_vec());
        indexes.scheduled.insert(tk, &inflight.queue);
    } else {
        let rk = ready_key(
            &inflight.queue,
            inflight.priority,
            inflight.enqueued_at,
            &req.job_id,
        );
        tx.insert(&store.ready, rk.clone(), attempt.to_be_bytes().to_vec());
        indexes.ready.insert(rk, attempt);
        cycle.new_ready.insert(inflight.queue.clone());
    }
    tx.remove(&store.inflight, req.job_id.into_bytes());
    tx.remove(&store.leases, lease_timer.clone());
    indexes.leases.remove(&lease_timer);
    cycle.nacked(&inflight.queue);
    cycle.dirty = true;
    Ok((false, inflight.trace_context))
}

fn apply_extend(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
    cycle: &mut Cycle,
    req: ExtendRequest,
) -> Result<(i64, Option<TraceContext>), Status> {
    let stored = tx
        .get(&store.inflight, req.job_id.as_bytes())
        .map_err(stg_err)?
        .ok_or_else(|| Status::not_found("job not found"))?;
    let mut inflight = decode_inflight(&stored)?;
    if inflight.attempt != req.attempt {
        return Err(Status::failed_precondition("attempt mismatch"));
    }
    let max_lease = store
        .registry
        .load()
        .effective(&inflight.queue)
        .max_lease_duration_ms;
    let old_timer = timer_key(inflight.lease_expires_at, &req.job_id);
    let lease_ms = req.lease_duration_ms.min(max_lease);
    let lease_expires_at = now_ms().saturating_add(i64::try_from(lease_ms).unwrap_or(i64::MAX));
    inflight.lease_expires_at = lease_expires_at;

    tx.insert(
        &store.inflight,
        req.job_id.clone().into_bytes(),
        encode_inflight(&inflight),
    );
    tx.remove(&store.leases, old_timer.clone());
    indexes.leases.remove(&old_timer);
    let new_timer = timer_key(lease_expires_at, &req.job_id);
    tx.insert(&store.leases, new_timer.clone(), Vec::new());
    indexes.leases.insert(new_timer, &inflight.queue);
    cycle.dirty = true;
    Ok((lease_expires_at, inflight.trace_context))
}

fn apply_sweep(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
    cycle: &mut Cycle,
) -> Result<usize, Status> {
    let now = now_ms();
    let mut processed = 0usize;

    // Each phase gets its own budget so a backlog of one timer kind cannot
    // starve another — most importantly, scheduled promotions must not crowd
    // out lease-expiry redelivery.
    let mut budget = store.params.sweep_limit;
    while budget > 0 {
        let Some((timer_k, _)) = indexes.scheduled.pop_due(now) else {
            break;
        };
        budget -= 1;
        processed += 1;
        let attempt_hint = tx
            .get(&store.scheduled, &timer_k)
            .map_err(stg_err)?
            .and_then(|v| read_u32(&v, 0));
        tx.remove(&store.scheduled, timer_k.clone());
        cycle.dirty = true;

        let Some(job_id) = timer_k.get(8..) else {
            continue;
        };
        let Some(stored) = tx.get(&store.jobs, job_id).map_err(stg_err)? else {
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
        tx.insert(&store.ready, rk.clone(), attempt.to_be_bytes().to_vec());
        indexes.ready.insert(rk, attempt);
        cycle.sweep_promotion(&queue);
        cycle.new_ready.insert(queue);
    }

    let mut budget = store.params.sweep_limit;
    while budget > 0 {
        let Some((timer_k, _)) = indexes.leases.pop_due(now) else {
            break;
        };
        budget -= 1;
        processed += 1;
        tx.remove(&store.leases, timer_k.clone());
        cycle.dirty = true;

        let Some(job_id) = timer_k.get(8..) else {
            continue;
        };
        let Some(stored) = tx.get(&store.inflight, job_id).map_err(stg_err)? else {
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
            let job_id_str = String::from_utf8_lossy(job_id);
            let _span = telemetry::enabled().then(|| {
                let span = tracing::info_span!(
                    "sepp.dead_letter",
                    job_id = %job_id_str,
                    queue = %inflight.queue,
                    attempt = inflight.attempt,
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
            let rk = ready_key(
                &inflight.queue,
                inflight.priority,
                inflight.enqueued_at,
                job_id_str,
            );
            tx.insert(&store.ready, rk.clone(), attempt.to_be_bytes().to_vec());
            indexes.ready.insert(rk, attempt);
            tx.remove(&store.inflight, job_id.to_vec());
            cycle.sweep_lease_redelivery(&inflight.queue);
            cycle.new_ready.insert(inflight.queue);
        }
    }

    let mut budget = store.params.sweep_limit;
    while budget > 0 {
        let Some((timer_k, queue)) = indexes.dedup_timers.pop_due(now) else {
            break;
        };
        budget -= 1;
        processed += 1;
        if let Some(dedup_k) = timer_k.get(8..) {
            tx.remove(&store.dedup, dedup_k.to_vec());
        }
        tx.remove(&store.dedup_timers, timer_k.clone());
        cycle.sweep_dedup_expiration(&queue);
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
    pub fn open(
        config: &Config,
        registry: SharedRegistry,
        metrics: Metrics,
    ) -> Result<Self, fjall::Error> {
        let mut builder = TxDatabase::builder(config.server.db_path.as_str());
        if let Some(bytes) = config.storage.cache_size_bytes {
            builder = builder.cache_size(bytes);
        }
        if let Some(bytes) = config.storage.max_journaling_size_bytes {
            builder = builder.max_journaling_size(bytes);
        }
        if let Some(n) = config.storage.max_cached_files {
            builder = builder.max_cached_files(Some(n));
        }
        if let Some(n) = config.storage.worker_threads {
            builder = builder.worker_threads(n);
        }
        let db = builder.open()?;
        let params = StorageParams {
            persist_mode: match config.storage.persist_mode {
                crate::config::PersistMode::SyncAll => PersistMode::SyncAll,
                crate::config::PersistMode::SyncData => PersistMode::SyncData,
                crate::config::PersistMode::Buffer => PersistMode::Buffer,
            },
            sweep_limit: config.storage.sweep_limit,
        };
        if matches!(
            config.storage.persist_mode,
            crate::config::PersistMode::Buffer
        ) {
            warn!(
                "storage is running in buffer persist mode: writes are not fsynced; \
                 an OS crash or power loss can lose any writes the kernel has not yet flushed"
            );
        }

        // Most reads we make in the hot path will have a match
        let hits = || KeyspaceCreateOptions::default().expect_point_read_hits(true);
        let store = Store {
            jobs: db.keyspace("jobs", hits)?,
            // Payloads ≥1 KiB land in blob files outside the LSM tree, so
            // compaction only rewrites references rather than payload bytes.
            payloads: db.keyspace("payloads", || {
                hits().with_kv_separation(Some(KvSeparationOptions::default()))
            })?,
            inflight: db.keyspace("inflight", hits)?,
            ready: db.keyspace("ready", hits)?,
            dedup: db.keyspace("dedup", KeyspaceCreateOptions::default)?,
            dedup_timers: db.keyspace("dedup_timers", hits)?,
            scheduled: db.keyspace("scheduled", hits)?,
            leases: db.keyspace("leases", hits)?,
            db,
            params,
            registry,
            metrics,
        };
        let indexes = rebuild_indexes(&store)?;

        if config.server.strict_queues {
            warn_on_undeclared_persisted_queues(&store);
        }

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

        info!(
            db_path = %config.server.db_path,
            persist_mode = ?config.storage.persist_mode,
            sweep_interval_ms = config.storage.sweep_interval_ms,
            sweep_limit = config.storage.sweep_limit,
            command_queue_capacity = config.storage.command_queue_capacity,
            "storage opened",
        );

        Ok(Self { tx, notifiers })
    }

    pub fn command_queue_depth(&self) -> usize {
        self.tx.len()
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

    pub async fn enqueue(&self, jobs: Vec<EnqueueRequest>) -> Result<Vec<EnqueueResponse>, Status> {
        self.send(|resp| Command::Enqueue { jobs, resp }).await?
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

    pub async fn ack(&self, job_id: String, attempt: u32) -> Result<Option<TraceContext>, Status> {
        self.send(|resp| Command::Ack {
            job_id,
            attempt,
            resp,
        })
        .await?
    }

    pub async fn nack(&self, req: NackRequest) -> Result<(bool, Option<TraceContext>), Status> {
        self.send(|resp| Command::Nack { req, resp }).await?
    }

    pub async fn extend(&self, req: ExtendRequest) -> Result<(i64, Option<TraceContext>), Status> {
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

    fn sample_inflight(queue: &str, trace_context: Option<TraceContext>) -> Inflight {
        Inflight {
            attempt: 4,
            lease_expires_at: 1_700_000_999_000,
            enqueued_at: 1_700_000_000_000,
            priority: 7,
            max_attempts: 10,
            queue: queue.to_string(),
            trace_context,
        }
    }

    #[test]
    fn inflight_encoding_round_trips() {
        let s = sample_inflight("my-queue", None);
        let d = decode_inflight(&encode_inflight(&s)).expect("decodes");
        assert_eq!(d.attempt, s.attempt);
        assert_eq!(d.lease_expires_at, s.lease_expires_at);
        assert_eq!(d.enqueued_at, s.enqueued_at);
        assert_eq!(d.priority, s.priority);
        assert_eq!(d.max_attempts, s.max_attempts);
        assert_eq!(d.queue, s.queue);
        assert_eq!(d.trace_context, None);
    }

    #[test]
    fn inflight_encoding_round_trips_with_trace_context() {
        let tc = TraceContext {
            traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01".to_string(),
            tracestate: Some("vendor=abc".to_string()),
        };
        let s = sample_inflight("orders", Some(tc.clone()));
        let d = decode_inflight(&encode_inflight(&s)).expect("decodes");
        assert_eq!(d.queue, "orders");
        assert_eq!(d.trace_context, Some(tc));
    }

    #[test]
    fn inflight_encoding_round_trips_empty_queue() {
        let tc = TraceContext {
            traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01".to_string(),
            tracestate: None,
        };
        let s = sample_inflight("", Some(tc.clone()));
        let d = decode_inflight(&encode_inflight(&s)).expect("decodes");
        assert_eq!(d.queue, "");
        assert_eq!(d.trace_context, Some(tc));
    }

    #[test]
    fn decode_inflight_rejects_truncated_input() {
        assert!(decode_inflight(&[]).is_err());
        assert!(decode_inflight(&[0u8; 20]).is_err());
        let bytes = encode_inflight(&sample_inflight("q", None));
        assert!(decode_inflight(&bytes[..10]).is_err());
    }

    #[test]
    fn decode_inflight_rejects_invalid_queue_utf8() {
        let mut bytes = encode_inflight(&sample_inflight("ab", None));
        bytes[30] = 0xff;
        bytes[31] = 0xff;
        assert!(decode_inflight(&bytes).is_err());
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
        idx.insert(timer_key(300, "c"), "q");
        idx.insert(timer_key(100, "a"), "q");
        idx.insert(timer_key(200, "b"), "q");

        assert_eq!(
            idx.pop_due(i64::MAX),
            Some((timer_key(100, "a"), "q".to_string()))
        );
        assert_eq!(
            idx.pop_due(i64::MAX),
            Some((timer_key(200, "b"), "q".to_string()))
        );
        assert_eq!(
            idx.pop_due(i64::MAX),
            Some((timer_key(300, "c"), "q".to_string()))
        );
        assert_eq!(idx.pop_due(i64::MAX), None);
    }

    #[test]
    fn timer_index_pop_due_respects_the_now_boundary() {
        let mut idx = TimerIndex::default();
        idx.insert(timer_key(100, "a"), "q");

        assert_eq!(idx.pop_due(99), None);
        assert_eq!(
            idx.pop_due(100),
            Some((timer_key(100, "a"), "q".to_string()))
        );
    }

    #[test]
    fn timer_index_only_yields_due_entries() {
        let mut idx = TimerIndex::default();
        idx.insert(timer_key(100, "a"), "q");
        idx.insert(timer_key(500, "b"), "q");

        assert_eq!(
            idx.pop_due(200),
            Some((timer_key(100, "a"), "q".to_string()))
        );
        assert_eq!(idx.pop_due(200), None);
        assert_eq!(
            idx.pop_due(500),
            Some((timer_key(500, "b"), "q".to_string()))
        );
    }

    #[test]
    fn timer_index_earliest_reports_the_lowest_deadline() {
        let mut idx = TimerIndex::default();
        assert_eq!(idx.earliest(), None);

        idx.insert(timer_key(300, "c"), "q");
        idx.insert(timer_key(100, "a"), "q");
        idx.insert(timer_key(200, "b"), "q");
        assert_eq!(idx.earliest(), Some(100));

        idx.remove(&timer_key(100, "a"));
        assert_eq!(idx.earliest(), Some(200));
    }

    #[test]
    fn next_deadline_is_the_minimum_across_every_timer_index() {
        let mut indexes = Indexes::default();
        assert_eq!(next_deadline(&indexes), None);

        indexes.scheduled.insert(timer_key(500, "s"), "q");
        indexes.leases.insert(timer_key(200, "l"), "q");
        indexes.dedup_timers.insert(dedup_timer_key(800, b"d"), "q");
        assert_eq!(next_deadline(&indexes), Some(200));

        indexes.leases.remove(&timer_key(200, "l"));
        assert_eq!(next_deadline(&indexes), Some(500));
    }

    #[test]
    fn timer_index_remove_drops_the_entry() {
        let mut idx = TimerIndex::default();
        idx.insert(timer_key(100, "a"), "q");
        idx.remove(&timer_key(100, "a"));
        assert_eq!(idx.pop_due(i64::MAX), None);
    }

    #[test]
    fn timer_index_tracks_per_queue_depths() {
        let mut idx = TimerIndex::default();
        idx.insert(timer_key(100, "a"), "qa");
        idx.insert(timer_key(200, "b"), "qa");
        idx.insert(timer_key(300, "c"), "qb");
        assert_eq!(idx.by_queue.get("qa").copied(), Some(2));
        assert_eq!(idx.by_queue.get("qb").copied(), Some(1));

        idx.remove(&timer_key(100, "a"));
        assert_eq!(idx.by_queue.get("qa").copied(), Some(1));

        idx.pop_due(i64::MAX);
        assert_eq!(idx.by_queue.get("qa").copied(), None);
        assert_eq!(idx.by_queue.get("qb").copied(), Some(1));
    }

    #[test]
    fn ready_index_tracks_per_queue_depths() {
        let mut idx = ReadyIndex::default();
        idx.insert(ready_key("qa", 5, 100, "j1"), 1);
        idx.insert(ready_key("qa", 5, 200, "j2"), 1);
        idx.insert(ready_key("qb", 5, 100, "j3"), 1);
        assert_eq!(idx.by_queue.get("qa").copied(), Some(2));
        assert_eq!(idx.by_queue.get("qb").copied(), Some(1));

        idx.pop_front(&queue_prefix("qa"));
        assert_eq!(idx.by_queue.get("qa").copied(), Some(1));
        idx.pop_front(&queue_prefix("qa"));
        assert_eq!(idx.by_queue.get("qa").copied(), None);
    }
}
