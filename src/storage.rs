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
use crate::keys::{
    DeadLetterKey, DedupKey, DedupTimerKey, DedupValue, Inflight, JobValue, ReadyKey, TimerKey,
    deadline_of, queue_prefix, read_queue,
};
use crate::metrics::{CycleMetrics, Metrics, QueueDepthSnapshot};
use crate::pb::sepp::v1::{
    DeadLetterCause, DeadLetterRecord, EnqueueRequest, EnqueueResponse, ExtendRequest, Job,
    NackRequest, Payload, TraceContext, nack_retry,
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
    dead_letter_retention_ms: u64,
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
    dead_letter: TxKeyspace,
    params: StorageParams,
    registry: SharedRegistry,
    metrics: Metrics,
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

    fn iter_oldest(&self) -> impl Iterator<Item = (&[u8], &str)> {
        self.keys.iter().map(|(k, v)| (k.as_slice(), v.as_str()))
    }
}

#[derive(Default)]
struct Indexes {
    ready: ReadyIndex,
    scheduled: TimerIndex,
    leases: TimerIndex,
    dedup_timers: TimerIndex,
    dead_letter: TimerIndex,
}

impl Indexes {
    fn live_depth(&self, queue: &str) -> u64 {
        self.ready.by_queue.get(queue).copied().unwrap_or(0)
            + self.scheduled.by_queue.get(queue).copied().unwrap_or(0)
            + self.leases.by_queue.get(queue).copied().unwrap_or(0)
    }

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
        let attempt = value
            .first_chunk::<4>()
            .map(|b| u32::from_be_bytes(*b))
            .unwrap_or(1);
        indexes.ready.insert(key.to_vec(), attempt);
    }

    for guard in snap.iter(&store.scheduled) {
        let (key, _) = guard.into_inner()?;
        let queue = TimerKey::job_id(&key)
            .and_then(|job_id| snap.get(&store.jobs, job_id).ok().flatten())
            .and_then(|stored| read_queue(&stored).map(str::to_owned))
            .unwrap_or_default();
        indexes.scheduled.insert(key.to_vec(), &queue);
    }

    for guard in snap.iter(&store.leases) {
        let (key, _) = guard.into_inner()?;
        let queue = TimerKey::job_id(&key)
            .and_then(|job_id| snap.get(&store.inflight, job_id).ok().flatten())
            .and_then(|stored| Inflight::decode(&stored).ok().map(|i| i.queue))
            .unwrap_or_default();
        indexes.leases.insert(key.to_vec(), &queue);
    }

    for guard in snap.iter(&store.dedup_timers) {
        let (key, _) = guard.into_inner()?;
        let queue = DedupTimerKey::queue(&key).unwrap_or("").to_string();
        indexes.dedup_timers.insert(key.to_vec(), &queue);
    }

    for guard in snap.iter(&store.dead_letter) {
        let key = guard.key()?;
        let queue = DeadLetterKey::queue(&key).unwrap_or("").to_string();
        indexes.dead_letter.insert(key.to_vec(), &queue);
    }

    Ok(indexes)
}

fn resync(store: &Store, indexes: &mut Indexes) {
    match rebuild_indexes(store) {
        Ok(fresh) => *indexes = fresh,
        Err(e) => error!(error = %e, "could not re-sync the in-memory indexes"),
    }
}

pub struct AckOutcome {
    pub queue: String,
    pub trace_context: Option<TraceContext>,
}

pub struct NackOutcome {
    pub queue: String,
    pub dead_lettered: bool,
    pub retry_delay_ms: u64,
    pub trace_context: Option<TraceContext>,
}

pub struct ExtendOutcome {
    pub queue: String,
    pub lease_expires_at: i64,
    pub trace_context: Option<TraceContext>,
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
        resp: oneshot::Sender<Result<AckOutcome, Status>>,
    },
    Nack {
        req: NackRequest,
        resp: oneshot::Sender<Result<NackOutcome, Status>>,
    },
    Extend {
        req: ExtendRequest,
        resp: oneshot::Sender<Result<ExtendOutcome, Status>>,
    },
    DrainDeadLetters {
        queue: Option<String>,
        max: usize,
        resp: oneshot::Sender<Result<Vec<DeadLetterRecord>, Status>>,
    },
}

enum Responder {
    Enqueue(
        oneshot::Sender<Result<Vec<EnqueueResponse>, Status>>,
        Vec<EnqueueResponse>,
    ),
    Reserve(oneshot::Sender<Result<Vec<Job>, Status>>, Vec<Job>),
    Ack(oneshot::Sender<Result<AckOutcome, Status>>, AckOutcome),
    Nack(oneshot::Sender<Result<NackOutcome, Status>>, NackOutcome),
    Extend(
        oneshot::Sender<Result<ExtendOutcome, Status>>,
        ExtendOutcome,
    ),
    Drain(
        oneshot::Sender<Result<Vec<DeadLetterRecord>, Status>>,
        Vec<DeadLetterRecord>,
    ),
}

impl Responder {
    fn respond(self, outcome: &Result<(), Status>) {
        match self {
            Responder::Enqueue(resp, payload) => {
                let _ = resp.send(outcome.clone().map(|()| payload));
            }
            Responder::Reserve(resp, payload) => {
                let _ = resp.send(outcome.clone().map(|()| payload));
            }
            Responder::Ack(resp, payload) => {
                let _ = resp.send(outcome.clone().map(|()| payload));
            }
            Responder::Nack(resp, payload) => {
                let _ = resp.send(outcome.clone().map(|()| payload));
            }
            Responder::Extend(resp, payload) => {
                let _ = resp.send(outcome.clone().map(|()| payload));
            }
            Responder::Drain(resp, payload) => {
                let _ = resp.send(outcome.clone().map(|()| payload));
            }
        }
    }
}

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

    fn dead_letter_expired(&mut self, n: u64) {
        if let Some(m) = self.metrics.as_mut() {
            m.dead_letters_expired += n;
        }
    }

    fn dead_letter_drained(&mut self, n: u64) {
        if let Some(m) = self.metrics.as_mut() {
            m.dead_letters_drained += n;
        }
    }
}

fn next_deadline(indexes: &Indexes, retention_ms: u64) -> Option<i64> {
    let dead_letter = (retention_ms > 0)
        .then(|| indexes.dead_letter.earliest())
        .flatten()
        .map(|f| f.saturating_add(i64::try_from(retention_ms).unwrap_or(i64::MAX)));
    [
        indexes.scheduled.earliest(),
        indexes.leases.earliest(),
        indexes.dedup_timers.earliest(),
        dead_letter,
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
    let retention_ms = store.params.dead_letter_retention_ms;
    loop {
        let sweep_due = next_deadline(&indexes, retention_ms).is_some_and(|d| d <= now_ms());
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
            let wait = match next_deadline(&indexes, retention_ms) {
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
    let mut responders: Vec<Responder> = Vec::with_capacity(rpcs.len());

    for cmd in rpcs {
        match cmd {
            Command::Enqueue { jobs, resp } => {
                match apply_enqueue(store, indexes, &mut tx, &mut cycle, jobs) {
                    Ok(enqueued) => responders.push(Responder::Enqueue(resp, enqueued)),
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
                Ok(jobs) => responders.push(Responder::Reserve(resp, jobs)),
                Err(e) => {
                    let _ = resp.send(Err(e));
                }
            },
            Command::Ack {
                job_id,
                attempt,
                resp,
            } => match apply_ack(store, indexes, &mut tx, &mut cycle, &job_id, attempt) {
                Ok(outcome) => responders.push(Responder::Ack(resp, outcome)),
                Err(e) => {
                    let _ = resp.send(Err(e));
                }
            },
            Command::Nack { req, resp } => {
                match apply_nack(store, indexes, &mut tx, &mut cycle, req) {
                    Ok(outcome) => responders.push(Responder::Nack(resp, outcome)),
                    Err(e) => {
                        let _ = resp.send(Err(e));
                    }
                }
            }
            Command::Extend { req, resp } => {
                match apply_extend(store, indexes, &mut tx, &mut cycle, req) {
                    Ok(outcome) => responders.push(Responder::Extend(resp, outcome)),
                    Err(e) => {
                        let _ = resp.send(Err(e));
                    }
                }
            }
            Command::DrainDeadLetters { queue, max, resp } => {
                match apply_drain(store, indexes, &mut tx, &mut cycle, queue, max) {
                    Ok(records) => responders.push(Responder::Drain(resp, records)),
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
        responder.respond(&outcome);
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

    {
        let mut wanted: HashMap<&str, u64> = HashMap::new();
        for req in &jobs {
            *wanted.entry(req.queue.as_str()).or_default() += 1;
        }

        for (queue, count) in wanted {
            if let Some(cap) = registry.effective(queue).max_queue_depth
                && indexes.live_depth(queue) + count > cap
            {
                return Err(Status::resource_exhausted(format!(
                    "queue {queue:?} is at capacity (max_queue_depth={cap})"
                )));
            }
        }
    }

    let mut results = Vec::with_capacity(jobs.len());

    for req in jobs {
        let limits = registry.effective(&req.queue);
        let mut stale_dedup_timer: Option<Vec<u8>> = None;

        if let Some(key) = &req.idempotency_key {
            let dkey = DedupKey {
                queue: &req.queue,
                idempotency_key: key,
            }
            .encode();
            if let Some(existing) = tx.get(&store.dedup, &dkey).map_err(stg_err)? {
                match DedupValue::decode(&existing) {
                    Some(dv) if now - dv.enqueued_at < limits.dedup_window_ms => {
                        cycle.deduplicated(&req.queue);
                        results.push(EnqueueResponse {
                            job_id: dv.job_id.to_owned(),
                            deduplicated: true,
                        });
                        continue;
                    }
                    Some(dv) => {
                        stale_dedup_timer = Some(
                            DedupTimerKey {
                                deadline: dv.enqueued_at + limits.dedup_window_ms,
                                dedup_key: &dkey,
                            }
                            .encode(),
                        );
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

        match job.scheduled_at {
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
                    enqueued_at: job.enqueued_at,
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

            let dtk = DedupTimerKey {
                deadline: now + limits.dedup_window_ms,
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
                    job_id: &id,
                }
                .encode(),
            );
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
        while jobs.len() < max_jobs {
            let Some((ready_k, attempt)) = indexes.ready.pop_front(&prefix) else {
                break;
            };

            let job_id: String = match ReadyKey::decode(&ready_k) {
                Some(rk) => rk.job_id.to_owned(),
                None => {
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

            let (job_queue, mut job) = match JobValue::decode(&stored) {
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
            job.queue = job_queue.clone();

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

fn apply_ack(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
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

fn read_dead_letter_job(
    store: &Store,
    tx: &mut WriteTransaction<'_>,
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

struct DeadLetterMeta {
    cause: DeadLetterCause,
    failed_at: i64,
    attempt: u32,
    last_reason: Option<String>,
}

// Depending on if the retention is set or not
fn maybe_store_dead_letter(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
    job_id: &[u8],
    meta: DeadLetterMeta,
) -> Result<(), Status> {
    if store.params.dead_letter_retention_ms == 0 {
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
        failed_at: meta.failed_at,
        final_attempt: meta.attempt,
        last_reason: meta.last_reason,
    };

    tx.insert(&store.dead_letter, key, record.encode_to_vec());
    Ok(())
}

fn apply_drain(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
    cycle: &mut Cycle,
    queue: Option<String>,
    max: usize,
) -> Result<Vec<DeadLetterRecord>, Status> {
    let mut chosen: Vec<Vec<u8>> = Vec::new();
    for (examined, (key, q)) in indexes.dead_letter.iter_oldest().enumerate() {
        if chosen.len() >= max || examined >= store.params.sweep_limit {
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

fn apply_nack(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
    cycle: &mut Cycle,
    req: NackRequest,
) -> Result<NackOutcome, Status> {
    let now = now_ms();
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

fn apply_extend(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
    cycle: &mut Cycle,
    req: ExtendRequest,
) -> Result<ExtendOutcome, Status> {
    let stored = tx
        .get(&store.inflight, req.job_id.as_bytes())
        .map_err(stg_err)?
        .ok_or_else(|| Status::not_found("job not found"))?;

    let mut inflight = Inflight::decode(&stored)?;
    if inflight.attempt != req.attempt {
        return Err(Status::failed_precondition("attempt mismatch"));
    }

    let max_lease = store
        .registry
        .load()
        .effective(&inflight.queue)
        .max_lease_duration_ms;
    let old_timer = TimerKey {
        deadline: inflight.lease_expires_at,
        job_id: &req.job_id,
    }
    .encode();
    let lease_ms = req.lease_duration_ms.min(max_lease);
    let lease_expires_at = now_ms().saturating_add(i64::try_from(lease_ms).unwrap_or(i64::MAX));
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

        let attempt = attempt_hint.unwrap_or(job.attempt);
        let _span = telemetry::enabled().then(|| {
            let span = tracing::info_span!(
                "sepp.promote",
                job_id = %job.id,
                queue = %queue,
                job_type = %job.job_type,
                attempt,
                priority = job.priority,
                scheduled_at = job.scheduled_at,
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
            enqueued_at: job.enqueued_at,
            job_id: &job.id,
        }
        .encode();

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

    let mut budget = store.params.sweep_limit;
    while budget > 0 {
        let Some((timer_k, queue)) = indexes.dedup_timers.pop_due(now) else {
            break;
        };

        budget -= 1;
        processed += 1;
        if let Some(dedup_k) = DedupTimerKey::dedup_key(&timer_k) {
            tx.remove(&store.dedup, dedup_k.to_vec());
        }

        tx.remove(&store.dedup_timers, timer_k.clone());
        cycle.sweep_dedup_expiration(&queue);
        cycle.dirty = true;
    }

    if store.params.dead_letter_retention_ms > 0 {
        let cutoff = now.saturating_sub(
            i64::try_from(store.params.dead_letter_retention_ms).unwrap_or(i64::MAX),
        );
        let mut budget = store.params.sweep_limit;
        let mut expired = 0u64;

        while budget > 0 {
            let Some((key, _queue)) = indexes.dead_letter.pop_due(cutoff) else {
                break;
            };
            budget -= 1;
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
            dead_letter_retention_ms: config.storage.dead_letter_retention_ms,
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
            dead_letter: db.keyspace("dead_letter", || {
                hits().with_kv_separation(Some(KvSeparationOptions::default()))
            })?,
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
            dead_letter_retention_ms = config.storage.dead_letter_retention_ms,
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

    pub async fn ack(&self, job_id: String, attempt: u32) -> Result<AckOutcome, Status> {
        self.send(|resp| Command::Ack {
            job_id,
            attempt,
            resp,
        })
        .await?
    }

    pub async fn nack(&self, req: NackRequest) -> Result<NackOutcome, Status> {
        self.send(|resp| Command::Nack { req, resp }).await?
    }

    pub async fn extend(&self, req: ExtendRequest) -> Result<ExtendOutcome, Status> {
        self.send(|resp| Command::Extend { req, resp }).await?
    }

    pub async fn drain_dead_letters(
        &self,
        queue: Option<String>,
        max: usize,
    ) -> Result<Vec<DeadLetterRecord>, Status> {
        self.send(|resp| Command::DrainDeadLetters { queue, max, resp })
            .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // So that each test doesnt have to spell out the entire struct
    fn ready_key(queue: &str, priority: u32, enqueued_at: i64, job_id: &str) -> Vec<u8> {
        ReadyKey {
            queue,
            priority,
            enqueued_at,
            job_id,
        }
        .encode()
    }

    fn job_id_of<'a>(_queue: &str, ready_k: &'a [u8]) -> &'a str {
        ReadyKey::decode(ready_k).unwrap().job_id
    }

    fn timer_key(deadline: i64, job_id: &str) -> Vec<u8> {
        TimerKey { deadline, job_id }.encode()
    }

    fn dedup_timer_key(deadline: i64, dedup_key: &[u8]) -> Vec<u8> {
        DedupTimerKey { deadline, dedup_key }.encode()
    }

    fn dead_letter_key(failed_at: i64, queue: &str, job_id: &[u8]) -> Vec<u8> {
        DeadLetterKey {
            failed_at,
            queue,
            job_id,
        }
        .encode()
    }

    #[test]
    fn timer_index_iter_oldest_walks_in_order() {
        let mut idx = TimerIndex::default();
        idx.insert(dead_letter_key(300, "qb", b"c"), "qb");
        idx.insert(dead_letter_key(100, "qa", b"a"), "qa");
        idx.insert(dead_letter_key(200, "qa", b"b"), "qa");
        let order: Vec<(i64, &str)> = idx
            .iter_oldest()
            .map(|(k, q)| (deadline_of(k), q))
            .collect();
        assert_eq!(order, vec![(100, "qa"), (200, "qa"), (300, "qb")]);
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
    fn next_deadline_is_the_minimum_across_every_index() {
        let mut indexes = Indexes::default();
        assert_eq!(next_deadline(&indexes, 0), None);

        indexes.scheduled.insert(timer_key(500, "s"), "q");
        indexes.leases.insert(timer_key(200, "l"), "q");
        indexes.dedup_timers.insert(dedup_timer_key(800, b"d"), "q");
        assert_eq!(next_deadline(&indexes, 0), Some(200));

        indexes.leases.remove(&timer_key(200, "l"));
        assert_eq!(next_deadline(&indexes, 0), Some(500));

        // The oldest dead-letter contributes failed_at + retention to the min.
        indexes
            .dead_letter
            .insert(dead_letter_key(100, "q", b"d"), "q");
        assert_eq!(
            next_deadline(&indexes, 50),
            Some(150),
            "oldest failed_at (100) + retention (50) becomes the minimum"
        );
        // With retention disabled, the dead-letter term drops out entirely.
        assert_eq!(next_deadline(&indexes, 0), Some(500));
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
