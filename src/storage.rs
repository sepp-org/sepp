use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arc_swap::ArcSwap;
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
use crate::pb::{duration_to_millis, millis_to_timestamp, timestamp_to_millis};
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
    admin_enabled: bool,
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

    fn attempt(&self, ready_key: &[u8]) -> Option<u32> {
        self.keys.get(ready_key).copied()
    }

    fn remove(&mut self, ready_key: &[u8]) -> Option<u32> {
        let attempt = self.keys.remove(ready_key)?;
        if let Some(queue) = read_queue(ready_key) {
            drop_queue(&mut self.by_queue, queue);
        }

        Some(attempt)
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
            dead_letter: self.dead_letter.by_queue.clone(),
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

#[derive(Clone, Copy)]
pub enum PeekState {
    Ready,
    Scheduled,
    Inflight,
    DeadLetter,
}

pub struct PeekPage {
    pub keys: Vec<Vec<u8>>,
    pub next_cursor: Option<Vec<u8>>,
    pub truncated: bool,
}

#[derive(Debug)]
pub struct RequeueOutcome {
    pub requeued: u64,
    pub missing: u64,
}

pub struct DeadLetterJobsOutcome {
    pub dead_lettered: u64,
    pub missing: u64,
}

#[derive(Debug)]
pub struct DeleteOutcome {
    pub deleted: u64,
    pub missing: u64,
}

#[derive(Debug)]
pub struct PurgeOutcome {
    pub purged: u64,
    pub remaining: bool,
}

#[derive(Default, Clone)]
pub struct QueueTotals {
    pub enqueued: u64,
    pub reserved: u64,
    pub acked: u64,
    pub nacked: u64,
    pub dead_lettered: u64,
}

#[derive(Default)]
pub struct AdminSnapshot {
    pub ts_ms: i64,
    pub depths: QueueDepthSnapshot,
    pub totals: HashMap<String, QueueTotals>,
    // Filled in by the reader at frame-build time; the committer leaves it 0.
    pub command_queue_len: usize,
}

const PEEK_LIMIT_MAX: usize = 100;
const PEEK_EXAMINE_CAP: usize = 10_000;
const PURGE_CHUNK_MAX: usize = 1000;
const ADMIN_PUBLISH_INTERVAL: Duration = Duration::from_millis(250);
const ADMIN_IDLE_EVICT_MS: i64 = 15 * 60 * 1000;

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
    PeekKeys {
        state: PeekState,
        queue: String,
        cursor: Option<Vec<u8>>,
        limit: usize,
        resp: oneshot::Sender<Result<PeekPage, Status>>,
    },
    RequeueDeadLetters {
        queue: String,
        keys: Vec<Vec<u8>>,
        resp: oneshot::Sender<Result<RequeueOutcome, Status>>,
    },
    DeadLetterJobs {
        queue: String,
        state: PeekState,
        keys: Vec<Vec<u8>>,
        reason: Option<String>,
        resp: oneshot::Sender<Result<DeadLetterJobsOutcome, Status>>,
    },
    DeleteDeadLetters {
        queue: String,
        keys: Vec<Vec<u8>>,
        resp: oneshot::Sender<Result<DeleteOutcome, Status>>,
    },
    PurgeQueueChunk {
        queue: String,
        max: usize,
        resp: oneshot::Sender<Result<PurgeOutcome, Status>>,
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
    Requeue(
        oneshot::Sender<Result<RequeueOutcome, Status>>,
        RequeueOutcome,
    ),
    DeadLetterJobs(
        oneshot::Sender<Result<DeadLetterJobsOutcome, Status>>,
        DeadLetterJobsOutcome,
    ),
    DeleteDeadLetters(
        oneshot::Sender<Result<DeleteOutcome, Status>>,
        DeleteOutcome,
    ),
    Purge(oneshot::Sender<Result<PurgeOutcome, Status>>, PurgeOutcome),
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
            Responder::Requeue(resp, payload) => {
                let _ = resp.send(outcome.clone().map(|()| payload));
            }
            Responder::DeadLetterJobs(resp, payload) => {
                let _ = resp.send(outcome.clone().map(|()| payload));
            }
            Responder::DeleteDeadLetters(resp, payload) => {
                let _ = resp.send(outcome.clone().map(|()| payload));
            }
            Responder::Purge(resp, payload) => {
                let _ = resp.send(outcome.clone().map(|()| payload));
            }
        }
    }
}

struct Cycle {
    dirty: bool,
    new_ready: HashSet<String>,
    // `None` when neither metrics nor admin stats want the deltas — every
    // recorder method becomes a no-op and we skip allocating into nine
    // HashMaps that would never be read.
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

    fn queue_purged(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            m.purged_queues.push(queue.to_string());
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

fn fold_admin_totals(
    totals: &mut HashMap<String, QueueTotals>,
    last_active: &mut HashMap<String, i64>,
    m: &CycleMetrics,
) {
    let now = now_ms();
    let mut fold = |by_queue: &HashMap<String, u64>, pick: fn(&mut QueueTotals) -> &mut u64| {
        for (queue, n) in by_queue {
            *pick(totals.entry(queue.clone()).or_default()) += n;
            last_active.insert(queue.clone(), now);
        }
    };

    fold(&m.enqueued_by_queue, |t| &mut t.enqueued);
    fold(&m.reserved_by_queue, |t| &mut t.reserved);
    fold(&m.acked_by_queue, |t| &mut t.acked);
    fold(&m.nacked_by_queue, |t| &mut t.nacked);

    for ((queue, _cause), n) in &m.dead_lettered_by_queue_cause {
        totals.entry(queue.clone()).or_default().dead_lettered += n;
        last_active.insert(queue.clone(), now);
    }
}

fn evict_idle_admin_totals(
    indexes: &Indexes,
    totals: &mut HashMap<String, QueueTotals>,
    last_active: &mut HashMap<String, i64>,
    now: i64,
) {
    totals.retain(|queue, _| {
        last_active
            .get(queue)
            .is_some_and(|&at| now - at < ADMIN_IDLE_EVICT_MS)
            || indexes.ready.by_queue.contains_key(queue)
            || indexes.scheduled.by_queue.contains_key(queue)
            || indexes.leases.by_queue.contains_key(queue)
            || indexes.dead_letter.by_queue.contains_key(queue)
    });
    last_active.retain(|queue, _| totals.contains_key(queue));
}

fn run_committer(
    store: Store,
    mut indexes: Indexes,
    rx: flume::Receiver<Command>,
    notifiers: QueueNotifiers,
    max_sweep_interval: Duration,
    admin_stats: Arc<ArcSwap<AdminSnapshot>>,
) {
    let retention_ms = store.params.dead_letter_retention_ms;
    let mut totals: HashMap<String, QueueTotals> = HashMap::new();
    let mut last_active: HashMap<String, i64> = HashMap::new();
    let mut last_published: Option<std::time::Instant> = None;
    loop {
        let sweep_due = next_deadline(&indexes, retention_ms).is_some_and(|d| d <= now_ms());
        if sweep_due {
            let cycle_metrics = run_sweep_cycle(&store, &mut indexes, &notifiers);
            if store.params.admin_enabled {
                if let Some(m) = cycle_metrics {
                    fold_admin_totals(&mut totals, &mut last_active, &m);
                }
                evict_idle_admin_totals(&indexes, &mut totals, &mut last_active, now_ms());
            }
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

            let cycle_metrics = run_rpc_cycle(&store, &mut indexes, &notifiers, rpcs);
            if store.params.admin_enabled
                && let Some(m) = cycle_metrics
            {
                // Purged-queue totals die with the queue; removing before the
                // fold keeps counts for jobs enqueued after the purge in the
                // same batch.
                for queue in &m.purged_queues {
                    totals.remove(queue);
                    last_active.remove(queue);
                }
                fold_admin_totals(&mut totals, &mut last_active, &m);
            }
        }

        if store.metrics.is_enabled() {
            store.metrics.set_queue_depths(indexes.snapshot());
        }

        // Runs on idle timeouts too, so a quiet server still refreshes ts_ms.
        if store.params.admin_enabled
            && last_published.is_none_or(|at| at.elapsed() >= ADMIN_PUBLISH_INTERVAL)
        {
            admin_stats.store(Arc::new(AdminSnapshot {
                ts_ms: now_ms(),
                depths: indexes.snapshot(),
                totals: totals.clone(),
                command_queue_len: 0,
            }));
            last_published = Some(std::time::Instant::now());
        }
    }

    info!("committer thread stopped; storage is no longer accepting commands");
}

fn run_rpc_cycle(
    store: &Store,
    indexes: &mut Indexes,
    notifiers: &QueueNotifiers,
    rpcs: Vec<Command>,
) -> Option<CycleMetrics> {
    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(store.metrics.is_enabled() || store.params.admin_enabled);
    let mut responders: Vec<Responder> = Vec::with_capacity(rpcs.len());

    let mut rpcs = rpcs.into_iter();
    let fatal = rpcs.by_ref().find_map(|cmd| {
        apply_command(store, indexes, &mut tx, &mut cycle, cmd, &mut responders).err()
    });

    // A storage-level failure can leave the shared transaction holding partial
    // writes of a command whose caller was already told it failed; committing
    // those would persist effects of a failed RPC (and break EnqueueAtomic's
    // all-or-nothing contract). Drop the transaction and fail the whole cycle.
    if let Some(status) = fatal {
        drop(tx);
        resync(store, indexes);
        for responder in responders {
            responder.respond(&Err(status.clone()));
        }
        for cmd in rpcs {
            fail_command(cmd, &status);
        }
        return None;
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
    if outcome.is_err() {
        return None;
    }

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

    cycle.metrics
}

// Applies one command to the cycle's shared transaction, answering business
// rejections immediately. Returns Err only for storage-level failures, which
// poison the whole cycle (see run_rpc_cycle).
fn apply_command(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
    cycle: &mut Cycle,
    cmd: Command,
    responders: &mut Vec<Responder>,
) -> Result<(), Status> {
    match cmd {
        Command::Enqueue { jobs, resp } => match apply_enqueue(store, indexes, tx, cycle, jobs) {
            Ok(enqueued) => responders.push(Responder::Enqueue(resp, enqueued)),
            Err(e) => return reject(resp, e),
        },
        Command::Reserve {
            queues,
            lease_ms,
            max_jobs,
            resp,
        } => match apply_reserve(store, indexes, tx, cycle, &queues, lease_ms, max_jobs) {
            Ok(jobs) => responders.push(Responder::Reserve(resp, jobs)),
            Err(e) => return reject(resp, e),
        },
        Command::Ack {
            job_id,
            attempt,
            resp,
        } => match apply_ack(store, indexes, tx, cycle, &job_id, attempt) {
            Ok(outcome) => responders.push(Responder::Ack(resp, outcome)),
            Err(e) => return reject(resp, e),
        },
        Command::Nack { req, resp } => match apply_nack(store, indexes, tx, cycle, req) {
            Ok(outcome) => responders.push(Responder::Nack(resp, outcome)),
            Err(e) => return reject(resp, e),
        },
        Command::Extend { req, resp } => match apply_extend(store, indexes, tx, cycle, req) {
            Ok(outcome) => responders.push(Responder::Extend(resp, outcome)),
            Err(e) => return reject(resp, e),
        },
        Command::DrainDeadLetters { queue, max, resp } => {
            match apply_drain(store, indexes, tx, cycle, queue, max) {
                Ok(records) => responders.push(Responder::Drain(resp, records)),
                Err(e) => return reject(resp, e),
            }
        }
        // Read-only against the in-memory indexes: answered inline so it
        // neither waits on the cycle's commit nor marks it dirty.
        Command::PeekKeys {
            state,
            queue,
            cursor,
            limit,
            resp,
        } => {
            let _ = resp.send(Ok(peek_keys(indexes, state, &queue, cursor, limit)));
        }
        Command::RequeueDeadLetters { queue, keys, resp } => {
            match apply_requeue_dead_letters(store, indexes, tx, cycle, &queue, keys) {
                Ok(outcome) => responders.push(Responder::Requeue(resp, outcome)),
                Err(e) => return reject(resp, e),
            }
        }
        Command::DeadLetterJobs {
            queue,
            state,
            keys,
            reason,
            resp,
        } => match apply_dead_letter_jobs(store, indexes, tx, cycle, &queue, state, keys, reason) {
            Ok(outcome) => responders.push(Responder::DeadLetterJobs(resp, outcome)),
            Err(e) => return reject(resp, e),
        },
        Command::DeleteDeadLetters { queue, keys, resp } => {
            let outcome = apply_delete_dead_letters(store, indexes, tx, cycle, &queue, keys);
            responders.push(Responder::DeleteDeadLetters(resp, outcome));
        }
        Command::PurgeQueueChunk { queue, max, resp } => {
            match apply_purge_queue_chunk(store, indexes, tx, cycle, &queue, max) {
                Ok(outcome) => responders.push(Responder::Purge(resp, outcome)),
                Err(e) => return reject(resp, e),
            }
        }
    }

    Ok(())
}

// Storage failures are always Status::internal; business rejections (NotFound,
// FailedPrecondition, ResourceExhausted) never are and never mutate the
// transaction before returning.
fn reject<T>(resp: oneshot::Sender<Result<T, Status>>, e: Status) -> Result<(), Status> {
    let fatal = (e.code() == tonic::Code::Internal).then(|| e.clone());
    let _ = resp.send(Err(e));
    match fatal {
        Some(status) => Err(status),
        None => Ok(()),
    }
}

fn fail_command(cmd: Command, status: &Status) {
    match cmd {
        Command::Enqueue { resp, .. } => {
            let _ = resp.send(Err(status.clone()));
        }
        Command::Reserve { resp, .. } => {
            let _ = resp.send(Err(status.clone()));
        }
        Command::Ack { resp, .. } => {
            let _ = resp.send(Err(status.clone()));
        }
        Command::Nack { resp, .. } => {
            let _ = resp.send(Err(status.clone()));
        }
        Command::Extend { resp, .. } => {
            let _ = resp.send(Err(status.clone()));
        }
        Command::DrainDeadLetters { resp, .. } => {
            let _ = resp.send(Err(status.clone()));
        }
        Command::PeekKeys { resp, .. } => {
            let _ = resp.send(Err(status.clone()));
        }
        Command::RequeueDeadLetters { resp, .. } => {
            let _ = resp.send(Err(status.clone()));
        }
        Command::DeadLetterJobs { resp, .. } => {
            let _ = resp.send(Err(status.clone()));
        }
        Command::DeleteDeadLetters { resp, .. } => {
            let _ = resp.send(Err(status.clone()));
        }
        Command::PurgeQueueChunk { resp, .. } => {
            let _ = resp.send(Err(status.clone()));
        }
    }
}

fn run_sweep_cycle(
    store: &Store,
    indexes: &mut Indexes,
    notifiers: &QueueNotifiers,
) -> Option<CycleMetrics> {
    let started = std::time::Instant::now();
    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(store.metrics.is_enabled() || store.params.admin_enabled);

    let processed = match apply_sweep(store, indexes, &mut tx, &mut cycle) {
        Ok(processed) => processed,
        Err(e) => {
            warn!(error = %e, "timer sweep aborted");
            resync(store, indexes);
            return None;
        }
    };

    let outcome = if cycle.dirty {
        commit_and_persist(store, tx)
    } else {
        Ok(())
    };

    if outcome.is_err() {
        resync(store, indexes);
        return None;
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

    cycle.metrics
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

        let scheduled_at_ms = req.scheduled_at.as_ref().map(timestamp_to_millis);
        let job = Job {
            id: id.clone(),
            job_type: req.job_type,
            payload: None,
            priority: req.priority.unwrap_or(limits.default_priority),
            trace_context: req.trace_context,
            enqueued_at: Some(millis_to_timestamp(now)),
            attempt: 1,
            max_attempts: req
                .max_attempts
                .unwrap_or(limits.default_max_attempts)
                .min(limits.max_attempts_ceiling),
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
        failed_at: Some(millis_to_timestamp(meta.failed_at)),
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

fn peek_keys(
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

fn apply_requeue_dead_letters(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
    cycle: &mut Cycle,
    queue: &str,
    keys: Vec<Vec<u8>>,
) -> Result<RequeueOutcome, Status> {
    let now = now_ms();
    let mut requeued = 0u64;
    let mut missing = 0u64;

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
    }

    Ok(RequeueOutcome { requeued, missing })
}

// The reverse of apply_requeue_dead_letters: moves live ready/scheduled jobs
// into the dead-letter queue (or drops them when retention is disabled),
// exactly as a nack with the dead_letter strategy would.
#[allow(clippy::too_many_arguments)]
fn apply_dead_letter_jobs(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
    cycle: &mut Cycle,
    queue: &str,
    state: PeekState,
    keys: Vec<Vec<u8>>,
    reason: Option<String>,
) -> Result<DeadLetterJobsOutcome, Status> {
    if !matches!(state, PeekState::Ready | PeekState::Scheduled) {
        return Err(Status::invalid_argument(
            "only ready or scheduled jobs can be dead-lettered",
        ));
    }

    let now = now_ms();
    let mut dead_lettered = 0u64;
    let mut missing = 0u64;

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
        tx.remove(&store.payloads, job_id);

        cycle.dead_lettered(queue, "admin");
        cycle.dirty = true;
        dead_lettered += 1;
    }

    Ok(DeadLetterJobsOutcome {
        dead_lettered,
        missing,
    })
}

fn apply_delete_dead_letters(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
    cycle: &mut Cycle,
    queue: &str,
    keys: Vec<Vec<u8>>,
) -> DeleteOutcome {
    let mut deleted = 0u64;
    let mut missing = 0u64;

    for key in keys {
        match indexes.dead_letter.keys.get(&key) {
            Some(owner) if owner == queue => {
                indexes.dead_letter.remove(&key);
                tx.remove(&store.dead_letter, key);
                cycle.dirty = true;
                deleted += 1;
            }
            _ => missing += 1,
        }
    }

    DeleteOutcome { deleted, missing }
}

fn apply_purge_queue_chunk(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
    cycle: &mut Cycle,
    queue: &str,
    max: usize,
) -> Result<PurgeOutcome, Status> {
    if indexes.leases.by_queue.get(queue).copied().unwrap_or(0) > 0 {
        return Err(Status::failed_precondition("queue has in-flight jobs"));
    }

    let max = max.clamp(1, PURGE_CHUNK_MAX);
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
        Some(nack_retry::Strategy::Delay(delay)) => {
            let max = store
                .registry
                .load()
                .effective(&inflight.queue)
                .max_schedule_horizon_ms;
            duration_to_millis(delay).min(max)
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
    let lease_ms = req
        .lease_duration
        .as_ref()
        .map(duration_to_millis)
        .unwrap_or(0)
        .min(max_lease);
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AdminJobState {
    Ready,
    Scheduled,
    Inflight,
}

pub struct AdminJob {
    pub key: Vec<u8>,
    pub state: AdminJobState,
    pub job: Job,
}

pub struct AdminDeadLetter {
    pub key: Vec<u8>,
    pub record: DeadLetterRecord,
}

// Point-get-only view of the database for admin reads off the committer
// thread. Methods are sync (callers wrap them in spawn_blocking) and peeked
// keys can vanish between peek and resolve, so misses are silently skipped.
#[derive(Clone)]
pub struct ReadHandle {
    db: TxDatabase,
    jobs: TxKeyspace,
    payloads: TxKeyspace,
    inflight: TxKeyspace,
    ready: TxKeyspace,
    scheduled: TxKeyspace,
    dead_letter: TxKeyspace,
}

impl ReadHandle {
    fn load_job(&self, snap: &impl Readable, job_id: &str) -> Option<Job> {
        let stored = snap.get(&self.jobs, job_id.as_bytes()).ok().flatten()?;
        let (queue, mut job) = JobValue::decode(&stored).ok()?;
        job.queue = queue;
        if let Some(bytes) = snap.get(&self.payloads, job_id.as_bytes()).ok().flatten() {
            job.payload = Payload::decode(&*bytes).ok();
        }

        Some(job)
    }

    pub fn resolve_ready(&self, keys: &[Vec<u8>]) -> Vec<AdminJob> {
        let snap = self.db.read_tx();
        keys.iter()
            .filter_map(|key| {
                let job_id = ReadyKey::decode(key)?.job_id.to_owned();
                let attempt = snap
                    .get(&self.ready, key)
                    .ok()
                    .flatten()
                    .and_then(|v| v.first_chunk::<4>().map(|b| u32::from_be_bytes(*b)))?;
                let mut job = self.load_job(&snap, &job_id)?;
                job.attempt = attempt;

                Some(AdminJob {
                    key: key.clone(),
                    state: AdminJobState::Ready,
                    job,
                })
            })
            .collect()
    }

    pub fn resolve_scheduled(&self, keys: &[Vec<u8>]) -> Vec<AdminJob> {
        let snap = self.db.read_tx();
        keys.iter()
            .filter_map(|key| {
                let job_id = std::str::from_utf8(TimerKey::job_id(key)?).ok()?;
                let attempt = snap
                    .get(&self.scheduled, key)
                    .ok()
                    .flatten()
                    .and_then(|v| v.first_chunk::<4>().map(|b| u32::from_be_bytes(*b)))?;
                let mut job = self.load_job(&snap, job_id)?;
                job.attempt = attempt;

                Some(AdminJob {
                    key: key.clone(),
                    state: AdminJobState::Scheduled,
                    job,
                })
            })
            .collect()
    }

    pub fn resolve_inflight(&self, keys: &[Vec<u8>]) -> Vec<AdminJob> {
        let snap = self.db.read_tx();
        keys.iter()
            .filter_map(|key| {
                let job_id = std::str::from_utf8(TimerKey::job_id(key)?).ok()?;
                let stored = snap.get(&self.inflight, job_id.as_bytes()).ok().flatten()?;
                let inflight = Inflight::decode(&stored).ok()?;
                // A peeked lease key goes stale when the job is extended or
                // re-reserved; only the key matching the live lease counts.
                if inflight.lease_expires_at != deadline_of(key) {
                    return None;
                }
                let mut job = self.load_job(&snap, job_id)?;
                job.attempt = inflight.attempt;
                job.lease_expires_at = Some(millis_to_timestamp(inflight.lease_expires_at));

                Some(AdminJob {
                    key: key.clone(),
                    state: AdminJobState::Inflight,
                    job,
                })
            })
            .collect()
    }

    pub fn resolve_dead_letters(&self, keys: &[Vec<u8>]) -> Vec<AdminDeadLetter> {
        let snap = self.db.read_tx();
        keys.iter()
            .filter_map(|key| {
                let stored = snap.get(&self.dead_letter, key).ok().flatten()?;
                // The record embeds the job and its payload (see
                // maybe_store_dead_letter); no further lookups needed.
                let record = DeadLetterRecord::decode(&*stored).ok()?;

                Some(AdminDeadLetter {
                    key: key.clone(),
                    record,
                })
            })
            .collect()
    }

    pub fn get_job(&self, job_id: &str) -> Option<AdminJob> {
        let snap = self.db.read_tx();
        let inflight = snap
            .get(&self.inflight, job_id.as_bytes())
            .ok()
            .flatten()
            .and_then(|stored| Inflight::decode(&stored).ok());

        let mut job = self.load_job(&snap, job_id)?;
        if let Some(inflight) = inflight {
            job.attempt = inflight.attempt;
            job.lease_expires_at = Some(millis_to_timestamp(inflight.lease_expires_at));
            let key = TimerKey {
                deadline: inflight.lease_expires_at,
                job_id,
            }
            .encode();

            return Some(AdminJob {
                key,
                state: AdminJobState::Inflight,
                job,
            });
        }

        // Keys are reconstructed best-effort: a nack-retry's timer deadline
        // is not recoverable from the job record, so such jobs report Ready.
        let scheduled_at = job.scheduled_at.as_ref().map(timestamp_to_millis);
        let (state, key) = match scheduled_at {
            Some(at) if at > now_ms() => (
                AdminJobState::Scheduled,
                TimerKey {
                    deadline: at,
                    job_id,
                }
                .encode(),
            ),
            _ => (
                AdminJobState::Ready,
                ReadyKey {
                    queue: &job.queue,
                    priority: job.priority,
                    enqueued_at: job
                        .enqueued_at
                        .as_ref()
                        .map(timestamp_to_millis)
                        .unwrap_or(0),
                    job_id,
                }
                .encode(),
            ),
        };

        Some(AdminJob { key, state, job })
    }
}

#[derive(Clone)]
pub struct Storage {
    tx: flume::Sender<Command>,
    notifiers: QueueNotifiers,
    read: ReadHandle,
    admin_stats: Arc<ArcSwap<AdminSnapshot>>,
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
            admin_enabled: config.admin.enabled,
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
        let read = ReadHandle {
            db: store.db.clone(),
            jobs: store.jobs.clone(),
            payloads: store.payloads.clone(),
            inflight: store.inflight.clone(),
            ready: store.ready.clone(),
            scheduled: store.scheduled.clone(),
            dead_letter: store.dead_letter.clone(),
        };
        let admin_stats = Arc::new(ArcSwap::from_pointee(AdminSnapshot::default()));
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
                let admin_stats = Arc::clone(&admin_stats);
                move || {
                    run_committer(
                        store,
                        indexes,
                        rx,
                        notifiers,
                        max_sweep_interval,
                        admin_stats,
                    )
                }
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

        Ok(Self {
            tx,
            notifiers,
            read,
            admin_stats,
        })
    }

    pub fn command_queue_depth(&self) -> usize {
        self.tx.len()
    }

    pub fn read_handle(&self) -> ReadHandle {
        self.read.clone()
    }

    pub fn admin_stats(&self) -> Arc<ArcSwap<AdminSnapshot>> {
        Arc::clone(&self.admin_stats)
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

    pub async fn peek_keys(
        &self,
        state: PeekState,
        queue: String,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<PeekPage, Status> {
        self.send(|resp| Command::PeekKeys {
            state,
            queue,
            cursor,
            limit,
            resp,
        })
        .await?
    }

    pub async fn requeue_dead_letters(
        &self,
        queue: String,
        keys: Vec<Vec<u8>>,
    ) -> Result<RequeueOutcome, Status> {
        self.send(|resp| Command::RequeueDeadLetters { queue, keys, resp })
            .await?
    }

    pub async fn dead_letter_jobs(
        &self,
        queue: String,
        state: PeekState,
        keys: Vec<Vec<u8>>,
        reason: Option<String>,
    ) -> Result<DeadLetterJobsOutcome, Status> {
        self.send(|resp| Command::DeadLetterJobs {
            queue,
            state,
            keys,
            reason,
            resp,
        })
        .await?
    }

    pub async fn delete_dead_letters(
        &self,
        queue: String,
        keys: Vec<Vec<u8>>,
    ) -> Result<DeleteOutcome, Status> {
        self.send(|resp| Command::DeleteDeadLetters { queue, keys, resp })
            .await?
    }

    pub async fn purge_queue_chunk(
        &self,
        queue: String,
        max: usize,
    ) -> Result<PurgeOutcome, Status> {
        self.send(|resp| Command::PurgeQueueChunk { queue, max, resp })
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
        DedupTimerKey {
            deadline,
            dedup_key,
        }
        .encode()
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
    fn reject_flags_only_storage_errors_as_fatal() {
        let (tx, _rx) = oneshot::channel::<Result<(), Status>>();
        assert!(reject(tx, Status::not_found("job not found")).is_ok());

        let (tx, _rx) = oneshot::channel::<Result<(), Status>>();
        assert!(reject(tx, Status::failed_precondition("attempt mismatch")).is_ok());

        let (tx, _rx) = oneshot::channel::<Result<(), Status>>();
        assert!(reject(tx, Status::resource_exhausted("queue full")).is_ok());

        let (tx, _rx) = oneshot::channel::<Result<(), Status>>();
        let fatal = reject(tx, Status::internal("storage error"));
        assert_eq!(fatal.unwrap_err().code(), tonic::Code::Internal);
    }

    #[test]
    fn reject_answers_the_caller_with_the_original_error() {
        let (tx, mut rx) = oneshot::channel::<Result<(), Status>>();
        let _ = reject(tx, Status::internal("storage error"));
        let sent = rx.try_recv().expect("responder is answered immediately");
        assert_eq!(sent.unwrap_err().code(), tonic::Code::Internal);
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

    #[test]
    fn peek_keys_pages_ready_with_a_cursor() {
        let mut indexes = Indexes::default();
        for i in 0..5i64 {
            indexes
                .ready
                .insert(ready_key("q", 5, 100 + i, &format!("j{i}")), 1);
        }
        indexes.ready.insert(ready_key("other", 5, 100, "x"), 1);

        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let page = peek_keys(&indexes, PeekState::Ready, "q", cursor, 2);
            assert!(!page.truncated);
            assert!(page.keys.len() <= 2);
            seen.extend(page.keys);
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        let expected: Vec<Vec<u8>> = (0..5i64)
            .map(|i| ready_key("q", 5, 100 + i, &format!("j{i}")))
            .collect();
        assert_eq!(seen, expected, "pages walk every key once, in order");
    }

    #[test]
    fn peek_keys_truncates_at_the_examined_cap() {
        let mut indexes = Indexes::default();
        for i in 0..PEEK_EXAMINE_CAP as i64 {
            indexes
                .dead_letter
                .insert(dead_letter_key(i, "noise", b"j"), "noise");
        }
        let wanted = dead_letter_key(PEEK_EXAMINE_CAP as i64, "wanted", b"j");
        indexes.dead_letter.insert(wanted.clone(), "wanted");

        let page = peek_keys(&indexes, PeekState::DeadLetter, "wanted", None, 10);
        assert!(page.truncated, "the cap hits before the page fills");
        assert!(page.keys.is_empty());
        let cursor = page
            .next_cursor
            .expect("a truncated page carries a resume cursor");

        let resumed = peek_keys(&indexes, PeekState::DeadLetter, "wanted", Some(cursor), 10);
        assert!(!resumed.truncated);
        assert_eq!(resumed.keys, vec![wanted]);
        assert_eq!(resumed.next_cursor, None);
    }

    #[test]
    fn admin_totals_fold_only_the_five_rpc_counters() {
        let mut m = CycleMetrics::default();
        m.enqueued_by_queue.insert("qa".into(), 3);
        m.reserved_by_queue.insert("qa".into(), 2);
        m.acked_by_queue.insert("qa".into(), 1);
        m.nacked_by_queue.insert("qb".into(), 4);
        m.dead_lettered_by_queue_cause
            .insert(("qb".into(), "rejected"), 1);
        m.dead_lettered_by_queue_cause
            .insert(("qb".into(), "attempts_exhausted"), 2);
        m.sweep_promotions_by_queue.insert("qc".into(), 9);

        let mut totals = HashMap::new();
        let mut last_active = HashMap::new();
        fold_admin_totals(&mut totals, &mut last_active, &m);
        fold_admin_totals(&mut totals, &mut last_active, &m);

        let qa = &totals["qa"];
        assert_eq!(
            (
                qa.enqueued,
                qa.reserved,
                qa.acked,
                qa.nacked,
                qa.dead_lettered
            ),
            (6, 4, 2, 0, 0)
        );
        let qb = &totals["qb"];
        assert_eq!((qb.nacked, qb.dead_lettered), (8, 6), "causes are summed");
        assert!(
            !totals.contains_key("qc"),
            "sweep counters do not feed totals"
        );
        assert!(last_active.contains_key("qa") && last_active.contains_key("qb"));
    }

    fn open_test_store() -> Store {
        let path = std::env::temp_dir().join(format!("sepp-storage-test-{}", Uuid::new_v4()));
        let db = TxDatabase::builder(path)
            .temporary(true)
            .open()
            .expect("open temporary db");
        let opts = KeyspaceCreateOptions::default;

        Store {
            jobs: db.keyspace("jobs", opts).unwrap(),
            payloads: db.keyspace("payloads", opts).unwrap(),
            inflight: db.keyspace("inflight", opts).unwrap(),
            ready: db.keyspace("ready", opts).unwrap(),
            dedup: db.keyspace("dedup", opts).unwrap(),
            dedup_timers: db.keyspace("dedup_timers", opts).unwrap(),
            scheduled: db.keyspace("scheduled", opts).unwrap(),
            leases: db.keyspace("leases", opts).unwrap(),
            dead_letter: db.keyspace("dead_letter", opts).unwrap(),
            db,
            params: StorageParams {
                persist_mode: PersistMode::Buffer,
                sweep_limit: 1000,
                dead_letter_retention_ms: 0,
                admin_enabled: true,
            },
            registry: crate::queues::QueueRegistry::from_config(&Config::default()).into_shared(),
            metrics: Metrics::new(false),
        }
    }

    #[test]
    fn purge_refuses_while_jobs_are_inflight() {
        let store = open_test_store();
        let mut indexes = Indexes::default();
        indexes.leases.insert(timer_key(500, "j1"), "q");

        let mut tx = store.db.write_tx();
        let mut cycle = Cycle::new(false);
        let err = apply_purge_queue_chunk(&store, &mut indexes, &mut tx, &mut cycle, "q", 100)
            .expect_err("in-flight jobs must block a purge");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(!cycle.dirty, "a refused purge writes nothing");

        indexes.leases.remove(&timer_key(500, "j1"));
        let outcome = apply_purge_queue_chunk(&store, &mut indexes, &mut tx, &mut cycle, "q", 100)
            .expect("an idle queue purges");
        assert_eq!(outcome.purged, 0);
        assert!(!outcome.remaining);
    }
}
