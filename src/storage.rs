use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    future::Future,
    hash::{DefaultHasher, Hash, Hasher},
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

use crate::config::{Config, RetryBackoff};
use crate::keys::{
    AUDIT_SEQ_KEY, AuditValue, CLOSING_PREFIX, DeadLetterKey, DedupKey, DedupTimerKey, DedupValue,
    Inflight, JobValue, ReadyKey, TimerKey, closing_key, closing_queue, deadline_of, queue_prefix,
    read_queue,
};
use crate::metrics::{CycleMetrics, Metrics, QueueDepthSnapshot};
use crate::op::{Op, PreparedJob};
use crate::pb::sepp::storage::v1::AuditRecord;
use crate::pb::sepp::v1::{
    DeadLetterCause, DeadLetterRecord, EnqueueRequest, EnqueueResponse, ExtendRequest, Job,
    JobRejection, NackRequest, Payload, QueueClosing, QueueFull, TraceContext, job_rejection,
    nack_retry,
};
use crate::pb::{duration_to_millis, millis_to_timestamp, timestamp_to_millis};
use crate::queues::{QueueRegistry, RetryPolicy, SharedRegistry};
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
    meta: TxKeyspace,
    audit: TxKeyspace,
    params: StorageParams,
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
    // Queues an admin delete is draining, mapped to a grace deadline (ms). While
    // a queue is closing, enqueues to it are rejected (QueueClosing) so the
    // delete's purge loop is guaranteed to drain rather than livelock against a
    // concurrent producer. The deadline auto-clears the tombstone if the delete
    // handler dies; the handler refreshes it each chunk and clears it on finish.
    // Mirrors the `closing/<queue>` rows in `meta`.
    closing: HashMap<String, i64>,
}

impl Indexes {
    fn live_depth(&self, queue: &str) -> u64 {
        self.ready.by_queue.get(queue).copied().unwrap_or(0)
            + self.scheduled.by_queue.get(queue).copied().unwrap_or(0)
            + self.leases.by_queue.get(queue).copied().unwrap_or(0)
    }

    fn depth_counts(&self, queue: &str) -> QueueDepthCounts {
        QueueDepthCounts {
            ready: self.ready.by_queue.get(queue).copied().unwrap_or(0),
            scheduled: self.scheduled.by_queue.get(queue).copied().unwrap_or(0),
            inflight: self.leases.by_queue.get(queue).copied().unwrap_or(0),
            dead_letter: self.dead_letter.by_queue.get(queue).copied().unwrap_or(0),
        }
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

fn warn_on_undeclared_persisted_queues(store: &Store, registry: &QueueRegistry) {
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

    // Because sweep works based on the in-memory indexes, we must load them all at boot.
    // Otherwise the expired tombstones would be orphaned.
    for guard in snap.prefix(&store.meta, CLOSING_PREFIX) {
        let (key, value) = guard.into_inner()?;
        let Some(queue) = closing_queue(&key) else {
            continue;
        };
        let deadline = value
            .first_chunk::<8>()
            .map(|b| i64::from_be_bytes(*b))
            .unwrap_or(0);
        indexes.closing.insert(queue.to_string(), deadline);
    }

    Ok(indexes)
}

fn resync(store: &Store, indexes: &mut Indexes) {
    match rebuild_indexes(store) {
        Ok(fresh) => *indexes = fresh,
        Err(e) => error!(error = %e, "could not re-sync the in-memory indexes"),
    }
}

#[derive(Debug)]
pub struct AckOutcome {
    pub queue: String,
    pub trace_context: Option<TraceContext>,
}

#[derive(Debug)]
pub struct NackOutcome {
    pub queue: String,
    pub dead_lettered: bool,
    pub retry_delay_ms: u64,
    pub trace_context: Option<TraceContext>,
}

#[derive(Debug)]
pub struct ExtendOutcome {
    pub queue: String,
    pub lease_expires_at: i64,
    pub trace_context: Option<TraceContext>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
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
    pub job_ids: Vec<String>,
}

#[derive(Debug)]
pub struct DeadLetterJobsOutcome {
    pub dead_lettered: u64,
    pub missing: u64,
    pub job_ids: Vec<String>,
}

#[derive(Debug)]
pub struct DeleteOutcome {
    pub deleted: u64,
    pub missing: u64,
    pub job_ids: Vec<String>,
}

#[derive(Debug)]
pub struct PurgeOutcome {
    pub purged: u64,
    pub remaining: bool,
}

// Exact live per-queue depths from the in-memory by_queue counters, so the
// admin delete path can check emptiness without a capped key scan.
#[derive(Debug, Default)]
pub struct QueueDepthCounts {
    pub ready: u64,
    pub scheduled: u64,
    pub inflight: u64,
    pub dead_letter: u64,
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
// How long a close tombstone outlives its last refresh; see Indexes::closing.
const CLOSE_GRACE_MS: i64 = 30_000;

// Per-job outcome of a best-effort enqueue. Err carries a per-job rejection
// (currently only queue_full); storage failures stay whole-batch `Status`.
pub type EnqueueResult = Result<EnqueueResponse, JobRejection>;

// Outcome of an atomic enqueue: every job committed, or none were and the
// offending jobs are reported by position in the submitted batch.
#[derive(Debug)]
pub enum AtomicEnqueueOutcome {
    Committed(Vec<EnqueueResponse>),
    Rejected(Vec<(u32, JobRejection)>),
}

enum Command {
    // A mutating operation
    Op {
        op: Op,
        resp: oneshot::Sender<Result<OpOutcome, Status>>,
    },
    // Read-only
    PeekKeys {
        state: PeekState,
        queue: String,
        cursor: Option<Vec<u8>>,
        limit: usize,
        resp: oneshot::Sender<Result<PeekPage, Status>>,
    },
    QueueDepths {
        queue: String,
        resp: oneshot::Sender<Result<QueueDepthCounts, Status>>,
    },
}

#[derive(Debug)]
enum OpOutcome {
    Enqueue(Vec<EnqueueResult>),
    EnqueueAtomic(AtomicEnqueueOutcome),
    Reserve(Vec<Job>),
    Ack(AckOutcome),
    Nack(NackOutcome),
    Extend(ExtendOutcome),
    DrainDeadLetters(Vec<DeadLetterRecord>),
    CloseQueue,
    OpenQueue,
    RequeueDeadLetters(RequeueOutcome),
    DeadLetterJobs(DeadLetterJobsOutcome),
    DeleteDeadLetters(DeleteOutcome),
    PurgeQueueChunk(PurgeOutcome),
    Sweep(usize),
    AuditAppend { seq: u64, ts_ms: i64 },
}

// An applied op's outcome parked until the cycle's commit decides whether the
// caller sees it or the commit error.
struct PendingReply {
    resp: oneshot::Sender<Result<OpOutcome, Status>>,
    outcome: OpOutcome,
}

impl PendingReply {
    fn respond(self, outcome: &Result<(), Status>) {
        let _ = self.resp.send(match outcome {
            Ok(()) => Ok(self.outcome),
            Err(e) => Err(e.clone()),
        });
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
        indexes.closing.values().min().copied(),
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
    let mut responders: Vec<PendingReply> = Vec::with_capacity(rpcs.len());

    // One clock read per cycle: every op applies under the same stamp.
    let now = now_ms();
    let mut rpcs = rpcs.into_iter();
    let fatal = rpcs.by_ref().find_map(|cmd| {
        apply_command(
            store,
            indexes,
            &mut tx,
            &mut cycle,
            cmd,
            now,
            &mut responders,
        )
        .err()
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
    now: i64,
    responders: &mut Vec<PendingReply>,
) -> Result<(), Status> {
    match cmd {
        Command::Op { mut op, resp } => {
            op.stamp(now);
            match apply_op(store, indexes, tx, cycle, op) {
                Ok(outcome) => responders.push(PendingReply { resp, outcome }),
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
        // Read-only against the in-memory by_queue counters; answered inline.
        Command::QueueDepths { queue, resp } => {
            let _ = resp.send(Ok(indexes.depth_counts(&queue)));
        }
    }

    Ok(())
}

fn apply_op(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
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

// Storage failures are always Status::internal; business rejections (NotFound,
// FailedPrecondition) never are and never mutate the transaction before
// returning.
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
        Command::Op { resp, .. } => {
            let _ = resp.send(Err(status.clone()));
        }
        Command::PeekKeys { resp, .. } => {
            let _ = resp.send(Err(status.clone()));
        }
        Command::QueueDepths { resp, .. } => {
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

    let now = now_ms();
    let retention_ms = store.params.dead_letter_retention_ms;
    let op = Op::Sweep {
        now_ms: now,
        budget: store.params.sweep_limit,
        retention_cutoff_ms: (retention_ms > 0)
            .then(|| now.saturating_sub(i64::try_from(retention_ms).unwrap_or(i64::MAX))),
        dead_letter_enabled: retention_ms > 0,
    };
    let processed = match apply_op(store, indexes, &mut tx, &mut cycle, op) {
        Ok(OpOutcome::Sweep(processed)) => processed,
        Ok(_) => 0,
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
    let outcome = match tx.commit() {
        Ok(()) => match store.db.persist(store.params.persist_mode) {
            Ok(()) => Ok(()),
            Err(e) => {
                error!(error = %e, "storage persist failed; aborting");
                panic!("storage persist failed: {e}");
            }
        },
        // No need to panic here, commit is recoverable by design
        Err(e) => {
            error!(error = %e, "storage commit failed");
            Err(Status::internal("storage commit failed"))
        }
    };

    store.metrics.record_commit(started.elapsed());
    outcome
}

fn queue_full(queue: &str, cap: u64) -> JobRejection {
    JobRejection {
        reason: Some(job_rejection::Reason::QueueFull(QueueFull {
            queue: queue.to_string(),
            limit: cap,
        })),
    }
}

fn queue_closing(queue: &str) -> JobRejection {
    JobRejection {
        reason: Some(job_rejection::Reason::QueueClosing(QueueClosing {
            queue: queue.to_string(),
        })),
    }
}

enum DedupCheck {
    Hit(EnqueueResponse),
    Miss { stale_timer: Option<Vec<u8>> },
}

fn check_dedup(
    store: &Store,
    tx: &mut WriteTransaction<'_>,
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

fn apply_enqueue(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
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

fn apply_enqueue_atomic(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
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

fn insert_job(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
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
fn apply_reserve(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
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

// Stores the job in the DLQ, or drops it when the op says retention is
// disabled.
fn maybe_store_dead_letter(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
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
fn apply_drain(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
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
fn apply_dead_letter_jobs(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
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

fn apply_close_queue(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
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

fn apply_open_queue(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
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
fn retry_jitter_hash(job_id: &str, attempt: u32) -> u64 {
    let mut h = DefaultHasher::new();
    job_id.hash(&mut h);
    attempt.hash(&mut h);
    h.finish()
}

fn policy_retry_delay_ms(policy: &RetryPolicy, attempt: u32, job_id: &str) -> u64 {
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
fn apply_nack(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
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

fn apply_extend(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
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
fn apply_sweep(
    store: &Store,
    indexes: &mut Indexes,
    tx: &mut WriteTransaction<'_>,
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

fn apply_audit_append(
    store: &Store,
    tx: &mut WriteTransaction<'_>,
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

// Past this many distinct queue names, `get` prunes notifiers that no Reserve
// is parked on; otherwise every queue name ever reserved leaks an Arc<Notify>
// for the process lifetime.
const NOTIFIER_PRUNE_THRESHOLD: usize = 4096;

#[derive(Clone, Default)]
struct QueueNotifiers {
    map: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
}

impl QueueNotifiers {
    fn get(&self, queue: &str) -> Arc<Notify> {
        let mut map = self.map.lock().unwrap();
        // An entry is safe to drop only when no JobWaiter holds it, i.e.
        // strong_count == 1 (just the map); every access takes this one mutex,
        // so the count read can't race a concurrent clone. Pruning before the
        // insert keeps the O(n) retain amortized.
        if map.len() >= NOTIFIER_PRUNE_THRESHOLD {
            map.retain(|_, n| Arc::strong_count(n) > 1);
        }
        Arc::clone(
            map.entry(queue.to_owned())
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

pub struct AuditEntry {
    pub seq: u64,
    pub ts_ms: i64,
    pub record: AuditRecord,
}

#[derive(Default)]
pub struct AuditFilter {
    pub actor: Option<String>,
    pub action_prefix: Option<String>,
}

impl AuditFilter {
    fn matches(&self, record: &AuditRecord) -> bool {
        self.actor.as_deref().is_none_or(|a| record.actor == a)
            && self
                .action_prefix
                .as_deref()
                .is_none_or(|p| record.action.starts_with(p))
    }
}

pub struct AuditPage {
    pub entries: Vec<AuditEntry>,
    // Resume cursor: pass as `before` to continue the walk. None means the
    // scan reached the oldest entry.
    pub next_before: Option<u64>,
}

// Rows examined per list_audit call. Bounds the work of a filtered walk: a
// page can come back short (even empty) with next_before set instead of
// scanning arbitrarily far for the next match.
const AUDIT_SCAN_CAP: usize = 1_000;

// Snapshot-read view of the database for admin reads off the committer
// thread: point gets, plus the bounded audit range. Methods are sync (callers
// wrap them in spawn_blocking) and peeked keys can vanish between peek and
// resolve, so misses are silently skipped.
#[derive(Clone)]
pub struct ReadHandle {
    db: TxDatabase,
    jobs: TxKeyspace,
    payloads: TxKeyspace,
    inflight: TxKeyspace,
    ready: TxKeyspace,
    scheduled: TxKeyspace,
    dead_letter: TxKeyspace,
    audit: TxKeyspace,
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

    // A nack only carries the job ID, so we need a way to look up the queue
    // to get the effective limits applied for that queue. It's a point read
    // on a tx snapshot, which is cheap and doesn't contend with the committer
    // thread. A stale answer is harmless (attempt fencing rejects the op at
    // apply); a read error is not, so it fails the nack instead of resolving
    // a wrong delay.
    pub(crate) fn queue_of_inflight_job(&self, job_id: &str) -> Result<Option<String>, Status> {
        let snap = self.db.read_tx();
        let Some(stored) = snap
            .get(&self.inflight, job_id.as_bytes())
            .map_err(stg_err)?
        else {
            return Ok(None);
        };
        Ok(Some(Inflight::decode(&stored)?.queue))
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

    // The keyspace is cold (admin actions only) and every call scans at
    // most AUDIT_SCAN_CAP rows, so it does not contend with the committer.
    pub fn list_audit(&self, before: Option<u64>, limit: usize, filter: &AuditFilter) -> AuditPage {
        let snap = self.db.read_tx();
        let iter = match before {
            Some(seq) => snap.range(&self.audit, ..seq.to_be_bytes().to_vec()),
            None => snap.iter(&self.audit),
        };

        let mut entries = Vec::new();
        let mut last_seq = 0;
        for (scanned, guard) in iter.rev().enumerate() {
            // Checked before consuming the pulled row, so next_before is only
            // set when at least one unexamined row remains.
            if entries.len() >= limit || scanned >= AUDIT_SCAN_CAP {
                return AuditPage {
                    entries,
                    next_before: Some(last_seq),
                };
            }
            let Ok((key, value)) = guard.into_inner() else {
                continue;
            };
            let Ok(bytes) = <[u8; 8]>::try_from(key.as_ref()) else {
                continue;
            };
            last_seq = u64::from_be_bytes(bytes);
            if let Some((ts_ms, record)) = AuditValue::decode(&value)
                && filter.matches(&record)
            {
                entries.push(AuditEntry {
                    seq: last_seq,
                    ts_ms,
                    record,
                });
            }
        }

        AuditPage {
            entries,
            next_before: None,
        }
    }
}

#[derive(Clone)]
pub struct Storage {
    tx: flume::Sender<Command>,
    notifiers: QueueNotifiers,
    read: ReadHandle,
    admin_stats: Arc<ArcSwap<AdminSnapshot>>,
    drain_scan_cap: usize,
    registry: SharedRegistry,
    boot_registry: Arc<QueueRegistry>,
    dead_letter_enabled: bool,
}

impl Storage {
    pub fn open(
        config: &Config,
        registry: SharedRegistry,
        metrics: Metrics,
    ) -> Result<Self, Box<dyn std::error::Error>> {
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

        info!(db_path = %config.server.db_path, "opening storage...");
        let started = std::time::Instant::now();
        let db = builder.open()?;

        const FORMAT_VERSION: u64 = 1;
        const FORMAT_VERSION_KEY: &[u8] = b"format_version";
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default)?;
        match db.read_tx().get(&meta, FORMAT_VERSION_KEY)? {
            Some(v) if v.as_ref() == FORMAT_VERSION.to_be_bytes().as_slice() => {}
            Some(v) => {
                let found = <[u8; 8]>::try_from(v.as_ref())
                    .map(|b| u64::from_be_bytes(b).to_string())
                    .unwrap_or_else(|_| format!("unrecognized bytes {:?}", v.as_ref()));
                return Err(format!(
                    "refusing to open database at {:?}: its on-disk format version is {found}, \
                     this binary supports version {FORMAT_VERSION}",
                    config.server.db_path,
                )
                .into());
            }
            None => {
                let mut tx = db.write_tx();
                tx.insert(
                    &meta,
                    FORMAT_VERSION_KEY.to_vec(),
                    FORMAT_VERSION.to_be_bytes().to_vec(),
                );
                tx.commit()?;
                db.persist(PersistMode::SyncAll)?;
            }
        }

        if config.cluster.enabled {
            crate::cluster::verify_or_stamp_identity(&db, &config.cluster, &config.server.db_path)?;
        }

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
            meta,
            audit: db.keyspace("audit", KeyspaceCreateOptions::default)?,
            db,
            params,
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
            audit: store.audit.clone(),
        };
        let admin_stats = Arc::new(ArcSwap::from_pointee(AdminSnapshot::default()));
        let indexes = rebuild_indexes(&store)?;
        info!(
            elapsed_ms = started.elapsed().as_millis() as u64,
            ready = indexes.ready.by_queue.values().sum::<u64>(),
            scheduled = indexes.scheduled.by_queue.values().sum::<u64>(),
            inflight = indexes.leases.by_queue.values().sum::<u64>(),
            dead_letter = indexes.dead_letter.by_queue.values().sum::<u64>(),
            "storage ready",
        );

        if config.server.strict_queues {
            warn_on_undeclared_persisted_queues(&store, &registry.load());
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
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        run_committer(
                            store,
                            indexes,
                            rx,
                            notifiers,
                            max_sweep_interval,
                            admin_stats,
                        )
                    }));
                    if result.is_err() {
                        error!("committer thread panicked; aborting the process");
                        std::process::abort();
                    }
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
            drain_scan_cap: config.storage.sweep_limit,
            boot_registry: registry.load_full(),
            registry,
            dead_letter_enabled: config.storage.dead_letter_retention_ms > 0,
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

    async fn send_op(&self, op: Op) -> Result<OpOutcome, Status> {
        self.send(|resp| Command::Op { op, resp }).await?
    }

    // Each op kind produces exactly the outcome variant its handle method
    // unwraps; the pairing is fixed in apply_op. Reaching the fallback arm in
    // any method below is a bug.
    fn mismatched_outcome() -> Status {
        Status::internal("storage returned a mismatched op outcome")
    }

    fn prepare_jobs(&self, jobs: Vec<EnqueueRequest>) -> Vec<PreparedJob> {
        let live = self.registry.load();
        jobs.into_iter()
            .map(|req| PreparedJob::new(req, &live, &self.boot_registry))
            .collect()
    }

    pub async fn enqueue(&self, jobs: Vec<EnqueueRequest>) -> Result<Vec<EnqueueResult>, Status> {
        let jobs = self.prepare_jobs(jobs);
        // now_ms: 0 on every op built here; the committer stamps the real
        // drain time (see Op::stamp).
        match self.send_op(Op::Enqueue { jobs, now_ms: 0 }).await? {
            OpOutcome::Enqueue(results) => Ok(results),
            _ => Err(Self::mismatched_outcome()),
        }
    }

    pub async fn enqueue_atomic(
        &self,
        jobs: Vec<EnqueueRequest>,
    ) -> Result<AtomicEnqueueOutcome, Status> {
        let jobs = self.prepare_jobs(jobs);
        match self.send_op(Op::EnqueueAtomic { jobs, now_ms: 0 }).await? {
            OpOutcome::EnqueueAtomic(outcome) => Ok(outcome),
            _ => Err(Self::mismatched_outcome()),
        }
    }

    pub async fn reserve_once(
        &self,
        queues: Vec<String>,
        lease_ms: u64,
        max_jobs: usize,
    ) -> Result<Vec<Job>, Status> {
        let op = Op::Reserve {
            queues,
            lease_ms,
            max_jobs,
            now_ms: 0,
        };
        match self.send_op(op).await? {
            OpOutcome::Reserve(jobs) => Ok(jobs),
            _ => Err(Self::mismatched_outcome()),
        }
    }

    pub async fn ack(&self, job_id: String, attempt: u32) -> Result<AckOutcome, Status> {
        match self.send_op(Op::Ack { job_id, attempt }).await? {
            OpOutcome::Ack(outcome) => Ok(outcome),
            _ => Err(Self::mismatched_outcome()),
        }
    }

    pub async fn nack(&self, req: NackRequest) -> Result<NackOutcome, Status> {
        let retry_delay_ms = match req.retry.as_ref().and_then(|r| r.strategy.as_ref()) {
            // Apply dead-letters from the strategy; the delay is never read.
            Some(nack_retry::Strategy::DeadLetter(_)) => 0,
            _ => self.resolve_retry_delay(&req).await?,
        };
        match self
            .send_op(Op::Nack {
                req,
                retry_delay_ms,
                dead_letter_enabled: self.dead_letter_enabled,
                now_ms: 0,
            })
            .await?
        {
            OpOutcome::Nack(outcome) => Ok(outcome),
            _ => Err(Self::mismatched_outcome()),
        }
    }

    async fn resolve_retry_delay(&self, req: &NackRequest) -> Result<u64, Status> {
        let explicit = match req.retry.as_ref().and_then(|r| r.strategy.as_ref()) {
            Some(nack_retry::Strategy::Delay(delay)) => Some(duration_to_millis(delay)),
            _ => None,
        };
        // With no policy configured anywhere a directive-less nack is always
        // 0; skip the inflight lookup in that common case.
        if explicit.is_none() && !self.registry.load().any_retry_policy() {
            return Ok(0);
        }

        let read = self.read.clone();
        let job_id = req.job_id.clone();
        let queue = tokio::task::spawn_blocking(move || read.queue_of_inflight_job(&job_id))
            .await
            .map_err(|_| Status::internal("storage read task failed"))??;

        // Stale job. Will be fenced off.
        let Some(queue) = queue else { return Ok(0) };

        let policy = self.registry.load().retry_policy(&queue);
        Ok(match explicit {
            Some(ms) => ms.min(policy.max_schedule_horizon_ms),
            None => policy_retry_delay_ms(&policy, req.attempt, &req.job_id),
        })
    }

    pub async fn extend(&self, req: ExtendRequest) -> Result<ExtendOutcome, Status> {
        let lease_ms = self.resolve_extend_lease(&req).await?;
        match self
            .send_op(Op::Extend {
                req,
                lease_ms,
                now_ms: 0,
            })
            .await?
        {
            OpOutcome::Extend(outcome) => Ok(outcome),
            _ => Err(Self::mismatched_outcome()),
        }
    }

    // Like resolve_retry_delay: the job's queue names the max-lease clamp. A
    // job's queue never changes, so the read-side lookup racing the committer
    // is safe.
    async fn resolve_extend_lease(&self, req: &ExtendRequest) -> Result<u64, Status> {
        let requested = req
            .lease_duration
            .as_ref()
            .map(duration_to_millis)
            .unwrap_or(0)
            .max(1);

        let registry = self.registry.load();
        // Fast path, like any_retry_policy for nacks: at or under every
        // queue's ceiling the clamp is the identity, so skip the lookup.
        if requested <= registry.min_max_lease_ms() {
            return Ok(requested);
        }

        let read = self.read.clone();
        let job_id = req.job_id.clone();
        let queue = tokio::task::spawn_blocking(move || read.queue_of_inflight_job(&job_id))
            .await
            .map_err(|_| Status::internal("storage read task failed"))??;
        let Some(queue) = queue else {
            return Err(Status::not_found("job not found"));
        };

        let max_lease = registry.effective(&queue).max_lease_duration_ms;
        Ok(requested.min(max_lease))
    }

    pub async fn drain_dead_letters(
        &self,
        queue: Option<String>,
        max: usize,
    ) -> Result<Vec<DeadLetterRecord>, Status> {
        let op = Op::DrainDeadLetters {
            queue,
            max,
            scan_cap: self.drain_scan_cap,
        };
        match self.send_op(op).await? {
            OpOutcome::DrainDeadLetters(records) => Ok(records),
            _ => Err(Self::mismatched_outcome()),
        }
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

    pub async fn queue_depths(&self, queue: String) -> Result<QueueDepthCounts, Status> {
        self.send(|resp| Command::QueueDepths { queue, resp })
            .await?
    }

    // Durably tombstone a queue (refreshable) so enqueues to it are rejected
    // with QueueClosing while an admin delete drains it; open_queue clears it.
    pub async fn close_queue(&self, queue: String) -> Result<(), Status> {
        let op = Op::CloseQueue {
            queue,
            now_ms: 0,
            grace_ms: CLOSE_GRACE_MS,
        };
        match self.send_op(op).await? {
            OpOutcome::CloseQueue => Ok(()),
            _ => Err(Self::mismatched_outcome()),
        }
    }

    pub async fn open_queue(&self, queue: String) -> Result<(), Status> {
        match self.send_op(Op::OpenQueue { queue }).await? {
            OpOutcome::OpenQueue => Ok(()),
            _ => Err(Self::mismatched_outcome()),
        }
    }

    pub async fn requeue_dead_letters(
        &self,
        queue: String,
        keys: Vec<Vec<u8>>,
    ) -> Result<RequeueOutcome, Status> {
        let op = Op::RequeueDeadLetters {
            queue,
            keys,
            now_ms: 0,
        };
        match self.send_op(op).await? {
            OpOutcome::RequeueDeadLetters(outcome) => Ok(outcome),
            _ => Err(Self::mismatched_outcome()),
        }
    }

    pub async fn dead_letter_jobs(
        &self,
        queue: String,
        state: PeekState,
        keys: Vec<Vec<u8>>,
        reason: Option<String>,
    ) -> Result<DeadLetterJobsOutcome, Status> {
        let op = Op::DeadLetterJobs {
            queue,
            state,
            keys,
            reason,
            dead_letter_enabled: self.dead_letter_enabled,
            now_ms: 0,
        };
        match self.send_op(op).await? {
            OpOutcome::DeadLetterJobs(outcome) => Ok(outcome),
            _ => Err(Self::mismatched_outcome()),
        }
    }

    pub async fn delete_dead_letters(
        &self,
        queue: String,
        keys: Vec<Vec<u8>>,
    ) -> Result<DeleteOutcome, Status> {
        match self.send_op(Op::DeleteDeadLetters { queue, keys }).await? {
            OpOutcome::DeleteDeadLetters(outcome) => Ok(outcome),
            _ => Err(Self::mismatched_outcome()),
        }
    }

    pub async fn purge_queue_chunk(
        &self,
        queue: String,
        max: usize,
    ) -> Result<PurgeOutcome, Status> {
        let max = max.clamp(1, PURGE_CHUNK_MAX);
        match self.send_op(Op::PurgeQueueChunk { queue, max }).await? {
            OpOutcome::PurgeQueueChunk(outcome) => Ok(outcome),
            _ => Err(Self::mismatched_outcome()),
        }
    }

    // Returns the stored entry so the caller can fan it out (SSE) exactly as
    // a later page read would return it.
    pub async fn append_audit(&self, record: AuditRecord) -> Result<AuditEntry, Status> {
        let op = Op::AuditAppend {
            record: record.clone(),
            now_ms: 0,
        };
        match self.send_op(op).await? {
            OpOutcome::AuditAppend { seq, ts_ms } => Ok(AuditEntry { seq, ts_ms, record }),
            _ => Err(Self::mismatched_outcome()),
        }
    }
}

#[cfg(test)]
mod replay_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

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

    fn retry_limits(base: u64, backoff: RetryBackoff, max: u64) -> RetryPolicy {
        RetryPolicy {
            retry_delay_ms: base,
            retry_backoff: backoff,
            retry_delay_max_ms: max,
            max_schedule_horizon_ms: crate::config::LimitsConfig::default().max_schedule_horizon_ms,
        }
    }

    #[test]
    fn retry_policy_zero_base_is_immediate() {
        let limits = retry_limits(0, RetryBackoff::Exponential, 60_000);
        assert_eq!(policy_retry_delay_ms(&limits, 1, "job-1"), 0);
        assert_eq!(policy_retry_delay_ms(&limits, 7, "job-1"), 0);
    }

    #[test]
    fn retry_policy_fixed_delay_jitters_within_a_quarter() {
        let limits = retry_limits(10_000, RetryBackoff::None, 60_000);
        for attempt in 1..=5 {
            let d = policy_retry_delay_ms(&limits, attempt, "job-1");
            assert!((7_500..=10_000).contains(&d), "attempt {attempt}: {d}");
            assert_eq!(
                d,
                policy_retry_delay_ms(&limits, attempt, "job-1"),
                "same (job, attempt) must always produce the same delay"
            );
        }
    }

    #[test]
    fn retry_policy_exponential_doubles_and_caps() {
        let limits = retry_limits(1_000, RetryBackoff::Exponential, 10_000);
        let d1 = policy_retry_delay_ms(&limits, 1, "job-1");
        let d3 = policy_retry_delay_ms(&limits, 3, "job-1");
        assert!((750..=1_000).contains(&d1), "{d1}");
        assert!((3_000..=4_000).contains(&d3), "{d3}");
        // 2^59 growth saturates instead of overflowing, and the cap stays hard.
        for attempt in [5, 8, 60] {
            let d = policy_retry_delay_ms(&limits, attempt, "job-1");
            assert!((7_500..=10_000).contains(&d), "attempt {attempt}: {d}");
        }
        // At the cap the jitter must keep spreading jobs; a batch that failed
        // together must not retry in lockstep forever.
        let at_cap: std::collections::HashSet<u64> = (0..20)
            .map(|i| policy_retry_delay_ms(&limits, 60, &format!("job-{i}")))
            .collect();
        assert!(
            at_cap.len() > 10,
            "expected spread at the cap, got {at_cap:?}"
        );
    }

    #[test]
    fn retry_policy_respects_schedule_horizon() {
        let mut limits = retry_limits(8_000, RetryBackoff::None, 60_000);
        limits.max_schedule_horizon_ms = 5_000;
        let d = policy_retry_delay_ms(&limits, 1, "job-1");
        assert!((3_750..=5_000).contains(&d), "{d}");
    }

    #[test]
    fn retry_policy_jitter_spreads_jobs() {
        let limits = retry_limits(100_000, RetryBackoff::None, 1_000_000);
        let delays: std::collections::HashSet<u64> = (0..20)
            .map(|i| policy_retry_delay_ms(&limits, 1, &format!("job-{i}")))
            .collect();
        assert!(delays.len() > 10, "expected spread, got {delays:?}");
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

    #[test]
    fn open_refuses_unknown_format_version() {
        let path = std::env::temp_dir().join(format!("sepp-storage-test-{}", Uuid::new_v4()));
        // Pre-stamp the database with a format version this binary doesn't know.
        {
            let db = TxDatabase::builder(&path).open().expect("open db");
            let meta = db
                .keyspace("meta", KeyspaceCreateOptions::default)
                .expect("create meta keyspace");
            let mut tx = db.write_tx();
            tx.insert(
                &meta,
                b"format_version".to_vec(),
                2u64.to_be_bytes().to_vec(),
            );
            tx.commit().expect("commit version stamp");
            db.persist(PersistMode::SyncAll).expect("persist");
        }

        let mut config = Config::default();
        config.server.db_path = path.to_str().expect("utf-8 temp path").to_string();
        let registry = crate::queues::QueueRegistry::from_config(&config).into_shared();
        let err = Storage::open(&config, registry, Metrics::new(false))
            .err()
            .expect("open must refuse an unknown format version");
        assert!(
            err.to_string().contains("format version"),
            "unexpected error: {err}"
        );

        let _ = std::fs::remove_dir_all(&path);
    }

    // Stamps a cluster identity the way a first cluster-enabled boot would.
    fn stamp_identity(path: &std::path::Path, node_id: u64) {
        let db = TxDatabase::builder(path).open().expect("open db");
        let raft = db
            .keyspace("raft", KeyspaceCreateOptions::default)
            .expect("create raft keyspace");
        let mut tx = db.write_tx();
        tx.insert(&raft, b"node_id".to_vec(), node_id.to_be_bytes().to_vec());
        tx.insert(
            &raft,
            b"instance_uuid".to_vec(),
            Uuid::new_v4().as_bytes().to_vec(),
        );
        tx.commit().expect("commit identity");
        db.persist(PersistMode::SyncAll).expect("persist");
    }

    #[test]
    fn open_refuses_a_cluster_node_id_mismatch() {
        let path = std::env::temp_dir().join(format!("sepp-storage-test-{}", Uuid::new_v4()));
        stamp_identity(&path, 7);

        let mut config = Config::default();
        config.server.db_path = path.to_str().expect("utf-8 temp path").to_string();
        config.cluster.enabled = true;
        let registry = crate::queues::QueueRegistry::from_config(&config).into_shared();
        let err = Storage::open(&config, registry, Metrics::new(false))
            .err()
            .expect("open must refuse a node_id mismatch");
        assert!(
            err.to_string().contains("node_id"),
            "unexpected error: {err}"
        );

        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn cluster_identity_is_ignored_when_cluster_is_disabled() {
        let path = std::env::temp_dir().join(format!("sepp-storage-test-{}", Uuid::new_v4()));
        stamp_identity(&path, 7);

        // Disabled cluster mode never reads the raft keyspace, so the
        // mismatched stamp is invisible: exactly today's behavior.
        let mut config = Config::default();
        config.server.db_path = path.to_str().expect("utf-8 temp path").to_string();
        let registry = crate::queues::QueueRegistry::from_config(&config).into_shared();
        let _storage = Storage::open(&config, registry, Metrics::new(false))
            .expect("disabled cluster mode ignores identity stamps");

        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn prepare_jobs_resolves_live_limits_and_boot_dedup_window() {
        let path = std::env::temp_dir().join(format!("sepp-storage-test-{}", Uuid::new_v4()));
        let mut boot_cfg = Config::default();
        boot_cfg.server.db_path = path.to_str().expect("utf-8 temp path").to_string();
        boot_cfg.storage.dedup_window_ms = 1;
        let registry = QueueRegistry::from_config(&boot_cfg).into_shared();
        let storage =
            Storage::open(&boot_cfg, registry.clone(), Metrics::new(false)).expect("open storage");

        // A hot reload changes both knobs; only the priority may follow it.
        let mut live_cfg = Config::default();
        live_cfg.storage.dedup_window_ms = 3_600_000;
        live_cfg.limits.default_priority = 7;
        crate::queues::publish(&registry, QueueRegistry::from_config(&live_cfg));

        let jobs = storage.prepare_jobs(vec![test_req("q")]);
        assert_eq!(jobs[0].limits.priority, 7, "live limits follow the reload");
        assert_eq!(
            jobs[0].limits.dedup_window_ms, 1,
            "the dedup window stays boot-pinned"
        );

        drop(storage);
        let _ = std::fs::remove_dir_all(&path);
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
            meta: db.keyspace("meta", opts).unwrap(),
            audit: db.keyspace("audit", opts).unwrap(),
            db,
            params: StorageParams {
                persist_mode: PersistMode::Buffer,
                sweep_limit: 1000,
                dead_letter_retention_ms: 0,
                admin_enabled: true,
            },
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

    fn test_req(queue: &str) -> EnqueueRequest {
        EnqueueRequest {
            queue: queue.to_string(),
            job_type: "t".to_string(),
            ..Default::default()
        }
    }

    fn test_job(queue: &str) -> PreparedJob {
        let registry = QueueRegistry::from_config(&Config::default());
        PreparedJob::new(test_req(queue), &registry, &registry)
    }

    #[test]
    fn enqueue_rejects_jobs_for_a_closing_queue() {
        let store = open_test_store();
        let mut indexes = Indexes::default();
        indexes.closing.insert("q".into(), now_ms() + 60_000);

        let mut tx = store.db.write_tx();
        let mut cycle = Cycle::new(false);
        let results = apply_enqueue(
            &store,
            &mut indexes,
            &mut tx,
            &mut cycle,
            vec![test_job("q"), test_job("other")],
            now_ms(),
        )
        .expect("enqueue applies");

        match &results[0] {
            Err(JobRejection {
                reason: Some(job_rejection::Reason::QueueClosing(r)),
            }) => assert_eq!(r.queue, "q"),
            other => panic!("expected a QueueClosing rejection, got {other:?}"),
        }
        assert!(results[1].is_ok(), "other queues are unaffected");

        // An expired tombstone (its delete handler died) no longer rejects.
        indexes.closing.insert("q".into(), now_ms() - 1);
        let results = apply_enqueue(
            &store,
            &mut indexes,
            &mut tx,
            &mut cycle,
            vec![test_job("q")],
            now_ms(),
        )
        .expect("enqueue applies");
        assert!(results[0].is_ok());
    }

    #[test]
    fn atomic_enqueue_rejects_the_whole_batch_for_a_closing_queue() {
        let store = open_test_store();
        let mut indexes = Indexes::default();
        indexes.closing.insert("q".into(), now_ms() + 60_000);

        let mut tx = store.db.write_tx();
        let mut cycle = Cycle::new(false);
        let outcome = apply_enqueue_atomic(
            &store,
            &mut indexes,
            &mut tx,
            &mut cycle,
            vec![test_job("other"), test_job("q")],
            now_ms(),
        )
        .expect("atomic enqueue applies");

        match outcome {
            AtomicEnqueueOutcome::Rejected(rejections) => {
                assert_eq!(rejections.len(), 1);
                assert_eq!(rejections[0].0, 1, "the offending index is reported");
                assert!(matches!(
                    rejections[0].1.reason,
                    Some(job_rejection::Reason::QueueClosing(_))
                ));
            }
            AtomicEnqueueOutcome::Committed(_) => panic!("the batch must not commit"),
        }
        assert_eq!(
            indexes.live_depth("other"),
            0,
            "nothing from the rejected batch is inserted"
        );
    }

    #[test]
    fn sweep_drops_only_expired_close_tombstones() {
        let store = open_test_store();
        let mut indexes = Indexes::default();
        let mut tx = store.db.write_tx();
        let mut cycle = Cycle::new(false);
        apply_close_queue(
            &store,
            &mut indexes,
            &mut tx,
            &mut cycle,
            "active".into(),
            now_ms(),
            CLOSE_GRACE_MS,
        );
        // An expired tombstone, as if its delete handler died a while ago.
        indexes.closing.insert("abandoned".into(), now_ms() - 1);
        tx.insert(
            &store.meta,
            closing_key("abandoned"),
            (now_ms() - 1).to_be_bytes().to_vec(),
        );

        apply_sweep(
            &store,
            &mut indexes,
            &mut tx,
            &mut cycle,
            now_ms(),
            1000,
            None,
            false,
        )
        .expect("sweep applies");

        assert!(!indexes.closing.contains_key("abandoned"));
        assert!(indexes.closing.contains_key("active"));
        let row = |queue| tx.get(&store.meta, closing_key(queue)).expect("meta reads");
        assert!(row("abandoned").is_none(), "the expired row is deleted");
        assert!(row("active").is_some(), "the live row survives");
    }

    #[test]
    fn sweep_reads_the_retention_cutoff_from_the_op() {
        // The test store has dead_letter_retention_ms = 0; a cutoff riding in
        // the op must still expire dead letters, because a future follower
        // applies with the leader's scalars, not its own config.
        let store = open_test_store();
        let mut indexes = Indexes::default();
        let key = dead_letter_key(100, "q", b"j1");
        indexes.dead_letter.insert(key.clone(), "q");

        let mut tx = store.db.write_tx();
        tx.insert(&store.dead_letter, key.clone(), Vec::new());
        let mut cycle = Cycle::new(false);
        let processed = apply_sweep(
            &store,
            &mut indexes,
            &mut tx,
            &mut cycle,
            1_000,
            1000,
            Some(200),
            true,
        )
        .expect("sweep applies");

        assert_eq!(processed, 1);
        assert!(indexes.dead_letter.keys.is_empty());
        assert!(
            tx.get(&store.dead_letter, &key).expect("reads").is_none(),
            "the expired dead letter is deleted"
        );
    }

    #[test]
    fn close_tombstone_survives_a_rebuild() {
        let store = open_test_store();
        let mut indexes = Indexes::default();
        let mut tx = store.db.write_tx();
        let mut cycle = Cycle::new(false);
        apply_close_queue(
            &store,
            &mut indexes,
            &mut tx,
            &mut cycle,
            "q".into(),
            now_ms(),
            CLOSE_GRACE_MS,
        );
        assert!(cycle.dirty, "a close writes its tombstone durably");
        tx.commit().expect("commit");

        // As after a mid-purge restart: the rebuilt indexes still reject.
        let rebuilt = rebuild_indexes(&store).expect("rebuild");
        assert_eq!(rebuilt.closing.get("q"), indexes.closing.get("q"));

        let mut tx = store.db.write_tx();
        let mut cycle = Cycle::new(false);
        apply_open_queue(&store, &mut indexes, &mut tx, &mut cycle, "q");
        assert!(cycle.dirty);
        tx.commit().expect("commit");

        let rebuilt = rebuild_indexes(&store).expect("rebuild");
        assert!(rebuilt.closing.is_empty(), "open deletes the tombstone row");

        let mut tx = store.db.write_tx();
        let mut cycle = Cycle::new(false);
        apply_open_queue(&store, &mut indexes, &mut tx, &mut cycle, "q");
        assert!(
            !cycle.dirty,
            "opening a queue that isn't closing is a no-op"
        );
    }

    fn audit_record(actor: &str, action: &str) -> AuditRecord {
        AuditRecord {
            actor: actor.into(),
            role: "admin".into(),
            action: action.into(),
            details_json: "{}".into(),
        }
    }

    fn audit_read_handle(store: &Store) -> ReadHandle {
        ReadHandle {
            db: store.db.clone(),
            jobs: store.jobs.clone(),
            payloads: store.payloads.clone(),
            inflight: store.inflight.clone(),
            ready: store.ready.clone(),
            scheduled: store.scheduled.clone(),
            dead_letter: store.dead_letter.clone(),
            audit: store.audit.clone(),
        }
    }

    #[test]
    fn audit_appends_read_back_newest_first_with_cursor() {
        let store = open_test_store();

        let mut tx = store.db.write_tx();
        let mut cycle = Cycle::new(false);
        apply_audit_append(
            &store,
            &mut tx,
            &mut cycle,
            &audit_record("root", "a.one"),
            1_000,
        )
        .expect("applies");
        apply_audit_append(
            &store,
            &mut tx,
            &mut cycle,
            &audit_record("root", "a.two"),
            1_000,
        )
        .expect("applies");
        assert!(cycle.dirty);
        tx.commit().expect("commit");

        // A later cycle continues from the persisted counter.
        let mut tx = store.db.write_tx();
        let mut cycle = Cycle::new(false);
        apply_audit_append(
            &store,
            &mut tx,
            &mut cycle,
            &audit_record("root", "a.three"),
            2_000,
        )
        .expect("applies");
        tx.commit().expect("commit");

        let read = audit_read_handle(&store);
        let all = AuditFilter::default();
        let brief = |page: AuditPage| {
            page.entries
                .into_iter()
                .map(|e| (e.seq, e.ts_ms, e.record.action))
                .collect::<Vec<_>>()
        };

        let first = read.list_audit(None, 2, &all);
        assert_eq!(
            first.next_before,
            Some(2),
            "a full page with rows left reports a resume cursor"
        );
        assert_eq!(
            brief(first),
            vec![(3, 2_000, "a.three".into()), (2, 1_000, "a.two".into())]
        );

        let rest = read.list_audit(Some(2), 10, &all);
        assert_eq!(rest.next_before, None, "the scan reached the oldest entry");
        assert_eq!(
            brief(rest),
            vec![(1, 1_000, "a.one".into())],
            "the cursor page excludes the cursor itself"
        );

        let exact = read.list_audit(None, 3, &all);
        assert_eq!(exact.entries.len(), 3);
        assert_eq!(
            exact.next_before, None,
            "a page ending exactly at the oldest entry has no cursor"
        );

        assert!(read.list_audit(Some(1), 10, &all).entries.is_empty());
    }

    #[test]
    fn audit_listing_filters_by_actor_and_action_prefix() {
        let store = open_test_store();

        let mut tx = store.db.write_tx();
        let mut cycle = Cycle::new(false);
        for (actor, action) in [
            ("root", "job.enqueue"),
            ("o", "session.login"),
            ("root", "job.delete"),
        ] {
            apply_audit_append(&store, &mut tx, &mut cycle, &audit_record(actor, action), 0)
                .expect("applies");
        }
        tx.commit().expect("commit");

        let read = audit_read_handle(&store);
        let seqs = |page: AuditPage| page.entries.into_iter().map(|e| e.seq).collect::<Vec<_>>();

        let by_actor = AuditFilter {
            actor: Some("root".into()),
            ..Default::default()
        };
        assert_eq!(seqs(read.list_audit(None, 10, &by_actor)), vec![3, 1]);

        let by_prefix = AuditFilter {
            action_prefix: Some("session.".into()),
            ..Default::default()
        };
        assert_eq!(seqs(read.list_audit(None, 10, &by_prefix)), vec![2]);

        let both = AuditFilter {
            actor: Some("o".into()),
            action_prefix: Some("job.".into()),
        };
        let page = read.list_audit(None, 10, &both);
        assert!(page.entries.is_empty());
        assert_eq!(
            page.next_before, None,
            "an exhausted scan reports no cursor even when nothing matched"
        );
    }

    #[test]
    fn audit_listing_bounds_scan_work_under_a_selective_filter() {
        let store = open_test_store();

        let mut tx = store.db.write_tx();
        let mut cycle = Cycle::new(false);
        for _ in 0..AUDIT_SCAN_CAP + 2 {
            apply_audit_append(
                &store,
                &mut tx,
                &mut cycle,
                &audit_record("root", "job.enqueue"),
                0,
            )
            .expect("applies");
        }
        tx.commit().expect("commit");

        let read = audit_read_handle(&store);
        let nobody = AuditFilter {
            actor: Some("nobody".into()),
            ..Default::default()
        };

        let page = read.list_audit(None, 10, &nobody);
        assert!(page.entries.is_empty());
        assert_eq!(
            page.next_before,
            Some(3),
            "the cap stops the walk with a resume cursor"
        );

        let page = read.list_audit(Some(3), 10, &nobody);
        assert!(page.entries.is_empty());
        assert_eq!(page.next_before, None);
    }

    #[test]
    fn next_deadline_considers_close_tombstones() {
        let mut indexes = Indexes::default();
        assert_eq!(next_deadline(&indexes, 0), None);
        indexes.closing.insert("q".into(), 1234);
        assert_eq!(next_deadline(&indexes, 0), Some(1234));
    }

    #[test]
    fn dedup_window_is_pinned_at_boot() {
        // The pin now lives at propose time: PreparedJob::new resolves the
        // dedup window from the boot registry, not the live (hot-reloaded)
        // one, and apply only ever sees the carried value.
        let store = open_test_store();
        let mut boot_cfg = Config::default();
        boot_cfg.storage.dedup_window_ms = 1;
        let boot = QueueRegistry::from_config(&boot_cfg);
        // An hour-long hot-reloaded window must not stretch the boot window.
        let mut live_cfg = Config::default();
        live_cfg.storage.dedup_window_ms = 3_600_000;
        let live = QueueRegistry::from_config(&live_cfg);

        let mut indexes = Indexes::default();
        let mut tx = store.db.write_tx();
        let mut cycle = Cycle::new(false);
        let req = EnqueueRequest {
            idempotency_key: Some("k".to_string()),
            ..test_req("q")
        };
        let first = apply_enqueue(
            &store,
            &mut indexes,
            &mut tx,
            &mut cycle,
            vec![PreparedJob::new(req.clone(), &live, &boot)],
            now_ms(),
        )
        .expect("enqueue applies")
        .remove(0)
        .expect("the first enqueue is accepted");

        // Outlive the 1ms boot window. The dedup record itself is still there
        // (no sweep has run), so a deadline from the live window would hit.
        std::thread::sleep(std::time::Duration::from_millis(10));
        let second = apply_enqueue(
            &store,
            &mut indexes,
            &mut tx,
            &mut cycle,
            vec![PreparedJob::new(req, &live, &boot)],
            now_ms(),
        )
        .expect("enqueue applies")
        .remove(0)
        .expect("the second enqueue is accepted");

        assert!(
            !second.deduplicated,
            "the boot window, not the live one, bounds the dedup deadline"
        );
        assert_ne!(second.job_id, first.job_id);
    }
}
