use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use fjall::Readable;
use openraft::EntryPayload;
use prost::Message;
use tokio::sync::{oneshot, watch};
use tonic::Status;
use tracing::{debug, error, info, warn};

use crate::keys::{APPLY_DIGEST_KEY, LAST_APPLIED_KEY, MEMBERSHIP_KEY, STAMP_HIGH_WATER_KEY};
use crate::metrics::CycleMetrics;
use crate::op::Op;
use crate::pb::sepp::v1::{DeadLetterRecord, Job};
use crate::raft::{Entry, StoredMembership, entry_to_proto, log_id_to_proto};

use super::*;

// A mutating operation in flight to the committer thread.
pub(crate) struct OpCommand {
    pub(crate) op: Op,
    pub(crate) resp: oneshot::Sender<Result<OpOutcome, Status>>,
}

// Read-only, answered from the applied in-memory indexes between batches.
pub(crate) enum ReadCommand {
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
pub enum OpOutcome {
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
    // Raft-path only: the filler for blank and membership entries (openraft
    // zips one response per entry) and a business rejection computed at
    // apply, which under raft is a replicated result, not an error.
    NonOp,
    Rejected(Status),
}

// An applied op's outcome parked until the cycle's commit decides whether the
// caller sees it or the commit error.
pub(crate) struct PendingReply {
    pub(crate) resp: oneshot::Sender<Result<OpOutcome, Status>>,
    pub(crate) outcome: OpOutcome,
}

impl PendingReply {
    pub(crate) fn respond(self, outcome: &Result<(), Status>) {
        let _ = self.resp.send(match outcome {
            Ok(()) => Ok(self.outcome),
            Err(e) => Err(e.clone()),
        });
    }
}

pub(crate) struct Cycle {
    pub(crate) dirty: bool,
    pub(crate) new_ready: HashSet<String>,
    // `None` when neither metrics nor admin stats want the deltas — every
    // recorder method becomes a no-op and we skip allocating into nine
    // HashMaps that would never be read.
    pub(crate) metrics: Option<CycleMetrics>,
}

impl Cycle {
    pub(crate) fn new(metrics_enabled: bool) -> Self {
        Self {
            dirty: false,
            new_ready: HashSet::new(),
            metrics: metrics_enabled.then(CycleMetrics::default),
        }
    }

    pub(crate) fn enqueued(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.enqueued_by_queue, queue);
        }
    }

    pub(crate) fn reserved(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.reserved_by_queue, queue);
        }
    }

    pub(crate) fn acked(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.acked_by_queue, queue);
        }
    }

    pub(crate) fn nacked(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.nacked_by_queue, queue);
        }
    }

    pub(crate) fn deduplicated(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.deduplicated_by_queue, queue);
        }
    }

    pub(crate) fn queue_purged(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            m.purged_queues.push(queue.to_string());
        }
    }

    pub(crate) fn dead_lettered(&mut self, queue: &str, cause: &'static str) {
        if let Some(m) = self.metrics.as_mut() {
            *m.dead_lettered_by_queue_cause
                .entry((queue.to_string(), cause))
                .or_default() += 1;
        }
    }

    pub(crate) fn sweep_promotion(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.sweep_promotions_by_queue, queue);
        }
    }

    pub(crate) fn sweep_lease_redelivery(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.sweep_lease_redeliveries_by_queue, queue);
        }
    }

    pub(crate) fn sweep_dedup_expiration(&mut self, queue: &str) {
        if let Some(m) = self.metrics.as_mut() {
            bump_queue(&mut m.sweep_dedup_expirations_by_queue, queue);
        }
    }

    pub(crate) fn dead_letter_expired(&mut self, n: u64) {
        if let Some(m) = self.metrics.as_mut() {
            m.dead_letters_expired += n;
        }
    }

    pub(crate) fn dead_letter_drained(&mut self, n: u64) {
        if let Some(m) = self.metrics.as_mut() {
            m.dead_letters_drained += n;
        }
    }
}

pub(crate) fn next_deadline(indexes: &Indexes, retention_ms: u64) -> Option<i64> {
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

pub(crate) fn fold_admin_totals(
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

pub(crate) fn evict_idle_admin_totals(
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

// Admin totals folded from cycle metrics, published to the SSE snapshot.
pub(crate) struct AdminFold {
    enabled: bool,
    stats: Arc<ArcSwap<AdminSnapshot>>,
    totals: HashMap<String, QueueTotals>,
    last_active: HashMap<String, i64>,
    last_published: Option<Instant>,
}

impl AdminFold {
    pub(crate) fn new(enabled: bool, stats: Arc<ArcSwap<AdminSnapshot>>) -> Self {
        Self {
            enabled,
            stats,
            totals: HashMap::new(),
            last_active: HashMap::new(),
            last_published: None,
        }
    }

    pub(crate) fn fold(&mut self, m: &CycleMetrics) {
        if !self.enabled {
            return;
        }

        // Purged-queue totals die with the queue; removing before the fold
        // keeps counts for jobs enqueued after the purge in the same batch.
        for queue in &m.purged_queues {
            self.totals.remove(queue);
            self.last_active.remove(queue);
        }

        fold_admin_totals(&mut self.totals, &mut self.last_active, m);
    }

    pub(crate) fn evict(&mut self, indexes: &Indexes) {
        if self.enabled {
            evict_idle_admin_totals(indexes, &mut self.totals, &mut self.last_active, now_ms());
        }
    }

    pub(crate) fn maybe_publish(&mut self, indexes: &Indexes) {
        if !self.enabled
            || self
                .last_published
                .is_some_and(|at| at.elapsed() < ADMIN_PUBLISH_INTERVAL)
        {
            return;
        }

        self.stats.store(Arc::new(AdminSnapshot {
            ts_ms: now_ms(),
            depths: indexes.snapshot(),
            totals: self.totals.clone(),
            command_queue_len: 0,
        }));

        self.last_published = Some(Instant::now());
    }
}

// A command from the raft state machine adapter to the committer thread.
// Dead-code allowance: reachable only through the raft path, which nothing
// in the server constructs until PR 8 wires the engine into boot.
#[allow(dead_code)]
pub(crate) enum SmCommand {
    Apply {
        entries: Vec<Entry>,
        resp: oneshot::Sender<Vec<OpOutcome>>,
    },
    // The install steps that need exclusive Store ownership: delete +
    // recreate + ingest, handle swap, index rebuild. The durable marker
    // protocol around them belongs to the adapter.
    Install {
        path: PathBuf,
        resp: oneshot::Sender<Result<(), Status>>,
    },
}

// The apply-a-batch unit: exclusive owner of the Store, the in-memory
// Indexes and the post-commit notifier wakes.
pub(crate) struct ApplyCore {
    store: Store,
    indexes: Indexes,
    notifiers: QueueNotifiers,
    deadline: watch::Sender<Option<i64>>,
    stamp: StampClamp,
    // Raft bookkeeping mirrored from `meta`: the divergence digest chain
    // head, the replicated stamp high-water mark and the applied log index.
    // Idle on the direct path (dead-code allowance as on SmCommand).
    #[allow(dead_code)]
    digest: [u8; 32],
    #[allow(dead_code)]
    stamp_high_water: i64,
    #[allow(dead_code)]
    last_applied_index: Option<u64>,
}

impl ApplyCore {
    pub(crate) fn new(
        store: Store,
        indexes: Indexes,
        notifiers: QueueNotifiers,
        stamp: StampClamp,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (deadline, _) = watch::channel(next_deadline(
            &indexes,
            store.params.dead_letter_retention_ms,
        ));
        let raft_state = load_raft_apply_state(&store)?;

        Ok(Self {
            store,
            indexes,
            notifiers,
            deadline,
            stamp,
            digest: raft_state.digest,
            stamp_high_water: raft_state.stamp_high_water,
            last_applied_index: raft_state.last_applied_index,
        })
    }

    pub(crate) fn subscribe_deadline(&self) -> watch::Receiver<Option<i64>> {
        self.deadline.subscribe()
    }

    pub(crate) fn run(
        mut self,
        ops_rx: flume::Receiver<OpCommand>,
        reads_rx: flume::Receiver<ReadCommand>,
        max_sweep_interval: Duration,
        mut admin: AdminFold,
    ) {
        enum Input {
            Op(OpCommand),
            Read(ReadCommand),
            Timeout,
            Disconnected,
        }

        let deadline = self.subscribe_deadline();
        loop {
            let sweep_due = (*deadline.borrow()).is_some_and(|d| d <= now_ms());
            if sweep_due {
                if let Some(m) = self.sweep() {
                    admin.fold(&m);
                }
                admin.evict(&self.indexes);
            }

            // A due sweep skips the blocking wait so queued commands are
            // picked up immediately; otherwise sleep until input, the next
            // deadline or the publish interval, whichever comes first.
            let first = if sweep_due {
                match ops_rx.try_recv() {
                    Ok(cmd) => Input::Op(cmd),
                    Err(flume::TryRecvError::Empty) => Input::Timeout,
                    Err(flume::TryRecvError::Disconnected) => Input::Disconnected,
                }
            } else {
                let wait = match *deadline.borrow() {
                    Some(d) => {
                        Duration::from_millis((d - now_ms()).max(0) as u64).min(max_sweep_interval)
                    }
                    None => max_sweep_interval,
                };
                match flume::Selector::new()
                    .recv(&ops_rx, |r| r.map(Input::Op).unwrap_or(Input::Disconnected))
                    .recv(&reads_rx, |r| {
                        r.map(Input::Read).unwrap_or(Input::Disconnected)
                    })
                    .wait_timeout(wait)
                {
                    Ok(input) => input,
                    Err(flume::select::SelectError::Timeout) => Input::Timeout,
                }
            };

            // Channels close only when every Storage handle has dropped,
            // which means the gRPC server has already stopped accepting
            // requests. Exit after the drain below: a disconnected channel
            // still yields already-accepted ops, and the selector can report
            // either channel first.
            let mut disconnected = false;
            let mut batch = Vec::new();
            match first {
                Input::Op(cmd) => batch.push(cmd),
                Input::Read(read) => self.answer(read),
                Input::Timeout => {}
                Input::Disconnected => disconnected = true,
            }

            while let Ok(read) = reads_rx.try_recv() {
                self.answer(read);
            }
            while let Ok(cmd) = ops_rx.try_recv() {
                batch.push(cmd);
            }

            if !batch.is_empty()
                && let Some(m) = self.apply_batch(batch)
            {
                admin.fold(&m);
            }

            if self.store.metrics.is_enabled() {
                self.store.metrics.set_queue_depths(self.indexes.snapshot());
            }

            // Runs on idle timeouts too, so a quiet server still refreshes
            // ts_ms.
            admin.maybe_publish(&self.indexes);

            if disconnected {
                break;
            }
        }

        info!("committer thread stopped; storage is no longer accepting commands");
    }

    pub(crate) fn apply_batch(&mut self, batch: Vec<OpCommand>) -> Option<CycleMetrics> {
        let metrics = self.batch_cycle(batch);
        self.publish_deadline();
        metrics
    }
}

// The raft apply path (dead-code allowance as on SmCommand).
#[allow(dead_code)]
impl ApplyCore {
    // The cluster-mode committer loop: committed entries and snapshot
    // installs arrive from the raft engine through the state machine adapter.
    // Sweeps are proposed by the leader's trigger task and arrive as ordinary
    // entries, so no local sweep evaluation runs here; the timeout arm keeps
    // the admin publish alive.
    pub(crate) fn run_raft(
        mut self,
        sm_rx: flume::Receiver<SmCommand>,
        reads_rx: flume::Receiver<ReadCommand>,
        mut admin: AdminFold,
    ) {
        enum Input {
            Sm(SmCommand),
            Read(ReadCommand),
            Timeout,
            SmDisconnected,
            ReadsDisconnected,
        }

        let mut reads_open = true;
        loop {
            let mut selector = flume::Selector::new().recv(&sm_rx, |r| {
                r.map(Input::Sm).unwrap_or(Input::SmDisconnected)
            });
            if reads_open {
                selector = selector.recv(&reads_rx, |r| {
                    r.map(Input::Read).unwrap_or(Input::ReadsDisconnected)
                });
            }

            match selector
                .wait_timeout(ADMIN_PUBLISH_INTERVAL)
                .unwrap_or(Input::Timeout)
            {
                Input::Sm(cmd) => self.handle_sm(cmd, &mut admin),
                Input::Read(read) => self.answer(read),
                Input::Timeout => {}
                // The engine dropped the adapter: shutdown. The read channel
                // closing first (Storage dropped) only removes its arm.
                Input::SmDisconnected => break,
                Input::ReadsDisconnected => reads_open = false,
            }

            while let Ok(read) = reads_rx.try_recv() {
                self.answer(read);
            }

            if self.store.metrics.is_enabled() {
                self.store.metrics.set_queue_depths(self.indexes.snapshot());
            }

            admin.maybe_publish(&self.indexes);
        }

        info!("committer thread stopped; the raft state machine input closed");
    }

    fn handle_sm(&mut self, cmd: SmCommand, admin: &mut AdminFold) {
        match cmd {
            SmCommand::Apply { entries, resp } => {
                let swept = entries
                    .iter()
                    .any(|e| matches!(&e.payload, EntryPayload::Normal(Op::Sweep { .. })));
                let (outcomes, metrics) = self.apply_entries(entries);
                if let Some(m) = metrics {
                    admin.fold(&m);
                }
                if swept {
                    admin.evict(&self.indexes);
                }
                let _ = resp.send(outcomes);
            }
            SmCommand::Install { path, resp } => {
                let _ = resp.send(self.install_snapshot(&path));
            }
        }
    }

    // The raft apply path: one committed batch = one write tx, exactly one
    // outcome per entry. Business rejections become outcomes; Internal errors
    // are fatal because a quorum-committed entry cannot be un-happened, so a
    // state machine that cannot apply it must not keep running.
    pub(crate) fn apply_entries(
        &mut self,
        entries: Vec<Entry>,
    ) -> (Vec<OpOutcome>, Option<CycleMetrics>) {
        if entries.is_empty() {
            return (Vec::new(), None);
        }

        let result = self.entries_cycle(entries);
        self.publish_deadline();
        result
    }

    fn entries_cycle(&mut self, entries: Vec<Entry>) -> (Vec<OpOutcome>, Option<CycleMetrics>) {
        let Self {
            store,
            indexes,
            notifiers,
            digest,
            stamp_high_water,
            last_applied_index,
            ..
        } = self;
        let mut tx = ApplyTx::new(store.db.write_tx());
        let mut cycle = Cycle::new(store.metrics.is_enabled() || store.params.admin_enabled);
        let mut outcomes = Vec::with_capacity(entries.len());
        let high_water_before = *stamp_high_water;
        let last = entries.last().expect("non-empty batch").log_id;

        for entry in &entries {
            // Re-apply is not idempotent, so an already-applied index must
            // fail-stop, never silently diverge. Forward gaps are the
            // engine's contract to prevent (and openraft's own test suite
            // legitimately skips indexes), so only the backward direction is
            // enforced here.
            if last_applied_index.is_some_and(|applied| entry.log_id.index <= applied) {
                error!(log_id = %entry.log_id, ?last_applied_index, "raft apply went backward");
                panic!(
                    "entry {} is at or below last_applied {:?}; re-apply is not idempotent",
                    entry.log_id, last_applied_index,
                );
            }
            *last_applied_index = Some(entry.log_id.index);

            let entry_bytes = entry_to_proto(entry).encode_to_vec();
            tx.begin_entry(digest, &entry_bytes);
            let outcome = match &entry.payload {
                EntryPayload::Blank | EntryPayload::Membership(_) => OpOutcome::NonOp,
                EntryPayload::Normal(op) => {
                    if let Some(stamp) = op.stamp_ms() {
                        *stamp_high_water = (*stamp_high_water).max(stamp);
                    }
                    match apply_op(store, indexes, &mut tx, &mut cycle, op.clone()) {
                        Ok(outcome) => outcome,
                        // A rejected op never touched the tx, so its recorded
                        // write-set is empty; the digest still advances over
                        // the entry bytes.
                        Err(e) if e.code() != tonic::Code::Internal => OpOutcome::Rejected(e),
                        Err(e) => {
                            error!(error = %e, log_id = %entry.log_id, "raft apply failed");
                            panic!("raft apply failed on a committed entry: {e}");
                        }
                    }
                }
            };
            *digest = tx.finish_entry();

            // Adapter bookkeeping stays outside the recorded scope so the
            // digest is batch-split-invariant; see ApplyTx.
            if let EntryPayload::Membership(m) = &entry.payload {
                let stored = StoredMembership::new(Some(entry.log_id), m.clone());
                tx.insert(
                    &store.meta,
                    MEMBERSHIP_KEY.to_vec(),
                    crate::raft::stored_membership_to_proto(&stored).encode_to_vec(),
                );
            }
            outcomes.push(outcome);
        }

        tx.insert(
            &store.meta,
            LAST_APPLIED_KEY.to_vec(),
            log_id_to_proto(&last).encode_to_vec(),
        );
        tx.insert(&store.meta, APPLY_DIGEST_KEY.to_vec(), digest.to_vec());
        if *stamp_high_water != high_water_before {
            tx.insert(
                &store.meta,
                STAMP_HIGH_WATER_KEY.to_vec(),
                stamp_high_water.to_be_bytes().to_vec(),
            );
        }

        // Unconditional commit: every entry advances last_applied, overriding
        // the direct path's cycle.dirty gate. No persist here: an apply tx
        // rides the next append-carrying IO cycle's persist and is re-derived
        // from the durable log after a crash.
        if let Err(e) = tx.commit() {
            error!(error = %e, "raft apply commit failed");
            panic!("raft apply commit failed: {e}");
        }

        if let Some(m) = &cycle.metrics {
            store.metrics.flush_cycle(m);
        }

        // Post-commit, on every node: harmless on followers, essential on a
        // deposed leader whose parked reserves must re-propose and redirect.
        for queue in &cycle.new_ready {
            notifiers.wake(queue);
        }

        (outcomes, cycle.metrics)
    }

    #[cfg(test)]
    pub(crate) fn store(&self) -> &Store {
        &self.store
    }

    #[cfg(test)]
    pub(crate) fn digest(&self) -> [u8; 32] {
        self.digest
    }

    #[cfg(test)]
    pub(crate) fn indexes(&self) -> &Indexes {
        &self.indexes
    }

    fn install_snapshot(&mut self, path: &Path) -> Result<(), Status> {
        let keyspaces = crate::raft::snapshot::ingest_snapshot_file(&self.store.db, path)
            .map_err(|e| Status::internal(format!("snapshot install failed: {e}")))?;
        self.store.swap_keyspaces(keyspaces);
        self.indexes = rebuild_indexes(&self.store).map_err(stg_err)?;

        // The installed meta carries the snapshot's digest, high-water and
        // applied index.
        let raft_state = load_raft_apply_state(&self.store)
            .map_err(|e| Status::internal(format!("installed meta is unreadable: {e}")))?;
        self.digest = raft_state.digest;
        self.stamp_high_water = raft_state.stamp_high_water;
        self.last_applied_index = raft_state.last_applied_index;
        self.publish_deadline();

        Ok(())
    }
}

impl ApplyCore {
    fn sweep(&mut self) -> Option<CycleMetrics> {
        let metrics = self.sweep_cycle();
        self.publish_deadline();
        metrics
    }

    fn batch_cycle(&mut self, batch: Vec<OpCommand>) -> Option<CycleMetrics> {
        let Self {
            store,
            indexes,
            notifiers,
            ..
        } = self;
        let mut tx = ApplyTx::new(store.db.write_tx());
        let mut cycle = Cycle::new(store.metrics.is_enabled() || store.params.admin_enabled);
        let mut responders: Vec<PendingReply> = Vec::with_capacity(batch.len());
        let mut batch = batch.into_iter();
        let fatal = batch.by_ref().find_map(|OpCommand { op, resp }| {
            match apply_op(store, indexes, &mut tx, &mut cycle, op) {
                Ok(outcome) => {
                    responders.push(PendingReply { resp, outcome });
                    None
                }
                Err(e) => reject(resp, e).err(),
            }
        });

        // A storage-level failure can leave the shared transaction holding
        // partial writes of a command whose caller was already told it failed;
        // committing those would persist effects of a failed RPC (and break
        // EnqueueAtomic's all-or-nothing contract). Drop the transaction and
        // fail the whole cycle.
        if let Some(status) = fatal {
            drop(tx);
            resync(store, indexes);

            for responder in responders {
                responder.respond(&Err(status.clone()));
            }

            for OpCommand { resp, .. } in batch {
                let _ = resp.send(Err(status.clone()));
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

    fn sweep_cycle(&mut self) -> Option<CycleMetrics> {
        let Self {
            store,
            indexes,
            notifiers,
            stamp,
            ..
        } = self;
        let started = Instant::now();
        let mut tx = ApplyTx::new(store.db.write_tx());
        let mut cycle = Cycle::new(store.metrics.is_enabled() || store.params.admin_enabled);

        let now = stamp.now_ms();
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

        // Post-commit, same reason as in batch_cycle.
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

    fn answer(&self, read: ReadCommand) {
        match read {
            ReadCommand::PeekKeys {
                state,
                queue,
                cursor,
                limit,
                resp,
            } => {
                let _ = resp.send(Ok(peek_keys(&self.indexes, state, &queue, cursor, limit)));
            }
            ReadCommand::QueueDepths { queue, resp } => {
                let _ = resp.send(Ok(self.indexes.depth_counts(&queue)));
            }
        }
    }

    fn publish_deadline(&self) {
        let next = next_deadline(&self.indexes, self.store.params.dead_letter_retention_ms);
        self.deadline.send_if_modified(|current| {
            let changed = *current != next;
            *current = next;
            changed
        });
    }
}

struct RaftApplyState {
    digest: [u8; 32],
    stamp_high_water: i64,
    last_applied_index: Option<u64>,
}

// Absent rows are a legal fresh state; malformed rows are corruption and
// refuse loudly at boot rather than seeding a digest that fail-stops the
// node much later.
fn load_raft_apply_state(
    store: &Store,
) -> Result<RaftApplyState, Box<dyn std::error::Error + Send + Sync>> {
    let snap = store.db.read_tx();
    let digest = match snap.get(&store.meta, APPLY_DIGEST_KEY)? {
        None => [0u8; 32],
        Some(v) => <[u8; 32]>::try_from(v.as_ref())
            .map_err(|_| format!("corrupt meta apply_digest row of {} bytes", v.len()))?,
    };
    let stamp_high_water = match snap.get(&store.meta, STAMP_HIGH_WATER_KEY)? {
        None => 0,
        Some(v) => <[u8; 8]>::try_from(v.as_ref())
            .map(i64::from_be_bytes)
            .map_err(|_| format!("corrupt meta stamp_high_water row of {} bytes", v.len()))?,
    };
    let last_applied_index = snap
        .get(&store.meta, LAST_APPLIED_KEY)?
        .map(|v| crate::pb::sepp::raft::v1::LogId::decode(v.as_ref()))
        .transpose()
        .map_err(|e| format!("corrupt meta last_applied row: {e}"))?
        .map(|msg| msg.index);

    Ok(RaftApplyState {
        digest,
        stamp_high_water,
        last_applied_index,
    })
}

// Storage failures are always Status::internal; business rejections (NotFound,
// FailedPrecondition) never are and never mutate the transaction before
// returning.
pub(crate) fn reject<T>(resp: oneshot::Sender<Result<T, Status>>, e: Status) -> Result<(), Status> {
    let fatal = (e.code() == tonic::Code::Internal).then(|| e.clone());
    let _ = resp.send(Err(e));
    match fatal {
        Some(status) => Err(status),
        None => Ok(()),
    }
}

pub(crate) fn commit_and_persist(store: &Store, tx: ApplyTx<'_>) -> Result<(), Status> {
    let started = Instant::now();
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
