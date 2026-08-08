use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use fjall::SingleWriterWriteTx as WriteTransaction;
use tokio::sync::{oneshot, watch};
use tonic::Status;
use tracing::{debug, error, info, warn};

use crate::metrics::CycleMetrics;
use crate::op::Op;
use crate::pb::sepp::v1::{DeadLetterRecord, Job};

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

// The apply-a-batch unit: exclusive owner of the Store, the in-memory
// Indexes and the post-commit notifier wakes.
pub(crate) struct ApplyCore {
    store: Store,
    indexes: Indexes,
    notifiers: QueueNotifiers,
    deadline: watch::Sender<Option<i64>>,
    stamp: StampClamp,
}

impl ApplyCore {
    pub(crate) fn new(
        store: Store,
        indexes: Indexes,
        notifiers: QueueNotifiers,
        stamp: StampClamp,
    ) -> Self {
        let (deadline, _) = watch::channel(next_deadline(
            &indexes,
            store.params.dead_letter_retention_ms,
        ));

        Self {
            store,
            indexes,
            notifiers,
            deadline,
            stamp,
        }
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
        let mut tx = store.db.write_tx();
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
        let mut tx = store.db.write_tx();
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

pub(crate) fn commit_and_persist(store: &Store, tx: WriteTransaction<'_>) -> Result<(), Status> {
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
