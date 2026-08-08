use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use arc_swap::ArcSwap;
use fjall::SingleWriterWriteTx as WriteTransaction;
use tokio::sync::oneshot;
use tonic::Status;
use tracing::{debug, error, info, warn};

use crate::metrics::CycleMetrics;
use crate::op::Op;
use crate::pb::sepp::v1::{DeadLetterRecord, Job};

use super::*;

pub(crate) enum Command {
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
pub(crate) enum OpOutcome {
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

pub(crate) fn run_committer(
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

pub(crate) fn run_rpc_cycle(
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
pub(crate) fn apply_command(
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

pub(crate) fn fail_command(cmd: Command, status: &Status) {
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

pub(crate) fn run_sweep_cycle(
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

pub(crate) fn commit_and_persist(store: &Store, tx: WriteTransaction<'_>) -> Result<(), Status> {
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
