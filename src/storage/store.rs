use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fjall::{
    PersistMode, Readable, SingleWriterTxDatabase as TxDatabase,
    SingleWriterTxKeyspace as TxKeyspace,
};
use tonic::Status;
use tracing::warn;

use crate::keys::read_queue;
use crate::metrics::Metrics;
use crate::queues::QueueRegistry;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as i64
}

// Propose-time clock: wall time clamped to never regress below the highest
// stamp already issued, so op stamps are non-decreasing across concurrent
// proposers even when the wall clock steps backward. Raft leadership
// acquisition later seeds the floor from the replicated stamp high-water.
#[derive(Clone)]
pub(crate) struct StampClamp(Arc<AtomicI64>);

impl StampClamp {
    pub(crate) fn new(floor: i64) -> Self {
        Self(Arc::new(AtomicI64::new(floor)))
    }

    pub(crate) fn now_ms(&self) -> i64 {
        let wall = now_ms();
        wall.max(self.0.fetch_max(wall, Ordering::Relaxed))
    }
}

pub(crate) struct StorageParams {
    pub(crate) persist_mode: PersistMode,
    pub(crate) sweep_limit: usize,
    pub(crate) dead_letter_retention_ms: u64,
    pub(crate) admin_enabled: bool,
}

pub(crate) struct Store {
    pub(crate) db: TxDatabase,
    pub(crate) jobs: TxKeyspace,
    pub(crate) payloads: TxKeyspace,
    pub(crate) inflight: TxKeyspace,
    pub(crate) ready: TxKeyspace,
    pub(crate) dedup: TxKeyspace,
    pub(crate) dedup_timers: TxKeyspace,
    pub(crate) scheduled: TxKeyspace,
    pub(crate) leases: TxKeyspace,
    pub(crate) dead_letter: TxKeyspace,
    pub(crate) meta: TxKeyspace,
    pub(crate) audit: TxKeyspace,
    pub(crate) params: StorageParams,
    pub(crate) metrics: Metrics,
}

pub(crate) fn stg_err(e: fjall::Error) -> Status {
    Status::internal(format!("storage error: {e}"))
}
pub(crate) fn warn_on_undeclared_persisted_queues(store: &Store, registry: &QueueRegistry) {
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
