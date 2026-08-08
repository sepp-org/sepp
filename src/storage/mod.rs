use std::{collections::HashMap, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use fjall::{
    KeyspaceCreateOptions, KvSeparationOptions, PersistMode, Readable,
    SingleWriterTxDatabase as TxDatabase,
};
use tokio::sync::oneshot;
use tonic::Status;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::metrics::{Metrics, QueueDepthSnapshot};
use crate::op::{Op, PreparedJob};
use crate::pb::duration_to_millis;
use crate::pb::sepp::storage::v1::AuditRecord;
use crate::pb::sepp::v1::{
    DeadLetterRecord, EnqueueRequest, EnqueueResponse, ExtendRequest, Job, JobRejection,
    NackRequest, TraceContext, nack_retry,
};
use crate::queues::{QueueRegistry, SharedRegistry};

mod apply;
mod committer;
mod indexes;
mod read;
mod store;

pub(crate) use apply::*;
pub(crate) use committer::*;
pub(crate) use indexes::*;
pub use read::*;
pub use store::*;

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
mod tests;
