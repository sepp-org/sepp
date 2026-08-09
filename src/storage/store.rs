use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fjall::{
    KeyspaceCreateOptions, KvSeparationOptions, PersistMode, Readable,
    SingleWriterTxDatabase as TxDatabase, SingleWriterTxKeyspace as TxKeyspace,
    SingleWriterWriteTx as WriteTransaction, UserKey, UserValue,
};
use sha2::{Digest as _, Sha256};
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

// The state machine keyspaces in canonical snapshot order. `raft` and
// `raft_log` are deliberately absent: vote, identity and the log must never
// ride in snapshots.
pub(crate) const SM_KEYSPACES: [&str; 11] = [
    "jobs",
    "payloads",
    "inflight",
    "ready",
    "dedup",
    "dedup_timers",
    "scheduled",
    "leases",
    "dead_letter",
    "meta",
    "audit",
];

// Handles to the state machine keyspaces. Snapshot install replaces the
// keyspaces on disk (delete + recreate gives them fresh internal ids), so
// everything that holds these handles must swap them afterwards.
pub(crate) struct Keyspaces {
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
}

impl Keyspaces {
    pub(crate) fn open(db: &TxDatabase) -> Result<Self, fjall::Error> {
        // Most reads we make in the hot path will have a match
        let hits = || KeyspaceCreateOptions::default().expect_point_read_hits(true);
        Ok(Self {
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
            meta: db.keyspace("meta", KeyspaceCreateOptions::default)?,
            audit: db.keyspace("audit", KeyspaceCreateOptions::default)?,
        })
    }

    pub(crate) fn by_name(&self, name: &str) -> Option<&TxKeyspace> {
        Some(match name {
            "jobs" => &self.jobs,
            "payloads" => &self.payloads,
            "inflight" => &self.inflight,
            "ready" => &self.ready,
            "dedup" => &self.dedup,
            "dedup_timers" => &self.dedup_timers,
            "scheduled" => &self.scheduled,
            "leases" => &self.leases,
            "dead_letter" => &self.dead_letter,
            "meta" => &self.meta,
            "audit" => &self.audit,
            _ => return None,
        })
    }
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

impl Store {
    pub(crate) fn new(
        db: TxDatabase,
        ks: Keyspaces,
        params: StorageParams,
        metrics: Metrics,
    ) -> Self {
        Self {
            db,
            jobs: ks.jobs,
            payloads: ks.payloads,
            inflight: ks.inflight,
            ready: ks.ready,
            dedup: ks.dedup,
            dedup_timers: ks.dedup_timers,
            scheduled: ks.scheduled,
            leases: ks.leases,
            dead_letter: ks.dead_letter,
            meta: ks.meta,
            audit: ks.audit,
            params,
            metrics,
        }
    }

    // Snapshot install replaced the keyspaces on disk; the old handles are
    // marked deleted and would error on every access. Dead-code allowance:
    // reachable only through the raft path until PR 8 wires the engine in.
    #[allow(dead_code)]
    pub(crate) fn swap_keyspaces(&mut self, ks: Keyspaces) {
        self.jobs = ks.jobs;
        self.payloads = ks.payloads;
        self.inflight = ks.inflight;
        self.ready = ks.ready;
        self.dedup = ks.dedup;
        self.dedup_timers = ks.dedup_timers;
        self.scheduled = ks.scheduled;
        self.leases = ks.leases;
        self.dead_letter = ks.dead_letter;
        self.meta = ks.meta;
        self.audit = ks.audit;
    }
}

// The apply-path write transaction: forwards to fjall and, between
// begin_entry and finish_entry, folds every write into the entry's divergence
// digest as (keyspace, key, value) triples in write order. The raft adapter's
// own meta rows are written outside the recording scope, which keeps the
// digest independent of how entries were batched into transactions.
pub(crate) struct ApplyTx<'a> {
    tx: WriteTransaction<'a>,
    digest: Option<Sha256>,
}

impl<'a> ApplyTx<'a> {
    pub(crate) fn new(tx: WriteTransaction<'a>) -> Self {
        Self { tx, digest: None }
    }

    // Dead-code allowance on the digest scope: reachable only through the
    // raft path until PR 8 wires the engine in.
    #[allow(dead_code)]
    pub(crate) fn begin_entry(&mut self, prev: &[u8; 32], entry_bytes: &[u8]) {
        let mut hasher = Sha256::new();
        hasher.update(prev);
        hasher.update((entry_bytes.len() as u64).to_be_bytes());
        hasher.update(entry_bytes);
        self.digest = Some(hasher);
    }

    #[allow(dead_code)]
    pub(crate) fn finish_entry(&mut self) -> [u8; 32] {
        self.digest
            .take()
            .expect("finish_entry without begin_entry")
            .finalize()
            .into()
    }

    pub(crate) fn get<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<fjall::Keyspace>,
        key: K,
    ) -> Result<Option<UserValue>, fjall::Error> {
        self.tx.get(keyspace, key)
    }

    pub(crate) fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        keyspace: &TxKeyspace,
        key: K,
        value: V,
    ) {
        let key = key.into();
        let value = value.into();
        if let Some(hasher) = &mut self.digest {
            record_write(hasher, 1, keyspace, &key, Some(&value));
        }
        self.tx.insert(keyspace, key, value);
    }

    pub(crate) fn remove<K: Into<UserKey>>(&mut self, keyspace: &TxKeyspace, key: K) {
        let key = key.into();
        if let Some(hasher) = &mut self.digest {
            record_write(hasher, 2, keyspace, &key, None);
        }
        self.tx.remove(keyspace, key);
    }

    pub(crate) fn commit(self) -> Result<(), fjall::Error> {
        self.tx.commit()
    }
}

// Every state machine keyspace as sorted KV maps, for whole-store equality
// assertions in replay and snapshot tests.
#[cfg(test)]
pub(crate) fn logical_contents(
    store: &Store,
) -> std::collections::BTreeMap<&'static str, std::collections::BTreeMap<Vec<u8>, Vec<u8>>> {
    let keyspaces: [(&'static str, &TxKeyspace); 11] = [
        ("jobs", &store.jobs),
        ("payloads", &store.payloads),
        ("inflight", &store.inflight),
        ("ready", &store.ready),
        ("dedup", &store.dedup),
        ("dedup_timers", &store.dedup_timers),
        ("scheduled", &store.scheduled),
        ("leases", &store.leases),
        ("dead_letter", &store.dead_letter),
        ("meta", &store.meta),
        ("audit", &store.audit),
    ];

    let snap = store.db.read_tx();
    let mut all = std::collections::BTreeMap::new();
    for (name, ks) in keyspaces {
        let mut kv = std::collections::BTreeMap::new();
        for guard in snap.iter(ks) {
            let (key, value) = guard.into_inner().expect("iterate keyspace");
            kv.insert(key.to_vec(), value.to_vec());
        }
        all.insert(name, kv);
    }
    all
}

fn record_write(
    hasher: &mut Sha256,
    tag: u8,
    keyspace: &TxKeyspace,
    key: &[u8],
    value: Option<&[u8]>,
) {
    let name = keyspace.inner().name();
    hasher.update([tag]);
    hasher.update((name.len() as u32).to_be_bytes());
    hasher.update(name.as_bytes());
    hasher.update((key.len() as u32).to_be_bytes());
    hasher.update(key);
    if let Some(value) = value {
        hasher.update((value.len() as u32).to_be_bytes());
        hasher.update(value);
    }
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
