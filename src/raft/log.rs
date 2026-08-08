// openraft's StorageError is what the storage traits hand back; its size is
// the engine's choice, not ours.
#![allow(clippy::result_large_err)]

use std::ops::Bound;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use anyerror::AnyError;
use fjall::{
    AbstractTree, KeyspaceCreateOptions, PersistMode, Readable,
    SingleWriterTxDatabase as TxDatabase, SingleWriterTxKeyspace as TxKeyspace,
};
use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{OptionalSend, RaftLogReader, StorageIOError};
use prost::Message;
use tokio::sync::Notify;

use super::{
    Entry, LogId, StorageError, TypeConfig, Vote, entry_from_proto, entry_to_proto,
    log_id_from_proto, log_id_to_proto, vote_from_proto, vote_to_proto,
};
use crate::pb::sepp::raft::v1 as pb;

const VOTE_KEY: &[u8] = b"vote";
const PURGE_FLOOR_KEY: &[u8] = b"purge_floor";

// drop_range takes the tree's major-compaction lock and runs synchronously,
// so physical reclamation advances in large steps behind the logical floor.
const DROP_RANGE_STEP: u64 = 1_000_000;

fn log_key(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

// The raft log and vote, sharing the state machine's fjall database so one
// persist can cover a log-append batch and an apply batch.
#[derive(Clone)]
pub struct RaftLogStore {
    db: TxDatabase,
    log: TxKeyspace,
    raft: TxKeyspace,
    persist_mode: PersistMode,
    pending: Arc<Mutex<Vec<LogFlushed<TypeConfig>>>>,
    flush_wanted: Arc<Notify>,
    // Index up to which drop_range has already reclaimed; lags the logical
    // floor by up to drop_step.
    dropped: Arc<AtomicU64>,
    drop_step: u64,
}

impl RaftLogStore {
    pub fn open(db: TxDatabase, persist_mode: PersistMode) -> Result<Self, StorageError> {
        let log = db
            .keyspace("raft_log", KeyspaceCreateOptions::default)
            .map_err(read_err)?;
        let raft = db
            .keyspace("raft", KeyspaceCreateOptions::default)
            .map_err(read_err)?;
        let store = Self {
            db,
            log,
            raft,
            persist_mode,
            pending: Arc::new(Mutex::new(Vec::new())),
            flush_wanted: Arc::new(Notify::new()),
            dropped: Arc::new(AtomicU64::new(0)),
            drop_step: DROP_RANGE_STEP,
        };

        // `dropped` is a reclamation hint, not correctness state, so it is
        // not persisted. Seeding it at the recovered floor skips a redundant
        // drop_range at boot.
        if let Some(floor) = store.purge_floor()? {
            store.dropped.store(floor.index, Ordering::Relaxed);
        }

        Ok(store)
    }

    pub fn flush(&self) -> Result<(), fjall::Error> {
        let pending = std::mem::take(&mut *self.pending.lock().unwrap());

        let result = self.db.persist(self.persist_mode);
        match &result {
            Ok(()) => {
                for callback in pending {
                    callback.log_io_completed(Ok(()));
                }
            }
            Err(e) => {
                let msg = e.to_string();
                for callback in pending {
                    callback.log_io_completed(Err(std::io::Error::other(msg.clone())));
                }
            }
        }

        result
    }

    // Resolves when at least one append is waiting on a flush.
    pub async fn flush_wanted(&self) {
        self.flush_wanted.notified().await;
    }

    fn purge_floor(&self) -> Result<Option<LogId>, StorageError> {
        let Some(bytes) = self
            .db
            .read_tx()
            .get(&self.raft, PURGE_FLOOR_KEY)
            .map_err(read_err)?
        else {
            return Ok(None);
        };

        let msg = pb::LogId::decode(bytes.as_ref()).map_err(read_err)?;
        Ok(Some(log_id_from_proto(&msg)))
    }
}

fn write_err(e: impl std::error::Error + 'static) -> StorageError {
    StorageIOError::write_logs(AnyError::new(&e)).into()
}

fn read_err(e: impl std::error::Error + 'static) -> StorageError {
    StorageIOError::read_logs(AnyError::new(&e)).into()
}

impl RaftLogReader<TypeConfig> for RaftLogStore {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError> {
        let mut start = match range.start_bound() {
            Bound::Included(x) => *x,
            Bound::Excluded(x) => x.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(x) => x.saturating_add(1),
            Bound::Excluded(x) => *x,
            Bound::Unbounded => u64::MAX,
        };

        // The logical floor governs visibility; entries physically lingering
        // below it are reclamation lag, not data.
        if let Some(floor) = self.purge_floor()? {
            start = start.max(floor.index.saturating_add(1));
        }
        if start >= end {
            return Ok(Vec::new());
        }

        let snap = self.db.read_tx();
        let mut entries = Vec::new();
        for guard in snap.range(&self.log, log_key(start)..log_key(end)) {
            let (_, value) = guard.into_inner().map_err(read_err)?;
            let msg = pb::Entry::decode(value.as_ref()).map_err(read_err)?;
            entries.push(entry_from_proto(msg).map_err(read_err)?);
        }

        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for RaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError> {
        let floor = self.purge_floor()?;
        let snap = self.db.read_tx();
        let last_log_id = match snap.iter(&self.log).next_back() {
            Some(guard) => {
                let (key, value) = guard.into_inner().map_err(read_err)?;
                let index = key
                    .as_ref()
                    .first_chunk::<8>()
                    .map(|b| u64::from_be_bytes(*b))
                    .unwrap_or(0);

                if floor.is_some_and(|f| index <= f.index) {
                    floor
                } else {
                    let msg = pb::Entry::decode(value.as_ref()).map_err(read_err)?;
                    Some(entry_from_proto(msg).map_err(read_err)?.log_id)
                }
            }
            None => floor,
        };

        Ok(LogState {
            last_purged_log_id: floor,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), StorageError> {
        let bytes = vote_to_proto(vote).encode_to_vec();
        let this = self.clone();

        // Vote writes are rare and gate election safety: own tx, immediate
        // full fsync, never the batched flush path.
        tokio::task::spawn_blocking(move || -> Result<(), fjall::Error> {
            let mut tx = this.db.write_tx();
            tx.insert(&this.raft, VOTE_KEY.to_vec(), bytes);
            tx.commit()?;
            this.db.persist(PersistMode::SyncAll)
        })
        .await
        .map_err(|e| StorageError::from(StorageIOError::write_vote(AnyError::new(&e))))?
        .map_err(|e| StorageIOError::write_vote(AnyError::new(&e)).into())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError> {
        let Some(bytes) = self
            .db
            .read_tx()
            .get(&self.raft, VOTE_KEY)
            .map_err(|e| StorageError::from(StorageIOError::read_vote(AnyError::new(&e))))?
        else {
            return Ok(None);
        };

        let msg = pb::Vote::decode(bytes.as_ref())
            .map_err(|e| StorageError::from(StorageIOError::read_vote(AnyError::new(&e))))?;
        Ok(Some(vote_from_proto(&msg)))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // Serialize before taking the tx: fjall's single-writer lock is held
        // from write_tx() to commit and blocks every other committer.
        let rows: Vec<([u8; 8], Vec<u8>)> = entries
            .into_iter()
            .map(|e| (log_key(e.log_id.index), entry_to_proto(&e).encode_to_vec()))
            .collect();

        let this = self.clone();
        tokio::task::spawn_blocking(move || -> Result<(), fjall::Error> {
            let mut tx = this.db.write_tx();
            for (key, value) in rows {
                tx.insert(&this.log, key.to_vec(), value);
            }

            // Memtable commit only: entries must be readable when append
            // returns. Durability and the callback ride the next flush().
            tx.commit()
        })
        .await
        .map_err(write_err)?
        .map_err(write_err)?;

        // Parked only after the commit: flush() snapshots this queue before
        // persisting, so a callback in the queue always refers to entries the
        // upcoming persist covers.
        self.pending.lock().unwrap().push(callback);
        self.flush_wanted.notify_one();

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || -> Result<(), fjall::Error> {
            let keys: Vec<_> = this
                .db
                .read_tx()
                .range(&this.log, log_key(log_id.index)..)
                .map(|guard| guard.into_inner().map(|(k, _)| k))
                .collect::<Result<_, _>>()?;

            let mut tx = this.db.write_tx();
            for key in keys {
                tx.remove(&this.log, key);
            }

            // An unpersisted truncate lost to a crash is safe: the stale
            // suffix conflicts again and the leader re-truncates, and the
            // journal's strict-prefix recovery means no later persisted
            // append can survive while this truncate is lost.
            tx.commit()
        })
        .await
        .map_err(write_err)?
        .map_err(write_err)
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.purge_blocking(log_id))
            .await
            .map_err(write_err)?
    }
}

impl RaftLogStore {
    fn purge_blocking(&self, floor: LogId) -> Result<(), StorageError> {
        let current = self.purge_floor()?;
        if current.is_some_and(|c| c.index >= floor.index) {
            return Ok(());
        }

        let mut tx = self.db.write_tx();
        tx.insert(
            &self.raft,
            PURGE_FLOOR_KEY.to_vec(),
            log_id_to_proto(&floor).encode_to_vec(),
        );
        tx.commit().map_err(write_err)?;

        // Logical truncation is the floor record above; drop_range is
        // reclamation only and advances in big steps off the hot path.
        let dropped = self.dropped.load(Ordering::Relaxed);
        if floor.index.saturating_sub(dropped) >= self.drop_step {
            // The floor must be durable before reclamation: recovery seeing
            // reclaimed indexes above a stale floor would be a hole in the
            // log.
            self.db.persist(PersistMode::SyncAll).map_err(write_err)?;
            self.log
                .inner()
                .tree
                .drop_range(..=log_key(floor.index))
                .map_err(write_err)?;
            self.dropped.store(floor.index, Ordering::Relaxed);
        }

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn with_drop_step(mut self, step: u64) -> Self {
        self.drop_step = step;
        self
    }

    // drop_range only drops whole on-disk tables; rotating seals the active
    // memtable into one so tests can observe reclamation.
    #[cfg(test)]
    pub(crate) fn rotate_log_memtable(&self) {
        self.log
            .inner()
            .rotate_memtable_and_wait()
            .expect("rotate memtable");
    }

    #[cfg(test)]
    pub(crate) fn raw_log_key_count(&self) -> usize {
        self.db
            .read_tx()
            .iter(&self.log)
            .filter_map(|g| g.into_inner().ok())
            .count()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use openraft::storage::RaftLogStorageExt;
    use openraft::{EntryPayload, LeaderId};
    use uuid::Uuid;

    use super::*;
    use crate::op::Op;

    fn temp_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("sepp-raft-log-{}", Uuid::new_v4()))
    }

    fn open_temp() -> RaftLogStore {
        let db = TxDatabase::builder(temp_path())
            .temporary(true)
            .open()
            .expect("open db");
        RaftLogStore::open(db, PersistMode::Buffer).expect("open store")
    }

    fn log_id(term: u64, index: u64) -> LogId {
        LogId {
            leader_id: LeaderId::new(term, 1),
            index,
        }
    }

    fn ent(term: u64, index: u64) -> Entry {
        Entry {
            log_id: log_id(term, index),
            payload: EntryPayload::Blank,
        }
    }

    // Appends and drives the deferred flush so the parked callback resolves.
    async fn append_flushed(store: &RaftLogStore, entries: Vec<Entry>) {
        let mut appender = store.clone();
        let handle = tokio::spawn(async move { appender.blocking_append(entries).await });
        store.flush_wanted().await;
        store.flush().expect("flush");
        handle.await.expect("join").expect("append");
    }

    #[tokio::test]
    async fn append_is_readable_before_flush_and_acked_after() {
        let mut store = open_temp();

        let mut appender = store.clone();
        let handle =
            tokio::spawn(async move { appender.blocking_append(vec![ent(1, 1), ent(1, 2)]).await });

        // Readable on return is the append contract; the durability callback
        // must still be parked until a flush persists.
        store.flush_wanted().await;
        let entries = store.try_get_log_entries(1..=2).await.expect("read");
        assert_eq!(entries.len(), 2);
        assert!(!handle.is_finished(), "callback must wait for the flush");

        store.flush().expect("flush");
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("callback fires after flush")
            .expect("join")
            .expect("append");
    }

    #[tokio::test]
    async fn entries_round_trip_through_the_store() {
        let mut store = open_temp();
        let op_entry = Entry {
            log_id: log_id(2, 3),
            payload: EntryPayload::Normal(Op::Ack {
                job_id: "job-1".into(),
                attempt: 4,
            }),
        };
        append_flushed(&store, vec![ent(2, 1), ent(2, 2), op_entry.clone()]).await;

        let read = store.try_get_log_entries(1..).await.expect("read");
        assert_eq!(read.len(), 3);
        assert_eq!(read[2], op_entry);

        let state = store.get_log_state().await.expect("state");
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(state.last_log_id, Some(log_id(2, 3)));
    }

    #[tokio::test]
    async fn vote_round_trips_and_survives_reopen() {
        let dir = temp_path();
        {
            let db = TxDatabase::builder(&dir).open().expect("open db");
            let mut store = RaftLogStore::open(db, PersistMode::SyncData).expect("open store");
            assert_eq!(store.read_vote().await.expect("read"), None);
            let vote = Vote::new(3, 2);
            store.save_vote(&vote).await.expect("save");
            assert_eq!(store.read_vote().await.expect("read"), Some(vote));
        }

        let db = TxDatabase::builder(&dir).open().expect("reopen db");
        let mut store = RaftLogStore::open(db, PersistMode::SyncData).expect("reopen store");
        assert_eq!(
            store.read_vote().await.expect("read"),
            Some(Vote::new(3, 2))
        );
        drop(store);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn truncate_removes_the_suffix() {
        let mut store = open_temp();
        append_flushed(&store, (1..=10).map(|i| ent(1, i)).collect()).await;

        store.truncate(log_id(1, 5)).await.expect("truncate");

        let read = store.try_get_log_entries(0..).await.expect("read");
        assert_eq!(read.len(), 4, "indexes 1..=4 remain");
        let state = store.get_log_state().await.expect("state");
        assert_eq!(state.last_log_id, Some(log_id(1, 4)));
    }

    #[tokio::test]
    async fn purge_clamps_reads_to_the_logical_floor() {
        let mut store = open_temp();
        append_flushed(&store, (1..=10).map(|i| ent(1, i)).collect()).await;

        store.purge(log_id(1, 3)).await.expect("purge");
        let read = store.try_get_log_entries(0..).await.expect("read");
        assert_eq!(read.first().map(|e| e.log_id.index), Some(4));
        let state = store.get_log_state().await.expect("state");
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 3)));
        assert_eq!(state.last_log_id, Some(log_id(1, 10)));

        // Purging past the last entry leaves an empty logical log whose ids
        // continue from the floor.
        store.purge(log_id(1, 20)).await.expect("purge all");
        assert!(
            store
                .try_get_log_entries(0..)
                .await
                .expect("read")
                .is_empty()
        );
        let state = store.get_log_state().await.expect("state");
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 20)));
        assert_eq!(state.last_log_id, Some(log_id(1, 20)));
        assert_eq!(
            store.raw_log_key_count(),
            10,
            "physical entries linger below the drop step"
        );
    }

    #[tokio::test]
    async fn drop_range_lags_the_logical_floor() {
        let mut store = open_temp().with_drop_step(5);
        // Two sealed tables: 1..=5 and 6..=10. drop_range drops only tables
        // fully inside the range, so the boundary table survives the floor.
        append_flushed(&store, (1..=5).map(|i| ent(1, i)).collect()).await;
        store.rotate_log_memtable();
        append_flushed(&store, (6..=10).map(|i| ent(1, i)).collect()).await;
        store.rotate_log_memtable();

        store.purge(log_id(1, 3)).await.expect("purge");
        assert_eq!(
            store.raw_log_key_count(),
            10,
            "below the step: no reclamation"
        );
        assert_eq!(
            store.try_get_log_entries(0..).await.expect("read").len(),
            7,
            "the floor clamps reads regardless"
        );

        store.purge(log_id(1, 8)).await.expect("purge");
        assert_eq!(
            store.raw_log_key_count(),
            5,
            "step crossed: the fully covered table is reclaimed, the boundary table lags"
        );
        assert_eq!(
            store.try_get_log_entries(0..).await.expect("read").len(),
            2,
            "lingering boundary entries stay invisible"
        );

        store.purge(log_id(1, 10)).await.expect("purge");
        assert_eq!(store.raw_log_key_count(), 5, "next step not yet reached");
    }

    #[tokio::test]
    async fn reopen_recovers_log_state_and_floor() {
        let dir = temp_path();
        {
            let db = TxDatabase::builder(&dir).open().expect("open db");
            let store = RaftLogStore::open(db, PersistMode::SyncData).expect("open store");
            append_flushed(&store, (1..=6).map(|i| ent(1, i)).collect()).await;
            let mut s = store.clone();
            s.purge(log_id(1, 2)).await.expect("purge");
            store.flush().expect("flush the floor");
        }

        let db = TxDatabase::builder(&dir).open().expect("reopen db");
        let mut store = RaftLogStore::open(db, PersistMode::SyncData).expect("reopen store");
        let state = store.get_log_state().await.expect("state");
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 2)));
        assert_eq!(state.last_log_id, Some(log_id(1, 6)));
        let read = store.try_get_log_entries(0..).await.expect("read");
        assert_eq!(read.first().map(|e| e.log_id.index), Some(3));
        assert_eq!(read.len(), 4);
        drop(store);
        std::fs::remove_dir_all(&dir).ok();
    }
}
