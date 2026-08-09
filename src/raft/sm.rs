// The RaftStateMachine adapter: a thin async shim between the engine's sm
// worker and the committer thread, which keeps exclusive ownership of the
// Store and Indexes (ApplyCore::run_raft). Apply batches and installs cross
// one small channel; snapshot builds read the database directly through an
// MVCC snapshot and never involve the committer.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyerror::AnyError;
use fjall::{
    KeyspaceCreateOptions, Readable, SingleWriterTxDatabase as TxDatabase,
    SingleWriterTxKeyspace as TxKeyspace,
};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{OptionalSend, StorageIOError};
use prost::Message;
use tokio::sync::oneshot;

use super::snapshot::{
    SnapshotDir, build_snapshot_file, clear_install_marker, verify_snapshot_file,
    write_install_marker,
};
use super::{
    Entry, LogId, SnapshotFile, SnapshotMeta, StorageError, StoredMembership, TypeConfig,
    log_id_from_proto, stored_membership_from_proto,
};
use crate::keys::{LAST_APPLIED_KEY, MEMBERSHIP_KEY};
use crate::pb::sepp::raft::v1 as pb;
use crate::storage::{OpOutcome, SmCommand};

fn sm_err(e: impl std::error::Error + 'static) -> StorageError {
    StorageIOError::write_state_machine(AnyError::new(&e)).into()
}

fn sm_msg_err(msg: impl ToString) -> StorageError {
    StorageIOError::write_state_machine(AnyError::error(msg)).into()
}

pub struct StateMachine {
    sm_tx: flume::Sender<SmCommand>,
    db: TxDatabase,
    raft: TxKeyspace,
    dir: SnapshotDir,
    // Names incoming snapshot files; uniqueness matters only within this
    // process, stale partials are overwritten or ignored.
    incoming_seq: Arc<AtomicU64>,
}

impl StateMachine {
    // Dead-code allowance: constructed only by tests until PR 8 wires the
    // engine into boot.
    #[allow(dead_code)]
    pub(crate) fn new(
        db: TxDatabase,
        sm_tx: flume::Sender<SmCommand>,
        db_path: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let raft = db.keyspace("raft", KeyspaceCreateOptions::default)?;
        let dir = SnapshotDir::open(db_path)?;

        Ok(Self {
            sm_tx,
            db,
            raft,
            dir,
            incoming_seq: Arc::new(AtomicU64::new(0)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        // Fetched by name, not cached: a snapshot install swaps the meta
        // keyspace out from under any long-lived handle.
        let meta = self
            .db
            .keyspace("meta", KeyspaceCreateOptions::default)
            .map_err(sm_err)?;
        let snap = self.db.read_tx();

        let last_applied = snap
            .get(&meta, LAST_APPLIED_KEY)
            .map_err(sm_err)?
            .map(|v| pb::LogId::decode(v.as_ref()))
            .transpose()
            .map_err(sm_err)?
            .map(|msg| log_id_from_proto(&msg));
        let membership = snap
            .get(&meta, MEMBERSHIP_KEY)
            .map_err(sm_err)?
            .map(|v| pb::StoredMembership::decode(v.as_ref()))
            .transpose()
            .map_err(sm_err)?
            .map(stored_membership_from_proto)
            .unwrap_or_default();

        Ok((last_applied, membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<OpOutcome>, StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry> = entries.into_iter().collect();
        let (resp, rx) = oneshot::channel();
        self.sm_tx
            .send_async(SmCommand::Apply { entries, resp })
            .await
            .map_err(|_| sm_msg_err("the committer thread is gone"))?;

        // A dropped responder means the committer hit a fatal apply error;
        // the returned StorageError stops RaftCore with it.
        rx.await
            .map_err(|_| sm_msg_err("the committer thread died mid-apply"))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            db: self.db.clone(),
            dir: self.dir.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<SnapshotFile>, StorageError> {
        let seq = self.incoming_seq.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(SnapshotFile {
            path: self.dir.incoming_path(seq),
        }))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<SnapshotFile>,
    ) -> Result<(), StorageError> {
        // 1. Verify the file end to end and park it under its staged name.
        //    Not the final `.snap` name yet: `current()` must never
        //    advertise a snapshot the state machine does not durably
        //    contain, or a crash before the marker leaves a stale advert
        //    that wedges snapshot replication (the engine skips re-installs
        //    of an already-advertised id) and trips openraft's
        //    snapshot <= committed boot assert.
        let final_path = self.dir.file_path(&meta.snapshot_id);
        let staged_path = self.dir.staged_path(&meta.snapshot_id);
        let file_name = final_path
            .file_name()
            .expect("snapshot paths end in a file name")
            .to_string_lossy()
            .into_owned();
        {
            let source = snapshot.path.clone();
            let staged_path = staged_path.clone();
            let in_dir = self.dir.contains(&source);
            let expect_last = meta.last_log_id;
            tokio::task::spawn_blocking(move || -> Result<(), super::snapshot::SnapshotError> {
                let header = verify_snapshot_file(&source)?;
                if header.last_log_id != expect_last {
                    return Err(format!(
                        "snapshot file covers {:?}, the engine is installing {:?}",
                        header.last_log_id, expect_last,
                    )
                    .into());
                }
                if source != staged_path {
                    // The transport writes into this node's dir (rename); the
                    // in-process Suite hands over another node's file (copy).
                    if in_dir {
                        let _ = std::fs::remove_file(&staged_path);
                        std::fs::rename(&source, &staged_path)?;
                    } else {
                        std::fs::copy(&source, &staged_path)?;
                    }
                    // Write access: Windows refuses to flush a file opened
                    // read-only.
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(&staged_path)?
                        .sync_all()?;
                }
                Ok(())
            })
            .await
            .map_err(sm_err)?
            .map_err(|e| StorageIOError::write_state_machine(AnyError::error(e)))?;
        }

        // 2. The fence, durable before the destructive steps begin. From
        //    here the install completes in this process or at next boot;
        //    the marker names the final file and roll-forward tries both
        //    names.
        {
            let db = self.db.clone();
            let raft = self.raft.clone();
            let meta = meta.clone();
            tokio::task::spawn_blocking(move || {
                write_install_marker(&db, &raft, &meta, &file_name)
            })
            .await
            .map_err(sm_err)?
            .map_err(sm_err)?;
        }

        // 3. Delete + recreate + ingest + handle swap + index rebuild, on
        //    the thread that owns the Store.
        let (resp, rx) = oneshot::channel();
        self.sm_tx
            .send_async(SmCommand::Install {
                path: staged_path.clone(),
                resp,
            })
            .await
            .map_err(|_| sm_msg_err("the committer thread is gone"))?;
        rx.await
            .map_err(|_| sm_msg_err("the committer thread died mid-install"))?
            .map_err(|e| sm_msg_err(format!("snapshot install failed: {e}")))?;

        // 4. The state machine now durably contains the snapshot (ingest
        //    finishes fsync their tables); publish it as current and only
        //    then clear the fence.
        {
            let staged_path = staged_path.clone();
            let final_path = final_path.clone();
            tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                let _ = std::fs::remove_file(&final_path);
                std::fs::rename(&staged_path, &final_path)
            })
            .await
            .map_err(sm_err)?
            .map_err(sm_err)?;
        }
        {
            let db = self.db.clone();
            let raft = self.raft.clone();
            tokio::task::spawn_blocking(move || clear_install_marker(&db, &raft))
                .await
                .map_err(sm_err)?
                .map_err(sm_err)?;
        }

        self.dir.remove_others(&final_path);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError> {
        let current = self.dir.current().map_err(sm_err)?;
        Ok(current.map(|(meta, path)| Snapshot {
            meta,
            snapshot: Box::new(SnapshotFile { path }),
        }))
    }
}

pub struct SnapshotBuilder {
    db: TxDatabase,
    dir: SnapshotDir,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError> {
        let db = self.db.clone();
        let dir = self.dir.clone();
        let (meta, path) = tokio::task::spawn_blocking(move || build_snapshot_file(&db, &dir))
            .await
            .map_err(|e| {
                StorageError::from(StorageIOError::write_snapshot(None, AnyError::new(&e)))
            })?
            .map_err(|e| StorageIOError::write_snapshot(None, AnyError::error(e)))?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(SnapshotFile { path }),
        })
    }
}
