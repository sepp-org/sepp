use std::io::Cursor;
use std::sync::{Arc, Mutex};

use fjall::{PersistMode, SingleWriterTxDatabase as TxDatabase};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::testing::{StoreBuilder, Suite};
use openraft::{EntryPayload, OptionalSend, SnapshotMeta, StorageIOError};
use prost::Message;
use uuid::Uuid;

use super::*;
use crate::pb::sepp::raft::v1 as pb;
use crate::storage::OpOutcome;

type Meta = SnapshotMeta<NodeId, ClusterNode>;

#[derive(Debug, Clone, Default)]
struct MemStateMachine {
    inner: Arc<Mutex<MemInner>>,
}

#[derive(Debug, Default)]
struct MemInner {
    last_applied: Option<LogId>,
    membership: StoredMembership,
    snapshot: Option<(Meta, Vec<u8>)>,
    snapshot_idx: u64,
}

impl RaftStateMachine<TypeConfig> for MemStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        let inner = self.inner.lock().unwrap();
        Ok((inner.last_applied, inner.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<OpOutcome>, StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.inner.lock().unwrap();
        let mut replies = Vec::new();
        for entry in entries {
            inner.last_applied = Some(entry.log_id);
            if let EntryPayload::Membership(m) = &entry.payload {
                inner.membership = StoredMembership::new(Some(entry.log_id), m.clone());
            }
            replies.push(OpOutcome::CloseQueue);
        }
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &Meta,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap();
        inner.last_applied = meta.last_log_id;
        inner.membership = meta.last_membership.clone();
        inner.snapshot = Some((meta.clone(), snapshot.into_inner()));
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.snapshot.clone().map(|(meta, data)| Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for MemStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError> {
        let mut inner = self.inner.lock().unwrap();
        inner.snapshot_idx += 1;
        let meta = Meta {
            last_log_id: inner.last_applied,
            last_membership: inner.membership.clone(),
            snapshot_id: format!("ss-{}", inner.snapshot_idx),
        };
        let data = pb::SnapshotMeta {
            last_log_id: meta.last_log_id.as_ref().map(log_id_to_proto),
            last_membership: Some(pb::StoredMembership {
                log_id: meta.last_membership.log_id().as_ref().map(log_id_to_proto),
                membership: Some(membership_to_proto(meta.last_membership.membership())),
            }),
            snapshot_id: meta.snapshot_id.clone(),
        }
        .encode_to_vec();
        inner.snapshot = Some((meta.clone(), data.clone()));
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

struct FjallStoreBuilder;

impl StoreBuilder<TypeConfig, RaftLogStore, MemStateMachine> for FjallStoreBuilder {
    async fn build(&self) -> Result<((), RaftLogStore, MemStateMachine), StorageError> {
        let path = std::env::temp_dir().join(format!("sepp-raft-suite-{}", Uuid::new_v4()));
        let db = TxDatabase::builder(path)
            .temporary(true)
            .open()
            .map_err(|e| StorageIOError::read_logs(anyerror::AnyError::new(&e)))?;
        let store = RaftLogStore::open(db, PersistMode::Buffer)?;

        // The suite's append helper blocks on the flush callback, so a pump
        // stands in for the IO loop that drives flush() in the real server.
        let pump = store.clone();
        tokio::spawn(async move {
            loop {
                pump.flush_wanted().await;
                let store = pump.clone();
                if tokio::task::spawn_blocking(move || store.flush())
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(((), store, MemStateMachine::default()))
    }
}

#[test]
fn openraft_storage_suite() {
    Suite::<TypeConfig, RaftLogStore, MemStateMachine, FjallStoreBuilder, ()>::test_all(
        FjallStoreBuilder,
    )
    .expect("openraft storage suite");
}
