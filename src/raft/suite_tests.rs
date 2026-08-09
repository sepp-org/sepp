// openraft's storage compliance suite over the fjall log store and the real
// sepp state machine: log/vote round-trips, apply contracts, snapshot build
// and transfer at the SM level. Op-carrying entries and the crash paths are
// covered by the replay-harness extension and the snapshot tests; the suite
// cannot construct `C::D` values itself.

use std::sync::Arc;

use arc_swap::ArcSwap;
use fjall::{PersistMode, SingleWriterTxDatabase as TxDatabase};
use openraft::StorageIOError;
use openraft::testing::{StoreBuilder, Suite};
use uuid::Uuid;

use super::*;
use crate::metrics::Metrics;
use crate::storage::{
    AdminFold, AdminSnapshot, ApplyCore, Keyspaces, QueueNotifiers, StampClamp, StorageParams,
    Store, rebuild_indexes,
};

struct FjallStoreBuilder;

impl StoreBuilder<TypeConfig, RaftLogStore, StateMachine> for FjallStoreBuilder {
    async fn build(&self) -> Result<((), RaftLogStore, StateMachine), StorageError> {
        let err = |e: Box<dyn std::error::Error>| {
            StorageError::from(StorageIOError::write_state_machine(
                anyerror::AnyError::error(e),
            ))
        };
        let fjall_err = |e: fjall::Error| {
            StorageError::from(StorageIOError::write_state_machine(
                anyerror::AnyError::new(&e),
            ))
        };

        let path = std::env::temp_dir().join(format!("sepp-raft-suite-{}", Uuid::new_v4()));
        let db = TxDatabase::builder(&path)
            .temporary(true)
            .open()
            .map_err(fjall_err)?;

        let log_store = RaftLogStore::open(db.clone(), PersistMode::Buffer)?;

        // The suite's append helper blocks on the flush callback, so a pump
        // stands in for the IO loop that drives flush() in the real server.
        let pump = log_store.clone();
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

        let store = Store::new(
            db.clone(),
            Keyspaces::open(&db).map_err(fjall_err)?,
            StorageParams {
                persist_mode: PersistMode::Buffer,
                sweep_limit: 100,
                dead_letter_retention_ms: 0,
                admin_enabled: false,
            },
            Metrics::new(false),
        );
        let indexes = rebuild_indexes(&store).map_err(fjall_err)?;
        let core = ApplyCore::new(
            store,
            indexes,
            QueueNotifiers::default(),
            StampClamp::new(0),
        )
        .map_err(|e| err(e))?;

        let (sm_tx, sm_rx) = flume::bounded(4);
        let (_reads_tx, reads_rx) = flume::bounded(4);
        let admin = AdminFold::new(
            false,
            Arc::new(ArcSwap::from_pointee(AdminSnapshot::default())),
        );
        std::thread::Builder::new()
            .name("sepp-suite-committer".into())
            .spawn(move || core.run_raft(sm_rx, reads_rx, admin))
            .expect("spawn committer thread");

        let sm = StateMachine::new(db, sm_tx, path.to_str().expect("utf-8 temp path"), 16 << 20)
            .map_err(err)?;

        Ok(((), log_store, sm))
    }
}

#[test]
fn openraft_storage_suite() {
    Suite::<TypeConfig, RaftLogStore, StateMachine, FjallStoreBuilder, ()>::test_all(
        FjallStoreBuilder,
    )
    .expect("openraft storage suite");
}
