use std::error::Error;

use fjall::{KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase as TxDatabase};
use tracing::info;
use uuid::Uuid;

use crate::config::ClusterConfig;

// Op decode goes through usize so it might lower a 64 bit value to 32 bits on 32-bit targets.
#[cfg(not(target_pointer_width = "64"))]
compile_error!("sepp cluster mode requires a 64-bit target");

const NODE_ID_KEY: &[u8] = b"node_id";
const INSTANCE_UUID_KEY: &[u8] = b"instance_uuid";

pub struct NodeIdentity {
    pub node_id: u64,
    pub instance_uuid: Uuid,
}

// First cluster-enabled boot of a directory stamps its identity into the
// `raft` keyspace; every later boot verifies the config still matches the
// disk.
pub fn verify_or_stamp_identity(
    db: &TxDatabase,
    config: &ClusterConfig,
    db_path: &str,
) -> Result<NodeIdentity, Box<dyn Error>> {
    let raft = db.keyspace("raft", KeyspaceCreateOptions::default)?;

    let existing = {
        let rtx = db.read_tx();
        match (
            rtx.get(&raft, NODE_ID_KEY)?,
            rtx.get(&raft, INSTANCE_UUID_KEY)?,
        ) {
            (None, None) => None,
            (Some(node_id), Some(uuid)) => {
                Some(decode_identity(node_id.as_ref(), uuid.as_ref(), db_path)?)
            }
            _ => {
                return Err(format!(
                    "refusing to open database at {db_path:?}: its raft keyspace holds a \
                     partial node identity",
                )
                .into());
            }
        }
    };

    match existing {
        None => {
            let identity = NodeIdentity {
                node_id: config.node_id,
                instance_uuid: Uuid::new_v4(),
            };
            let mut tx = db.write_tx();
            tx.insert(
                &raft,
                NODE_ID_KEY.to_vec(),
                identity.node_id.to_be_bytes().to_vec(),
            );
            tx.insert(
                &raft,
                INSTANCE_UUID_KEY.to_vec(),
                identity.instance_uuid.as_bytes().to_vec(),
            );
            tx.commit()?;
            db.persist(PersistMode::SyncAll)?;
            info!(
                node_id = identity.node_id,
                instance_uuid = %identity.instance_uuid,
                "stamped cluster identity",
            );
            Ok(identity)
        }
        Some(identity) if identity.node_id != config.node_id => Err(format!(
            "refusing to open database at {db_path:?}: it is stamped cluster.node_id {}, \
             the config says {}",
            identity.node_id, config.node_id,
        )
        .into()),
        Some(identity) => Ok(identity),
    }
}

fn decode_identity(
    node_id: &[u8],
    uuid: &[u8],
    db_path: &str,
) -> Result<NodeIdentity, Box<dyn Error>> {
    let corrupt = |row: &str| {
        format!("refusing to open database at {db_path:?}: its raft keyspace {row} row is corrupt")
    };
    Ok(NodeIdentity {
        node_id: <[u8; 8]>::try_from(node_id)
            .map(u64::from_be_bytes)
            .map_err(|_| corrupt("node_id"))?,
        instance_uuid: <[u8; 16]>::try_from(uuid)
            .map(Uuid::from_bytes)
            .map_err(|_| corrupt("instance_uuid"))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> (std::path::PathBuf, TxDatabase) {
        let path = std::env::temp_dir().join(format!("sepp-cluster-test-{}", Uuid::new_v4()));
        let db = TxDatabase::builder(&path).open().expect("open db");
        (path, db)
    }

    #[test]
    fn first_boot_stamps_and_later_boots_verify() {
        let (path, db) = temp_db();
        let config = ClusterConfig {
            node_id: 3,
            ..Default::default()
        };

        let stamped = verify_or_stamp_identity(&db, &config, "test").expect("first boot stamps");
        assert_eq!(stamped.node_id, 3);

        let verified =
            verify_or_stamp_identity(&db, &config, "test").expect("second boot verifies");
        assert_eq!(verified.node_id, 3);
        assert_eq!(
            verified.instance_uuid, stamped.instance_uuid,
            "the uuid is minted once, not per boot"
        );

        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn node_id_mismatch_refuses() {
        let (path, db) = temp_db();
        let stamp = ClusterConfig {
            node_id: 1,
            ..Default::default()
        };
        verify_or_stamp_identity(&db, &stamp, "test").expect("stamp");

        let changed = ClusterConfig {
            node_id: 2,
            ..Default::default()
        };
        let err = verify_or_stamp_identity(&db, &changed, "test")
            .err()
            .expect("mismatched node_id must refuse");
        assert!(
            err.to_string().contains("node_id"),
            "unexpected error: {err}"
        );

        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn partial_identity_refuses() {
        let (path, db) = temp_db();
        let raft = db
            .keyspace("raft", KeyspaceCreateOptions::default)
            .expect("create raft keyspace");
        let mut tx = db.write_tx();
        tx.insert(&raft, NODE_ID_KEY.to_vec(), 1u64.to_be_bytes().to_vec());
        tx.commit().expect("commit partial identity");

        let err = verify_or_stamp_identity(&db, &ClusterConfig::default(), "test")
            .err()
            .expect("partial identity must refuse");
        assert!(
            err.to_string().contains("partial"),
            "unexpected error: {err}"
        );

        let _ = std::fs::remove_dir_all(&path);
    }
}
