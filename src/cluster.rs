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

#[cfg(test)]
mod raft_proto_tests {
    use prost::Message;

    use crate::pb::sepp::raft::v1 as pb;
    use crate::pb::sepp::storage::v1 as op_pb;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn log_id(term: u64, node_id: u64, index: u64) -> pb::LogId {
        pb::LogId {
            term,
            node_id,
            index,
        }
    }

    fn leader_vote() -> pb::Vote {
        pb::Vote {
            term: 3,
            node_id: 2,
            committed: true,
        }
    }

    fn node(node_id: u64) -> pb::Node {
        pb::Node {
            node_id,
            peer_addr: format!("sepp-{node_id}.internal:50052"),
            client_addr: format!("sepp-{node_id}.example.com:50051"),
        }
    }

    // A joint-consensus membership mid-transition, learners included, so the
    // golden covers both configs entries and the nodes ordering.
    fn membership() -> pb::Membership {
        pb::Membership {
            configs: vec![
                pb::NodeIdSet {
                    node_ids: vec![1, 2, 3],
                },
                pb::NodeIdSet {
                    node_ids: vec![2, 3, 4],
                },
            ],
            nodes: (1..=4).map(node).collect(),
        }
    }

    fn stored_membership() -> pb::StoredMembership {
        pb::StoredMembership {
            log_id: Some(log_id(3, 2, 9)),
            membership: Some(membership()),
        }
    }

    fn sample_messages() -> Vec<(&'static str, Vec<u8>)> {
        let ack_op = op_pb::Op {
            op: Some(op_pb::op::Op::Ack(op_pb::AckOp {
                job_id: "job-1".into(),
                attempt: 2,
            })),
        };
        vec![
            ("vote", leader_vote().encode_to_vec()),
            (
                "entry_blank",
                pb::Entry {
                    log_id: Some(log_id(3, 2, 8)),
                    payload: Some(pb::entry::Payload::Blank(pb::Blank {})),
                }
                .encode_to_vec(),
            ),
            (
                "entry_membership",
                pb::Entry {
                    log_id: Some(log_id(3, 2, 9)),
                    payload: Some(pb::entry::Payload::Membership(membership())),
                }
                .encode_to_vec(),
            ),
            (
                "entry_op",
                pb::Entry {
                    log_id: Some(log_id(3, 2, 10)),
                    payload: Some(pb::entry::Payload::Op(ack_op)),
                }
                .encode_to_vec(),
            ),
            ("stored_membership", stored_membership().encode_to_vec()),
            (
                "append_entries_request",
                pb::AppendEntriesRequest {
                    vote: Some(leader_vote()),
                    prev_log_id: Some(log_id(3, 2, 7)),
                    entries: vec![pb::Entry {
                        log_id: Some(log_id(3, 2, 8)),
                        payload: Some(pb::entry::Payload::Blank(pb::Blank {})),
                    }],
                    leader_commit: Some(log_id(3, 2, 6)),
                    op_format_version: 1,
                }
                .encode_to_vec(),
            ),
            (
                "append_entries_response_partial",
                pb::AppendEntriesResponse {
                    result: Some(pb::append_entries_response::Result::PartialSuccess(
                        pb::PartialSuccess {
                            matched: Some(log_id(3, 2, 8)),
                        },
                    )),
                }
                .encode_to_vec(),
            ),
            (
                "append_entries_response_higher_vote",
                pb::AppendEntriesResponse {
                    result: Some(pb::append_entries_response::Result::HigherVote(pb::Vote {
                        term: 4,
                        node_id: 3,
                        committed: false,
                    })),
                }
                .encode_to_vec(),
            ),
            (
                "vote_request",
                pb::VoteRequest {
                    vote: Some(pb::Vote {
                        term: 4,
                        node_id: 3,
                        committed: false,
                    }),
                    last_log_id: Some(log_id(3, 2, 10)),
                }
                .encode_to_vec(),
            ),
            (
                "vote_response",
                pb::VoteResponse {
                    vote: Some(pb::Vote {
                        term: 4,
                        node_id: 3,
                        committed: false,
                    }),
                    vote_granted: true,
                    last_log_id: Some(log_id(3, 2, 10)),
                }
                .encode_to_vec(),
            ),
            (
                "snapshot_meta",
                pb::SnapshotMeta {
                    last_log_id: Some(log_id(3, 2, 9)),
                    last_membership: Some(stored_membership()),
                    snapshot_id: "3-2-9-1".into(),
                }
                .encode_to_vec(),
            ),
        ]
    }

    // Entry, Vote and the membership messages are durable replicated state;
    // the envelopes are the peer wire format. A byte change here breaks every
    // persisted log and mixed-version cluster.
    #[test]
    fn golden_raft_encoding_is_pinned() {
        const GOLDEN: &[(&str, &str)] = &[
            ("vote", "080310021801"),
            ("entry_blank", "0a060803100218082200"),
            (
                "entry_membership",
                "0a060803100218091ae2010a050a030102030a050a03020304123308011215736570702d312e696e7465726e616c3a35303035321a18736570702d312e6578616d706c652e636f6d3a3530303531123308021215736570702d322e696e7465726e616c3a35303035321a18736570702d322e6578616d706c652e636f6d3a3530303531123308031215736570702d332e696e7465726e616c3a35303035321a18736570702d332e6578616d706c652e636f6d3a3530303531123308041215736570702d342e696e7465726e616c3a35303035321a18736570702d342e6578616d706c652e636f6d3a3530303531",
            ),
            ("entry_op", "0a0608031002180a120b22090a056a6f622d311002"),
            (
                "stored_membership",
                "0a0608031002180912e2010a050a030102030a050a03020304123308011215736570702d312e696e7465726e616c3a35303035321a18736570702d312e6578616d706c652e636f6d3a3530303531123308021215736570702d322e696e7465726e616c3a35303035321a18736570702d322e6578616d706c652e636f6d3a3530303531123308031215736570702d332e696e7465726e616c3a35303035321a18736570702d332e6578616d706c652e636f6d3a3530303531123308041215736570702d342e696e7465726e616c3a35303035321a18736570702d342e6578616d706c652e636f6d3a3530303531",
            ),
            (
                "append_entries_request",
                "0a0608031002180112060803100218071a0a0a06080310021808220022060803100218062801",
            ),
            ("append_entries_response_partial", "12080a06080310021808"),
            ("append_entries_response_higher_vote", "220408041003"),
            ("vote_request", "0a0408041003120608031002180a"),
            ("vote_response", "0a040804100310011a0608031002180a"),
            (
                "snapshot_meta",
                "0a0608031002180912ed010a0608031002180912e2010a050a030102030a050a03020304123308011215736570702d312e696e7465726e616c3a35303035321a18736570702d312e6578616d706c652e636f6d3a3530303531123308021215736570702d322e696e7465726e616c3a35303035321a18736570702d322e6578616d706c652e636f6d3a3530303531123308031215736570702d332e696e7465726e616c3a35303035321a18736570702d332e6578616d706c652e636f6d3a3530303531123308041215736570702d342e696e7465726e616c3a35303035321a18736570702d342e6578616d706c652e636f6d3a35303035311a07332d322d392d31",
            ),
        ];

        let samples = sample_messages();
        assert_eq!(
            samples.len(),
            GOLDEN.len(),
            "one golden entry per sample message"
        );
        for ((name, bytes), (golden_name, golden)) in samples.iter().zip(GOLDEN) {
            assert_eq!(name, golden_name, "sample order changed");
            assert_eq!(&hex(bytes), golden, "encoding changed for {name}");
        }
    }

    // The op inside entry_op must encode exactly as op.rs's own golden for
    // the Ack variant: an Entry wraps the op bytes untouched.
    #[test]
    fn entry_embeds_op_bytes_unchanged() {
        let ack_golden = "22090a056a6f622d311002";
        let entry_op = &sample_messages()[3];
        assert_eq!(entry_op.0, "entry_op");
        assert!(
            hex(&entry_op.1).ends_with(ack_golden),
            "Entry payload no longer carries the pinned Op encoding"
        );
    }

    #[test]
    fn raft_messages_round_trip() {
        fn assert_reencodes<M: Message + Default + PartialEq + std::fmt::Debug>(bytes: &[u8]) {
            let decoded = M::decode(bytes).expect("decodes");
            assert_eq!(decoded.encode_to_vec(), bytes, "{decoded:?}");
        }

        for (name, bytes) in sample_messages() {
            match name {
                "vote" => assert_reencodes::<pb::Vote>(&bytes),
                "entry_blank" | "entry_membership" | "entry_op" => {
                    assert_reencodes::<pb::Entry>(&bytes)
                }
                "stored_membership" => assert_reencodes::<pb::StoredMembership>(&bytes),
                "append_entries_request" => assert_reencodes::<pb::AppendEntriesRequest>(&bytes),
                "append_entries_response_partial" | "append_entries_response_higher_vote" => {
                    assert_reencodes::<pb::AppendEntriesResponse>(&bytes)
                }
                "vote_request" => assert_reencodes::<pb::VoteRequest>(&bytes),
                "vote_response" => assert_reencodes::<pb::VoteResponse>(&bytes),
                "snapshot_meta" => assert_reencodes::<pb::SnapshotMeta>(&bytes),
                other => panic!("sample {other} has no round-trip arm"),
            }
        }
    }
}
