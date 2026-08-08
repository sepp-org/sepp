use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;

use tonic::Status;

use crate::op::Op;
use crate::pb::sepp::raft::v1 as pb;
use crate::storage::OpOutcome;

mod log;
#[cfg(test)]
mod suite_tests;

pub use log::RaftLogStore;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterNode {
    pub peer_addr: String,
    pub client_addr: String,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Op,
        R = OpOutcome,
        Node = ClusterNode,
);

pub type NodeId = u64;
pub type Entry = openraft::Entry<TypeConfig>;
pub type LogId = openraft::LogId<NodeId>;
pub type Vote = openraft::Vote<NodeId>;
pub type Membership = openraft::Membership<NodeId, ClusterNode>;
pub type StoredMembership = openraft::StoredMembership<NodeId, ClusterNode>;
pub type StorageError = openraft::StorageError<NodeId>;

use openraft::{EntryPayload, LeaderId};

fn corrupt(what: &str) -> Status {
    Status::internal(format!("corrupt raft record: {what}"))
}

pub fn log_id_to_proto(id: &LogId) -> pb::LogId {
    pb::LogId {
        term: id.leader_id.term,
        node_id: id.leader_id.node_id,
        index: id.index,
    }
}

pub fn log_id_from_proto(msg: &pb::LogId) -> LogId {
    LogId {
        leader_id: LeaderId::new(msg.term, msg.node_id),
        index: msg.index,
    }
}

pub fn vote_to_proto(vote: &Vote) -> pb::Vote {
    pb::Vote {
        term: vote.leader_id.term,
        node_id: vote.leader_id.node_id,
        committed: vote.committed,
    }
}

pub fn vote_from_proto(msg: &pb::Vote) -> Vote {
    Vote {
        leader_id: LeaderId::new(msg.term, msg.node_id),
        committed: msg.committed,
    }
}

pub fn membership_to_proto(m: &Membership) -> pb::Membership {
    pb::Membership {
        // BTreeSet and BTreeMap iteration give the sorted order the proto
        // comments promise, so encoding is deterministic.
        configs: m
            .get_joint_config()
            .iter()
            .map(|set| pb::NodeIdSet {
                node_ids: set.iter().copied().collect(),
            })
            .collect(),
        nodes: m
            .nodes()
            .map(|(id, node)| pb::Node {
                node_id: *id,
                peer_addr: node.peer_addr.clone(),
                client_addr: node.client_addr.clone(),
            })
            .collect(),
    }
}

pub fn membership_from_proto(msg: pb::Membership) -> Membership {
    let configs: Vec<BTreeSet<NodeId>> = msg
        .configs
        .into_iter()
        .map(|set| set.node_ids.into_iter().collect())
        .collect();
    let nodes: BTreeMap<NodeId, ClusterNode> = msg
        .nodes
        .into_iter()
        .map(|n| {
            (
                n.node_id,
                ClusterNode {
                    peer_addr: n.peer_addr,
                    client_addr: n.client_addr,
                },
            )
        })
        .collect();
    Membership::new(configs, nodes)
}

pub fn entry_to_proto(entry: &Entry) -> pb::Entry {
    let payload = match &entry.payload {
        EntryPayload::Blank => pb::entry::Payload::Blank(pb::Blank {}),
        EntryPayload::Normal(op) => pb::entry::Payload::Op(op.to_proto()),
        EntryPayload::Membership(m) => pb::entry::Payload::Membership(membership_to_proto(m)),
    };
    pb::Entry {
        log_id: Some(log_id_to_proto(&entry.log_id)),
        payload: Some(payload),
    }
}

pub fn entry_from_proto(msg: pb::Entry) -> Result<Entry, Status> {
    let log_id = log_id_from_proto(&msg.log_id.ok_or_else(|| corrupt("entry without log id"))?);
    let payload = match msg
        .payload
        .ok_or_else(|| corrupt("entry without payload"))?
    {
        pb::entry::Payload::Blank(_) => EntryPayload::Blank,
        pb::entry::Payload::Op(op) => EntryPayload::Normal(Op::from_proto(op)?),
        pb::entry::Payload::Membership(m) => EntryPayload::Membership(membership_from_proto(m)),
    };
    Ok(Entry { log_id, payload })
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn sample_membership() -> Membership {
        let node = |id: u64| ClusterNode {
            peer_addr: format!("sepp-{id}.internal:50052"),
            client_addr: format!("sepp-{id}.example.com:50051"),
        };
        Membership::new(
            vec![BTreeSet::from([1, 2, 3])],
            BTreeMap::from([(1, node(1)), (2, node(2)), (3, node(3)), (4, node(4))]),
        )
    }

    fn log_id(term: u64, node_id: u64, index: u64) -> LogId {
        LogId {
            leader_id: LeaderId::new(term, node_id),
            index,
        }
    }

    #[test]
    fn entry_conversions_round_trip() {
        let entries = vec![
            Entry {
                log_id: log_id(3, 1, 7),
                payload: EntryPayload::Blank,
            },
            Entry {
                log_id: log_id(3, 1, 8),
                payload: EntryPayload::Normal(Op::Ack {
                    job_id: "job-1".into(),
                    attempt: 2,
                }),
            },
            Entry {
                log_id: log_id(4, 2, 9),
                payload: EntryPayload::Membership(sample_membership()),
            },
        ];

        for entry in entries {
            let bytes = entry_to_proto(&entry).encode_to_vec();
            let decoded =
                entry_from_proto(pb::Entry::decode(bytes.as_slice()).expect("decode proto"))
                    .expect("convert entry");
            assert_eq!(decoded.log_id, entry.log_id);
            assert_eq!(decoded.payload, entry.payload);
        }
    }

    #[test]
    fn vote_conversion_round_trips() {
        let vote = Vote::new_committed(5, 2);
        assert_eq!(vote_from_proto(&vote_to_proto(&vote)), vote);
        let vote = Vote::new(6, 3);
        assert_eq!(vote_from_proto(&vote_to_proto(&vote)), vote);
    }

    // Round-trips alone can't catch a field swap made symmetrically in both
    // conversion directions, so pin the conversions to the same golden bytes
    // cluster.rs pins for the raw protos (same sample values).
    #[test]
    fn conversions_agree_with_the_golden_bytes() {
        fn hex(bytes: &[u8]) -> String {
            bytes.iter().map(|b| format!("{b:02x}")).collect()
        }
        fn unhex(s: &str) -> Vec<u8> {
            (0..s.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
                .collect()
        }

        let vote = Vote::new_committed(3, 2);
        let vote_golden = "080310021801";
        assert_eq!(hex(&vote_to_proto(&vote).encode_to_vec()), vote_golden);
        assert_eq!(
            vote_from_proto(&pb::Vote::decode(unhex(vote_golden).as_slice()).expect("decode")),
            vote
        );

        let node = |id: u64| ClusterNode {
            peer_addr: format!("sepp-{id}.internal:50052"),
            client_addr: format!("sepp-{id}.example.com:50051"),
        };
        let golden_membership = Membership::new(
            vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([2, 3, 4])],
            BTreeMap::from([(1, node(1)), (2, node(2)), (3, node(3)), (4, node(4))]),
        );
        let gid = |index: u64| log_id(3, 2, index);

        let cases = [
            (
                Entry {
                    log_id: gid(8),
                    payload: EntryPayload::Blank,
                },
                "0a060803100218082200".to_string(),
            ),
            (
                Entry {
                    log_id: gid(9),
                    payload: EntryPayload::Membership(golden_membership),
                },
                "0a060803100218091ae2010a050a030102030a050a03020304123308011215736570702d312e696e7465726e616c3a35303035321a18736570702d312e6578616d706c652e636f6d3a3530303531123308021215736570702d322e696e7465726e616c3a35303035321a18736570702d322e6578616d706c652e636f6d3a3530303531123308031215736570702d332e696e7465726e616c3a35303035321a18736570702d332e6578616d706c652e636f6d3a3530303531123308041215736570702d342e696e7465726e616c3a35303035321a18736570702d342e6578616d706c652e636f6d3a3530303531".to_string(),
            ),
            (
                Entry {
                    log_id: gid(10),
                    payload: EntryPayload::Normal(Op::Ack {
                        job_id: "job-1".into(),
                        attempt: 2,
                    }),
                },
                "0a0608031002180a120b22090a056a6f622d311002".to_string(),
            ),
        ];

        for (entry, golden) in cases {
            assert_eq!(hex(&entry_to_proto(&entry).encode_to_vec()), golden);
            let decoded =
                entry_from_proto(pb::Entry::decode(unhex(&golden).as_slice()).expect("decode"))
                    .expect("convert");
            assert_eq!(decoded, entry);
        }
    }

    #[test]
    fn membership_conversion_keeps_learners() {
        let m = sample_membership();
        let decoded = membership_from_proto(membership_to_proto(&m));
        assert_eq!(decoded, m, "voters, learners and node records survive");
    }
}
