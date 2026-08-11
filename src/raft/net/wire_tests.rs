use std::collections::BTreeMap;
use std::time::Duration;

use uuid::Uuid;

use super::super::testing::{TestNode, TestNodeOpts};
use super::dispatch::LocalPeer;
use super::grpc::TonicConnector;
use super::{CallOpts, PeerClient, PeerConnector, PeerError, SNAPSHOT_CHUNK_BYTES};
use crate::config::ClusterConfig;
use crate::op::Op;
use crate::pb::sepp::raft::v1 as pb;

fn temp_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("sepp-wire-{tag}-{}", Uuid::new_v4()))
}

async fn wait_until(what: &str, mut check: impl FnMut() -> bool) {
    for _ in 0..600 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_replicate_apply_and_transfer_leadership() {
    let cluster_id = Uuid::new_v4();
    let dirs: Vec<_> = (1..=3).map(|i| temp_dir(&format!("n{i}"))).collect();

    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        let opts = TestNodeOpts::new(dir.clone(), (i + 1) as u64, cluster_id);
        nodes.push(TestNode::start(opts).await.expect("start node"));
    }

    let members: BTreeMap<u64, _> = nodes
        .iter()
        .map(|n| (n.node_id, n.cluster_node()))
        .collect();
    nodes[0]
        .raft
        .initialize(members)
        .await
        .expect("initialize the cluster from node 1");

    // Only node 1 knows the membership, so it campaigns; 2 and 3 grant votes
    // over the wire and then learn the membership by replication.
    let m1 = nodes[0].raft.metrics();
    wait_until("node 1 to win the election", || {
        m1.borrow().current_leader == Some(1)
    })
    .await;

    for i in 0..3 {
        nodes[0]
            .raft
            .client_write(Op::OpenQueue {
                queue: format!("wire-{i}"),
            })
            .await
            .expect("client write on the leader");
    }

    // Followers must catch up and *apply*: their own metrics advance only
    // through their committer thread, so this proves append, flush-ack and
    // SM apply end to end on every node.
    let last_log = m1.borrow().last_log_index.expect("leader has a log");
    for node in &nodes[1..] {
        let metrics = node.raft.metrics();
        wait_until("the follower to apply the leader's log", || {
            metrics
                .borrow()
                .last_applied
                .is_some_and(|l| l.index >= last_log)
        })
        .await;
    }

    // The elect nudge is the leadership-transfer mechanism. A single nudge
    // can lose to a concurrently campaigning peer (same term, higher node
    // id wins), which is why the shutdown recipe re-nudges; the test does
    // the same.
    let connector =
        TonicConnector::new(nodes[0].local.clone(), &ClusterConfig::default()).expect("connector");
    let client = connector.connect(2, &nodes[1].cluster_node());
    let m2 = nodes[1].raft.metrics();
    let mut transferred = false;
    for _ in 0..10 {
        client
            .trigger_elect(CallOpts {
                timeout: Some(Duration::from_secs(2)),
            })
            .await
            .expect("trigger elect on node 2");

        for _ in 0..80 {
            if m1.borrow().current_leader == Some(2) {
                transferred = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if transferred {
            break;
        }
    }
    let m3 = nodes[2].raft.metrics();
    if !transferred {
        panic!(
            "node 2 never took leadership:\n n1: {:?} leader={:?} term={}\n n2: {:?} leader={:?} term={}\n n3: {:?} leader={:?} term={}",
            m1.borrow().state,
            m1.borrow().current_leader,
            m1.borrow().current_term,
            m2.borrow().state,
            m2.borrow().current_leader,
            m2.borrow().current_term,
            m3.borrow().state,
            m3.borrow().current_leader,
            m3.borrow().current_term,
        );
    }

    for node in nodes {
        node.shutdown().await;
    }
    for dir in dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}

// Exercises the snapshot stream over the wire up to the engine's vote
// check: a stale-vote install streams, reassembles and is refused without
// touching the state machine, so the file contents never matter. The real
// install path is PR 10's wire test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_vote_snapshot_stream_is_refused_by_the_engine() {
    let cluster_id = Uuid::new_v4();
    let dir = temp_dir("snap");
    let node = TestNode::start(TestNodeOpts::new(dir.clone(), 1, cluster_id))
        .await
        .expect("start node");
    node.raft
        .initialize(BTreeMap::from([(1, node.cluster_node())]))
        .await
        .expect("initialize");
    let metrics = node.raft.metrics();
    wait_until("self-election", || {
        metrics.borrow().current_leader == Some(1)
    })
    .await;

    let payload = temp_dir("snap-payload").with_extension("bin");
    std::fs::write(&payload, vec![0xa5; 3 * SNAPSHOT_CHUNK_BYTES + 17]).expect("write payload");

    let peer = LocalPeer::new(9, Uuid::new_v4(), Some(cluster_id), [0; 32]);
    let client = TonicConnector::new(peer, &ClusterConfig::default())
        .expect("connector")
        .connect(1, &node.cluster_node());

    let stale = pb::InstallSnapshotStart {
        vote: Some(pb::Vote {
            term: 0,
            node_id: 9,
            committed: true,
        }),
        meta: Some(pb::SnapshotMeta {
            last_log_id: None,
            last_membership: None,
            snapshot_id: "stale-test".into(),
        }),
    };
    let vote = client
        .install_snapshot(stale, payload.clone())
        .await
        .expect("a stale-vote install answers with the higher vote, not an error");
    assert!(
        vote.term >= 1,
        "the response carries the receiver's own newer vote: {vote:?}"
    );

    node.shutdown().await;
    let _ = std::fs::remove_file(payload);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_guards_refuse_foreign_clusters_and_bad_auth() {
    let cluster_id = Uuid::new_v4();
    let dir = temp_dir("guarded");
    let mut opts = TestNodeOpts::new(dir.clone(), 1, cluster_id);
    opts.peer_auth_keys = Some(vec!["good-key".into()]);
    let node = TestNode::start(opts).await.expect("start node");

    let connect = |cluster: Uuid, keys: Option<Vec<String>>| {
        let local = LocalPeer::new(9, Uuid::new_v4(), Some(cluster), [0; 32]);
        let config = ClusterConfig {
            peer_auth_keys: keys,
            ..Default::default()
        };
        TonicConnector::new(local, &config)
            .expect("connector")
            .connect(1, &node.cluster_node())
    };
    let opts = || CallOpts {
        timeout: Some(Duration::from_secs(2)),
    };

    // No key: refused before anything else, with the distinct auth error.
    let unauthenticated = connect(cluster_id, None);
    let err = unauthenticated
        .handshake(pb::PeerHello::default(), opts())
        .await
        .expect_err("no key must fail");
    assert!(matches!(err, PeerError::AuthFailed(_)), "{err:?}");

    // Wrong key: same classification.
    let wrong_key = connect(cluster_id, Some(vec!["wrong".into()]));
    let err = wrong_key
        .handshake(pb::PeerHello::default(), opts())
        .await
        .expect_err("a wrong key must fail");
    assert!(matches!(err, PeerError::AuthFailed(_)), "{err:?}");

    // Authenticated handshake answers identity even across cluster ids: the
    // join pre-check and operators need a legible answer.
    let foreign = connect(Uuid::new_v4(), Some(vec!["good-key".into()]));
    let hello = foreign
        .handshake(pb::PeerHello::default(), opts())
        .await
        .expect("handshake answers a foreign caller");
    assert_eq!(hello.node_id, 1);
    assert_eq!(hello.cluster_id, cluster_id.as_bytes().to_vec());

    // But raft RPCs from a foreign cluster are refused by name.
    let err = foreign
        .vote(
            pb::VoteRequest {
                vote: Some(pb::Vote {
                    term: 1,
                    node_id: 9,
                    committed: false,
                }),
                last_log_id: None,
            },
            opts(),
        )
        .await
        .expect_err("a foreign vote must be refused");
    match &err {
        PeerError::Refused(message) => {
            assert!(
                message.contains("belongs to"),
                "names both clusters: {message}"
            );
        }
        other => panic!("expected Refused, got {other:?}"),
    }

    let err = foreign
        .append_entries(
            pb::AppendEntriesRequest {
                vote: Some(pb::Vote {
                    term: 1,
                    node_id: 9,
                    committed: true,
                }),
                prev_log_id: None,
                entries: Vec::new(),
                leader_commit: None,
                op_format_version: 1,
            },
            opts(),
        )
        .await
        .expect_err("a foreign append must be refused");
    assert!(matches!(err, PeerError::Refused(_)), "{err:?}");

    node.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}
