use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use uuid::Uuid;

use sepp::raft::net::{CallOpts, PeerClient, PeerConnector, TonicConnector};
use sepp::raft::testing::{TestNode, TestNodeOpts};
use sepp::raft::{ClusterNode, NodeId};

const CHILD_ENV: &str = "SEPP_PEER_TRANSPORT_CHILD";

fn temp_dir(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("sepp-2proc-{tag}-{}", Uuid::new_v4()))
}

// The re-exec'd child's side channel back to the parent, all files in one
// directory: `addr` (the child's bound peer address), `applied` (its last
// applied index, refreshed continuously), `stop` (parent asks it to exit).
struct ChildFiles {
    dir: PathBuf,
}

impl ChildFiles {
    fn addr(&self) -> PathBuf {
        self.dir.join("addr")
    }
    fn applied(&self) -> PathBuf {
        self.dir.join("applied")
    }
    fn stop(&self) -> PathBuf {
        self.dir.join("stop")
    }
}

// The remote node's entry point, dispatched by env var when this test binary
// re-executes itself. Without the env var it is an instant no-op so plain
// `cargo test` runs clean.
#[test]
fn child_peer_node() {
    let Ok(config) = std::env::var(CHILD_ENV) else {
        return;
    };
    let (control_dir, db_dir, cluster_id) = {
        let mut parts = config.splitn(3, ';');
        (
            PathBuf::from(parts.next().expect("control dir")),
            PathBuf::from(parts.next().expect("db dir")),
            parts
                .next()
                .expect("cluster id")
                .parse::<Uuid>()
                .expect("uuid"),
        )
    };
    let files = ChildFiles { dir: control_dir };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("child runtime");
    runtime.block_on(async move {
        let node = TestNode::start(TestNodeOpts::new(db_dir, 2, cluster_id))
            .await
            .expect("start child node");
        std::fs::write(files.addr(), node.addr.to_string()).expect("report child addr");

        let metrics = node.raft.metrics();
        let deadline = Instant::now() + Duration::from_secs(120);
        while !files.stop().exists() && Instant::now() < deadline {
            let applied = metrics.borrow().last_applied.map(|l| l.index).unwrap_or(0);
            let _ = std::fs::write(files.applied(), applied.to_string());
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        node.shutdown().await;
    });
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_child(files: &ChildFiles, db_dir: &std::path::Path, cluster_id: Uuid) -> ChildGuard {
    let config = format!("{};{};{cluster_id}", files.dir.display(), db_dir.display(),);
    let child = Command::new(std::env::current_exe().expect("test binary path"))
        .args(["--exact", "child_peer_node", "--nocapture"])
        .env(CHILD_ENV, config)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn child node process");
    ChildGuard(child)
}

fn wait_for_file(path: &std::path::Path, what: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(content) = std::fs::read_to_string(path)
            && !content.is_empty()
        {
            return content;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_and_handshake_across_processes() {
    let control_dir = temp_dir("ctl");
    std::fs::create_dir_all(&control_dir).expect("create control dir");
    let files = ChildFiles {
        dir: control_dir.clone(),
    };
    let parent_dir = temp_dir("parent");
    let child_dir = temp_dir("child");
    let cluster_id = Uuid::new_v4();

    let _child = spawn_child(&files, &child_dir, cluster_id);
    let child_addr = wait_for_file(&files.addr(), "the child's peer address");

    let node = TestNode::start(TestNodeOpts::new(parent_dir.clone(), 1, cluster_id))
        .await
        .expect("start parent node");

    let child_node = ClusterNode {
        peer_addr: child_addr.clone(),
        client_addr: child_addr.clone(),
    };
    node.raft
        .initialize(BTreeMap::from([
            (1 as NodeId, node.cluster_node()),
            (2 as NodeId, child_node.clone()),
        ]))
        .await
        .expect("initialize the two-node cluster");

    let metrics = node.raft.metrics();
    let deadline = Instant::now() + Duration::from_secs(15);
    while metrics.borrow().current_leader != Some(1) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        metrics.borrow().current_leader,
        Some(1),
        "the parent needs the child's vote over the wire to elect itself",
    );

    for i in 0..5 {
        node.raft
            .client_write(sepp::op::Op::OpenQueue {
                queue: format!("cross-process-{i}"),
            })
            .await
            .expect("client write");
    }
    let last_log = metrics.borrow().last_log_index.expect("leader has a log");

    // The child reports its applied index over the file channel; reaching
    // last_log proves append, flush-ack and apply in the other process.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let applied: u64 = std::fs::read_to_string(files.applied())
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if applied >= last_log {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "child never applied the leader's log (at {applied} of {last_log})",
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Handshake across processes reports the child's identity.
    let client = TonicConnector::new(node.local.clone(), &sepp::config::ClusterConfig::default())
        .expect("connector")
        .connect(2, &child_node);
    let hello = client
        .handshake(
            node.local.hello(),
            CallOpts {
                timeout: Some(Duration::from_secs(5)),
            },
        )
        .await
        .expect("handshake with the child process");
    assert_eq!(hello.node_id, 2);
    assert_eq!(hello.cluster_id, cluster_id.as_bytes().to_vec());
    assert_eq!(hello.op_format_version, sepp::op::OP_FORMAT_VERSION);

    std::fs::write(files.stop(), "").expect("ask the child to exit");
    node.shutdown().await;

    for dir in [control_dir, parent_dir, child_dir] {
        let _ = std::fs::remove_dir_all(dir);
    }
}
