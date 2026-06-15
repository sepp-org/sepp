//! Shared test helpers used by both `integration.rs` and `admin.rs`.
//! Each test file adds `mod common;` and reuses these primitives.

use std::collections::HashMap;
use std::io::BufRead;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use sepp::pb::sepp::v1::{
    AckRequest, EnqueueBatchRequest, EnqueueRequest, EnqueueResponse, Job, ReserveRequest,
    job_result, queue_service_client::QueueServiceClient,
};
use tonic::transport::Channel;

pub type Client = QueueServiceClient<Channel>;

/// Startup log-line substrings we extract bound ports from, keyed by the short
/// name tests ask for. The server binds `127.0.0.1:0` and logs the *actual*
/// bound address, so these carry the OS-assigned port.
pub const PORT_MARKERS: &[(&str, &str)] = &[
    ("grpc", "queue server listening"),
    ("prometheus", "prometheus metrics endpoint listening"),
    ("admin", "admin UI listening"),
];

/// How long to wait for a freshly spawned server to report its bound port.
pub const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// Bound ports discovered from the server's stdout, filled in as it logs each
/// "... listening" line. Shared with the stdout-draining thread.
#[derive(Clone)]
pub struct PortBook {
    pub ports: Arc<Mutex<HashMap<&'static str, u16>>>,
}

/// Pulls the port out of a `127.0.0.1:<port>` substring. Works for both the
/// text (`addr=127.0.0.1:54321`) and JSON (`"addr":"127.0.0.1:54321"`) log
/// formats.
pub fn parse_listen_port(line: &str) -> Option<u16> {
    const NEEDLE: &str = "127.0.0.1:";
    let start = line.find(NEEDLE)? + NEEDLE.len();
    line[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()
}

/// Polls `book` until `key`'s port appears, or returns `None` after `timeout`.
pub async fn wait_for_port(book: &PortBook, key: &str, timeout: Duration) -> Option<u16> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(port) = book.ports.lock().expect("port book lock").get(key).copied() {
            return Some(port);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// An empty config file in the temp dir, shared by every test server that
/// does not bring its own config. The server treats an empty file as
/// all-defaults; concurrent re-writes of identical content are harmless.
pub fn isolated_config_path() -> PathBuf {
    let path = std::env::temp_dir().join("sepp-integration-defaults.toml");
    if !path.exists() {
        let _ = std::fs::write(&path, "");
    }
    path
}

/// Spawns the server binary with `extra_env`, binding `127.0.0.1:0` so the OS
/// hands out a free, unique port. Returns the child and a [`PortBook`] the
/// caller queries (via [`wait_for_port`]) for the actual bound port(s).
pub fn spawn_server_process(
    db_path: &std::path::Path,
    extra_env: &[(&str, &str)],
) -> (Child, PortBook) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sepp"));
    command
        .env("SEPP_SERVER__LISTEN_ADDR", "127.0.0.1:0")
        // The admin UI is on by default; its fixed default port would clash
        // across concurrently spawned test servers.
        .env("SEPP_ADMIN__LISTEN_ADDR", "127.0.0.1:0")
        .env("SEPP_SERVER__DB_PATH", db_path)
        // Isolate from any sepp.toml in the developer's working directory:
        // a local file pinning ports or strict_queues would break
        // concurrently spawned test servers. Tests that need a config
        // override SEPP_CONFIG through `extra_env`.
        .env("SEPP_CONFIG", isolated_config_path())
        // Force info so the startup "listening" lines are always emitted,
        // whatever level a test's config might otherwise set.
        .env("RUST_LOG", "sepp=info")
        .stdout(Stdio::piped());
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let mut child = command.spawn().expect("spawn sepp server");

    let book = PortBook {
        ports: Arc::new(Mutex::new(HashMap::new())),
    };
    let stdout = child.stdout.take().expect("child stdout is piped");
    let sink = book.clone();
    // Drain stdout for the child's whole life so its log writes never block on a
    // full pipe; record listening ports as they appear.
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
        {
            for (key, needle) in PORT_MARKERS {
                if line.contains(needle)
                    && let Some(port) = parse_listen_port(&line)
                {
                    sink.ports.lock().expect("port book lock").insert(key, port);
                }
            }
        }
    });

    (child, book)
}

/// Connects a gRPC client to `127.0.0.1:port`, retrying until reachable.
pub async fn connect_client(port: u16) -> Client {
    let addr = format!("http://127.0.0.1:{port}");
    for _ in 0..100 {
        if let Ok(client) = QueueServiceClient::connect(addr.clone()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("server did not become reachable on {addr}");
}

pub fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// A `Duration` as a wire `Duration`.
pub fn dur(d: Duration) -> prost_types::Duration {
    sepp::pb::millis_to_duration(d.as_millis() as u64)
}

/// An epoch-ms instant as a wire `Timestamp`.
pub fn ts_ms(ms: i64) -> prost_types::Timestamp {
    sepp::pb::millis_to_timestamp(ms)
}

/// A wire `Timestamp` back to epoch ms.
#[allow(dead_code)]
pub fn ts_to_ms(ts: &prost_types::Timestamp) -> i64 {
    sepp::pb::timestamp_to_millis(ts)
}

pub fn epoch_ms_in(d: Duration) -> i64 {
    (SystemTime::now() + d)
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

pub const LEASE: Duration = Duration::from_secs(30);
pub const WAIT: Duration = Duration::from_secs(5);

pub fn enqueue_req(queue: &str) -> EnqueueRequest {
    EnqueueRequest {
        queue: queue.to_string(),
        job_type: "smoke-job".to_string(),
        ..Default::default()
    }
}

/// Enqueues a single job through `EnqueueBatch` and unwraps its success result.
pub async fn enqueue(client: &Client, req: EnqueueRequest) -> EnqueueResponse {
    let result = client
        .clone()
        .enqueue_batch(EnqueueBatchRequest { jobs: vec![req] })
        .await
        .expect("enqueue_batch RPC")
        .into_inner()
        .results
        .into_iter()
        .next()
        .expect("one result per submitted job");
    match result.outcome {
        Some(job_result::Outcome::Success(s)) => s,
        other => panic!("job was not accepted: {other:?}"),
    }
}

pub async fn ack(client: &Client, job: &Job) {
    client
        .clone()
        .ack(AckRequest {
            job_id: job.id.clone(),
            attempt: job.attempt,
            worker_id: None,
        })
        .await
        .expect("ack RPC");
}

pub async fn reserve(client: &Client, queue: &str, lease: Duration, wait: Duration) -> Option<Job> {
    client
        .clone()
        .reserve(ReserveRequest {
            queues: vec![queue.to_string()],
            wait_timeout: Some(dur(wait)),
            lease_duration: Some(dur(lease)),
            worker_id: None,
            max_jobs: None,
        })
        .await
        .expect("reserve RPC")
        .into_inner()
        .jobs
        .into_iter()
        .next()
}

pub struct ServerGuard {
    pub child: Child,
    pub db_path: Option<PathBuf>,
    pub cfg_path: Option<PathBuf>,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(path) = &self.db_path {
            let _ = std::fs::remove_dir_all(path);
        }
        if let Some(path) = &self.cfg_path {
            let _ = std::fs::remove_file(path);
        }
    }
}
