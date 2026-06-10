//! End-to-end tests for the admin web UI plane: HTTP API, SSE stats, job
//! inspection, dead-letter operations, queue creation and deletion and the
//! config editor, driven against a real server process. Mirrors the harness
//! in `tests/integration.rs`, plus a raw HTTP/1.1 client over `TcpStream` so
//! no extra dependencies are needed.

use std::collections::{HashMap, HashSet};
use std::io::BufRead;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use sepp::pb::sepp::v1::{
    AckRequest, EnqueueBatchRequest, EnqueueRequest, EnqueueResponse, Job, NackRequest, NackRetry,
    Payload, ReserveRequest, job_result, nack_retry, queue_service_client::QueueServiceClient,
};
use tonic::transport::Channel;

type Client = QueueServiceClient<Channel>;

// ---------------------------------------------------------------------------
// Server harness (mirrors tests/integration.rs)
// ---------------------------------------------------------------------------

/// Startup log-line substrings we extract bound ports from. The server binds
/// `127.0.0.1:0` and logs the actual bound address.
const PORT_MARKERS: &[(&str, &str)] = &[
    ("grpc", "queue server listening"),
    ("admin", "admin UI listening"),
];

const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct PortBook {
    ports: Arc<Mutex<HashMap<&'static str, u16>>>,
}

fn parse_listen_port(line: &str) -> Option<u16> {
    const NEEDLE: &str = "127.0.0.1:";
    let start = line.find(NEEDLE)? + NEEDLE.len();
    line[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()
}

async fn wait_for_port(book: &PortBook, key: &str, timeout: Duration) -> Option<u16> {
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

fn spawn_server_process(
    db_path: &std::path::Path,
    extra_env: &[(&str, &str)],
) -> (Child, PortBook) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sepp"));
    command
        .env("SEPP_SERVER__LISTEN_ADDR", "127.0.0.1:0")
        .env("SEPP_SERVER__DB_PATH", db_path)
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
    // Drain stdout for the child's whole life so its log writes never block.
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

async fn connect_client(port: u16) -> Client {
    let addr = format!("http://127.0.0.1:{port}");
    for _ in 0..100 {
        if let Ok(client) = QueueServiceClient::connect(addr.clone()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("server did not become reachable on {addr}");
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

struct ServerGuard {
    child: Child,
    db_path: std::path::PathBuf,
    cfg_path: std::path::PathBuf,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.db_path);
        let _ = std::fs::remove_file(&self.cfg_path);
    }
}

const ADMIN_CFG: &str = "[admin]\nenabled = true\nlisten_addr = \"127.0.0.1:0\"\n";

/// Boots a server from `toml` with the admin UI on port 0. The config file
/// stays on disk for the server's whole life so the watcher and the config
/// editor can rewrite it; the guard removes it on drop.
async fn start_admin_server(
    tag: &str,
    toml: &str,
    extra_env: &[(&str, &str)],
) -> (ServerGuard, Client, u16) {
    let unique = unique_suffix();
    let db_path = std::env::temp_dir().join(format!("sepp-adm-{tag}-{unique}"));
    let cfg_path = std::env::temp_dir().join(format!("sepp-adm-{tag}-{unique}.toml"));
    std::fs::write(&cfg_path, toml).expect("write temp config");
    let cfg_str = cfg_path.to_str().expect("utf-8 config path").to_string();

    let mut env: Vec<(&str, &str)> = vec![("SEPP_CONFIG", cfg_str.as_str())];
    env.extend_from_slice(extra_env);
    let (mut child, book) = spawn_server_process(&db_path, &env);

    let grpc_port = match wait_for_port(&book, "grpc", STARTUP_TIMEOUT).await {
        Some(port) => port,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            panic!("server did not report a grpc port");
        }
    };
    let admin_port = match wait_for_port(&book, "admin", STARTUP_TIMEOUT).await {
        Some(port) => port,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            panic!("server did not report an admin port");
        }
    };
    let client = connect_client(grpc_port).await;

    (
        ServerGuard {
            child,
            db_path,
            cfg_path,
        },
        client,
        admin_port,
    )
}

// ---------------------------------------------------------------------------
// gRPC helpers (subset of tests/integration.rs)
// ---------------------------------------------------------------------------

const LEASE: Duration = Duration::from_secs(30);
const WAIT: Duration = Duration::from_secs(5);

fn dur(d: Duration) -> prost_types::Duration {
    sepp::pb::millis_to_duration(d.as_millis() as u64)
}

fn ts_ms(ms: i64) -> prost_types::Timestamp {
    sepp::pb::millis_to_timestamp(ms)
}

fn epoch_ms_in(d: Duration) -> i64 {
    (SystemTime::now() + d)
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn enqueue_req(queue: &str) -> EnqueueRequest {
    EnqueueRequest {
        queue: queue.to_string(),
        job_type: "smoke-job".to_string(),
        ..Default::default()
    }
}

async fn enqueue(client: &Client, req: EnqueueRequest) -> EnqueueResponse {
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

async fn reserve(client: &Client, queue: &str, lease: Duration, wait: Duration) -> Option<Job> {
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

async fn ack(client: &Client, job: &Job) {
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

async fn nack_dead_letter(client: &Client, job: &Job, reason: &str) -> bool {
    client
        .clone()
        .nack(NackRequest {
            job_id: job.id.clone(),
            attempt: job.attempt,
            reason: Some(reason.to_string()),
            retry: Some(NackRetry {
                strategy: Some(nack_retry::Strategy::DeadLetter(())),
            }),
            worker_id: None,
        })
        .await
        .expect("nack RPC")
        .into_inner()
        .dead_lettered
}

// ---------------------------------------------------------------------------
// Raw HTTP/1.1 client
// ---------------------------------------------------------------------------

struct HttpResponse {
    status: u16,
    body: String,
}

impl HttpResponse {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body)
            .unwrap_or_else(|e| panic!("response body is not JSON ({e}): {:?}", self.body))
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Decodes as much of an HTTP/1.1 chunked body as `raw` holds. Returns the
/// decoded text and how many bytes were consumed; a trailing partial chunk
/// stays unconsumed for the caller's next read to complete.
fn dechunk(raw: &[u8]) -> (String, usize) {
    let mut out = String::new();
    let mut pos = 0;
    while let Some(line_end) = find(&raw[pos..], b"\r\n") {
        let size_text = std::str::from_utf8(&raw[pos..pos + line_end]).unwrap_or("");
        let Ok(size) = usize::from_str_radix(size_text.split(';').next().unwrap_or("").trim(), 16)
        else {
            break;
        };
        let data_start = pos + line_end + 2;
        if size == 0 {
            pos = data_start;
            break;
        }
        if raw.len() < data_start + size + 2 {
            break;
        }
        out.push_str(&String::from_utf8_lossy(
            &raw[data_start..data_start + size],
        ));
        pos = data_start + size + 2;
    }
    (out, pos)
}

fn parse_response(raw: &[u8]) -> HttpResponse {
    let header_end = find(raw, b"\r\n\r\n").expect("response has a header block");
    let head = String::from_utf8_lossy(&raw[..header_end]).into_owned();
    let mut lines = head.lines();
    let status: u16 = lines
        .next()
        .expect("status line")
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric status code");
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(n, v)| (n.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();

    let rest = &raw[header_end + 4..];
    let chunked = headers
        .iter()
        .any(|(n, v)| n == "transfer-encoding" && v.contains("chunked"));
    let body = if chunked {
        dechunk(rest).0
    } else {
        String::from_utf8_lossy(rest).into_owned()
    };

    HttpResponse { status, body }
}

async fn http(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> HttpResponse {
    let work = async {
        let mut stream = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect to admin endpoint");
        let mut req =
            format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
        for (name, value) in headers {
            req.push_str(&format!("{name}: {value}\r\n"));
        }
        if let Some(body) = body {
            req.push_str(&format!(
                "Content-Type: application/json\r\nContent-Length: {}\r\n",
                body.len()
            ));
        }
        req.push_str("\r\n");
        if let Some(body) = body {
            req.push_str(body);
        }
        stream
            .write_all(req.as_bytes())
            .await
            .expect("write request");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.expect("read response");
        parse_response(&raw)
    };
    tokio::time::timeout(Duration::from_secs(15), work)
        .await
        .unwrap_or_else(|_| panic!("{method} {path} timed out"))
}

/// Percent-encodes a base64 value for use in a path segment or query string
/// ('+', '/', and '=' are not query-safe), the way a browser client would.
fn url_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// SSE reader: incremental, deadline-bounded reads over a dechunked stream
// ---------------------------------------------------------------------------

struct SseReader {
    stream: TcpStream,
    raw: Vec<u8>,
    body: String,
}

async fn read_some(stream: &mut TcpStream, into: &mut Vec<u8>) {
    let mut buf = [0u8; 4096];
    match tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf)).await {
        Ok(Ok(0)) => panic!("SSE stream closed unexpectedly"),
        Ok(Ok(n)) => into.extend_from_slice(&buf[..n]),
        Ok(Err(e)) => panic!("SSE read failed: {e}"),
        // A quiet half-second; the caller re-checks its own deadline.
        Err(_) => {}
    }
}

async fn sse_connect(port: u16) -> SseReader {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect to admin endpoint");
    let req =
        "GET /admin/api/v1/events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n";
    stream
        .write_all(req.as_bytes())
        .await
        .expect("write SSE request");

    let mut raw = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    let header_end = loop {
        if let Some(end) = find(&raw, b"\r\n\r\n") {
            break end;
        }
        assert!(
            Instant::now() < deadline,
            "SSE response headers never arrived"
        );
        read_some(&mut stream, &mut raw).await;
    };
    let head = String::from_utf8_lossy(&raw[..header_end]).into_owned();
    assert!(
        head.starts_with("HTTP/1.1 200"),
        "SSE request failed: {head}"
    );

    let rest = raw.split_off(header_end + 4);
    let mut reader = SseReader {
        stream,
        raw: rest,
        body: String::new(),
    };
    reader.drain();
    reader
}

impl SseReader {
    fn drain(&mut self) {
        let (text, consumed) = dechunk(&self.raw);
        self.body.push_str(&text);
        self.raw.drain(..consumed);
    }

    async fn wait_for(&mut self, deadline: Instant, mut pred: impl FnMut(&str) -> bool) {
        loop {
            if pred(&self.body) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "SSE stream never matched; received so far:\n{}",
                self.body
            );
            read_some(&mut self.stream, &mut self.raw).await;
            self.drain();
        }
    }
}

/// Every complete `event: stats` frame in the decoded SSE body, in order.
fn stats_frames(body: &str) -> Vec<Value> {
    let mut frames = Vec::new();
    let mut lines = body.lines().peekable();
    while let Some(line) = lines.next() {
        if line.trim() == "event: stats"
            && let Some(data) = lines.peek().and_then(|l| l.strip_prefix("data: "))
            && let Ok(frame) = serde_json::from_str(data)
        {
            frames.push(frame);
        }
    }
    frames
}

// ---------------------------------------------------------------------------
// 1. Overview + queues
// ---------------------------------------------------------------------------

#[tokio::test]
async fn overview_and_queues_report_depths_and_totals() {
    let (_guard, client, port) = start_admin_server("overview", ADMIN_CFG, &[]).await;

    for _ in 0..3 {
        enqueue(&client, enqueue_req("adm-ov-q")).await;
    }

    // Snapshots flow committer -> ArcSwap (250ms gate) -> 1 Hz stats hub, so
    // poll the observable frame rather than sleeping a fixed amount.
    let deadline = Instant::now() + Duration::from_secs(10);
    let frame = loop {
        let resp = http(port, "GET", "/admin/api/v1/overview", &[], None).await;
        assert_eq!(resp.status, 200);
        let overview = resp.json();
        if overview["frame"]["queues"]["adm-ov-q"]["ready"] == json!(3) {
            assert_eq!(overview["server"]["version"], env!("CARGO_PKG_VERSION"));
            assert!(overview["server"]["started_at_ms"].as_i64().unwrap() > 0);
            assert!(overview["server"]["now_ms"].as_i64().unwrap() > 0);
            assert!(overview["server"]["command_queue_len"].is_number());
            assert!(overview["history"].is_object());
            break overview["frame"].clone();
        }
        assert!(
            Instant::now() < deadline,
            "overview never showed the enqueued jobs: {overview}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let q = &frame["queues"]["adm-ov-q"];
    assert_eq!(q["scheduled"], json!(0));
    assert_eq!(q["inflight"], json!(0));
    assert_eq!(q["dead_lettered"], json!(0));
    assert_eq!(q["totals"]["enqueued"], json!(3));
    assert_eq!(q["totals"]["acked"], json!(0));
    assert!(q["rates"]["enqueued"].is_number());

    // The queues listing reports the same depths for the undeclared queue.
    let resp = http(port, "GET", "/admin/api/v1/queues", &[], None).await;
    assert_eq!(resp.status, 200);
    let queues = resp.json();
    let entry = queues
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "adm-ov-q")
        .expect("the active queue is listed");
    assert_eq!(entry["declared"], json!(false));
    assert_eq!(entry["depths"]["ready"], json!(3));
    assert!(entry["overrides"].is_null());
    assert!(entry["effective"]["max_payload_bytes"].is_number());

    let resp = http(port, "GET", "/admin/api/v1/queues/adm-ov-q", &[], None).await;
    assert_eq!(resp.status, 200);
    assert_eq!(resp.json()["depths"]["ready"], json!(3));
}

// ---------------------------------------------------------------------------
// 2. Peek + cursor paging
// ---------------------------------------------------------------------------

#[tokio::test]
async fn peek_lists_ready_jobs_and_pages_with_cursors() {
    let (_guard, client, port) = start_admin_server("peek", ADMIN_CFG, &[]).await;

    let mut ids = HashSet::new();
    for i in 0..5 {
        let resp = enqueue(
            &client,
            EnqueueRequest {
                payload: Some(Payload {
                    data: format!("p{i}").into_bytes(),
                    encoding: "text/plain".to_string(),
                }),
                ..enqueue_req("adm-peek-q")
            },
        )
        .await;
        ids.insert(resp.job_id);
    }

    // Peeks answer from the committer's in-memory indexes, so the jobs are
    // visible as soon as the enqueue RPC returns.
    let resp = http(
        port,
        "GET",
        "/admin/api/v1/queues/adm-peek-q/jobs?state=ready",
        &[],
        None,
    )
    .await;
    assert_eq!(resp.status, 200);
    let page = resp.json();
    assert_eq!(page["truncated"], json!(false));
    let jobs = page["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 5);
    let mut payloads = HashSet::new();
    for job in jobs {
        assert!(ids.contains(job["id"].as_str().unwrap()));
        assert_eq!(job["job_type"], "smoke-job");
        assert_eq!(job["attempt"], json!(1));
        assert_eq!(job["payload"]["encoding"], "text/plain");
        let data = B64
            .decode(
                job["payload"]["data_b64"]
                    .as_str()
                    .expect("small payloads are inlined"),
            )
            .unwrap();
        payloads.insert(String::from_utf8(data).unwrap());
    }
    assert_eq!(payloads, (0..5).map(|i| format!("p{i}")).collect());

    // The job-detail route resolves a live job by id; unknown ids are 404.
    let some_id = ids.iter().next().unwrap();
    let resp = http(
        port,
        "GET",
        &format!("/admin/api/v1/jobs/{some_id}"),
        &[],
        None,
    )
    .await;
    assert_eq!(resp.status, 200);
    let detail = resp.json();
    assert_eq!(detail["id"], some_id.as_str());
    assert!(detail["payload"]["data_b64"].is_string());
    let resp = http(
        port,
        "GET",
        "/admin/api/v1/jobs/00000000-0000-4000-8000-000000000000",
        &[],
        None,
    )
    .await;
    assert_eq!(resp.status, 404);

    // limit=2 cursor paging walks all five jobs without overlap.
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let path = match &cursor {
            Some(c) => format!(
                "/admin/api/v1/queues/adm-peek-q/jobs?state=ready&limit=2&cursor={}",
                url_encode(c)
            ),
            None => "/admin/api/v1/queues/adm-peek-q/jobs?state=ready&limit=2".to_string(),
        };
        let resp = http(port, "GET", &path, &[], None).await;
        assert_eq!(resp.status, 200, "paging failed: {}", resp.body);
        let page = resp.json();
        for job in page["jobs"].as_array().unwrap() {
            seen.push(job["id"].as_str().unwrap().to_string());
        }
        assert!(seen.len() <= 5, "cursor paging looped past the queue");
        match page["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_string()),
            None => break,
        }
    }
    assert_eq!(seen.len(), 5, "paging visits every job exactly once");
    assert_eq!(
        seen.iter().cloned().collect::<HashSet<_>>(),
        ids,
        "pages cover the full queue without overlap"
    );
}

// ---------------------------------------------------------------------------
// 3. Enqueue over HTTP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_enqueue_round_trips_and_rejects_bad_encoding() {
    let cfg = format!("{ADMIN_CFG}\n[limits]\nallowed_encodings = [\"json\"]\n");
    let (_guard, client, port) = start_admin_server("henq", &cfg, &[]).await;

    let body = json!({
        "job_type": "via-http",
        "payload": { "encoding": "json", "data_text": "{\"n\":1}" },
        "priority": 7,
        "custom": { "region": "eu", "retries": 2 },
    })
    .to_string();
    let resp = http(
        port,
        "POST",
        "/admin/api/v1/queues/adm-henq-q/jobs",
        &[],
        Some(&body),
    )
    .await;
    assert_eq!(resp.status, 200, "enqueue failed: {}", resp.body);
    let job_id = resp.json()["job_id"].as_str().unwrap().to_string();
    assert!(!job_id.is_empty());

    // Peek sees it with the submitted fields intact.
    let resp = http(
        port,
        "GET",
        "/admin/api/v1/queues/adm-henq-q/jobs?state=ready",
        &[],
        None,
    )
    .await;
    let page = resp.json();
    let jobs = page["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["id"], job_id.as_str());
    assert_eq!(jobs[0]["priority"], json!(7));
    assert_eq!(jobs[0]["custom"]["region"], "eu");
    assert_eq!(jobs[0]["custom"]["retries"], json!(2));

    // A worker can actually run the HTTP-enqueued job.
    let job = reserve(&client, "adm-henq-q", LEASE, WAIT)
        .await
        .expect("HTTP-enqueued job is reservable");
    assert_eq!(job.id, job_id);
    assert_eq!(job.job_type, "via-http");
    assert_eq!(
        job.payload.as_ref().map(|p| p.data.as_slice()),
        Some(&b"{\"n\":1}"[..])
    );
    ack(&client, &job).await;

    // A disallowed encoding is a 422 carrying the rejection reason.
    let bad = json!({
        "job_type": "via-http",
        "payload": { "encoding": "xml", "data_text": "<x/>" },
    })
    .to_string();
    let resp = http(
        port,
        "POST",
        "/admin/api/v1/queues/adm-henq-q/jobs",
        &[],
        Some(&bad),
    )
    .await;
    assert_eq!(resp.status, 422);
    let err = resp.json();
    assert_eq!(err["code"], "rejected");
    assert_eq!(err["rejection"]["reason"], "encoding_not_allowed");
    assert!(
        err["rejection"]["detail"].as_str().unwrap().contains("xml"),
        "detail names the offending encoding: {err}"
    );
}

// ---------------------------------------------------------------------------
// 4. Dead letters: peek, requeue, delete
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dead_letters_peek_requeue_and_delete() {
    let cfg = format!("{ADMIN_CFG}\n[storage]\ndead_letter_retention_ms = 600000\n");
    let (_guard, client, port) = start_admin_server("dlq", &cfg, &[]).await;
    let jobs_path = |state: &str| format!("/admin/api/v1/queues/adm-dlq-q/jobs?state={state}");

    enqueue(
        &client,
        EnqueueRequest {
            payload: Some(Payload {
                data: b"replay-me".to_vec(),
                encoding: "text/plain".to_string(),
            }),
            ..enqueue_req("adm-dlq-q")
        },
    )
    .await;
    let job = reserve(&client, "adm-dlq-q", LEASE, WAIT)
        .await
        .expect("job reservable before dead-lettering");
    let job_id = job.id.clone();
    assert!(nack_dead_letter(&client, &job, "boom").await);

    let page = http(port, "GET", &jobs_path("dead_letter"), &[], None)
        .await
        .json();
    let records = page["jobs"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record["id"], job_id.as_str());
    assert_eq!(record["cause"], "rejected");
    assert_eq!(record["last_reason"], "boom");
    assert_eq!(record["attempt"], json!(1));
    let key_b64 = record["key_b64"].as_str().unwrap().to_string();

    // The detail route resolves the same record with its full payload.
    let detail_path = format!(
        "/admin/api/v1/queues/adm-dlq-q/dead-letters/{}",
        url_encode(&key_b64)
    );
    let resp = http(port, "GET", &detail_path, &[], None).await;
    assert_eq!(resp.status, 200, "dead-letter detail failed: {}", resp.body);
    let detail = resp.json();
    assert_eq!(
        B64.decode(detail["payload"]["data_b64"].as_str().unwrap())
            .unwrap(),
        b"replay-me"
    );

    // Requeue moves it back to ready with the attempt reset.
    let body = json!({ "keys_b64": [key_b64] }).to_string();
    let resp = http(
        port,
        "POST",
        "/admin/api/v1/queues/adm-dlq-q/dead-letters:requeue",
        &[],
        Some(&body),
    )
    .await;
    assert_eq!(resp.status, 200, "requeue failed: {}", resp.body);
    let outcome = resp.json();
    assert_eq!(outcome["requeued"], json!(1));
    assert_eq!(outcome["missing"], json!(0));

    let page = http(port, "GET", &jobs_path("dead_letter"), &[], None)
        .await
        .json();
    assert!(page["jobs"].as_array().unwrap().is_empty());
    let page = http(port, "GET", &jobs_path("ready"), &[], None)
        .await
        .json();
    let jobs = page["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["id"], job_id.as_str());
    assert_eq!(jobs[0]["attempt"], json!(1), "requeue resets the attempt");

    // The requeued job is genuinely runnable with its payload intact.
    let job = reserve(&client, "adm-dlq-q", LEASE, WAIT)
        .await
        .expect("requeued job is reservable");
    assert_eq!(job.id, job_id);
    assert_eq!(job.attempt, 1);
    assert_eq!(
        job.payload.as_ref().map(|p| p.data.as_slice()),
        Some(&b"replay-me"[..])
    );

    // Dead-letter it again and bulk-delete it.
    assert!(nack_dead_letter(&client, &job, "again").await);
    let page = http(port, "GET", &jobs_path("dead_letter"), &[], None)
        .await
        .json();
    let key_b64 = page["jobs"][0]["key_b64"].as_str().unwrap().to_string();
    let body = json!({ "keys_b64": [key_b64] }).to_string();
    let resp = http(
        port,
        "POST",
        "/admin/api/v1/queues/adm-dlq-q/dead-letters:delete",
        &[],
        Some(&body),
    )
    .await;
    assert_eq!(resp.status, 200);
    let outcome = resp.json();
    assert_eq!(outcome["deleted"], json!(1));
    assert_eq!(outcome["missing"], json!(0));

    // Deleting the same key again counts as missing, not an error.
    let resp = http(
        port,
        "POST",
        "/admin/api/v1/queues/adm-dlq-q/dead-letters:delete",
        &[],
        Some(&body),
    )
    .await;
    assert_eq!(resp.status, 200);
    let outcome = resp.json();
    assert_eq!(outcome["deleted"], json!(0));
    assert_eq!(outcome["missing"], json!(1));

    let page = http(port, "GET", &jobs_path("dead_letter"), &[], None)
        .await
        .json();
    assert!(page["jobs"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn admin_dead_letters_ready_and_scheduled_jobs() {
    let cfg = format!("{ADMIN_CFG}\n[storage]\ndead_letter_retention_ms = 600000\n");
    let (_guard, client, port) = start_admin_server("adl", &cfg, &[]).await;
    let jobs_path = |state: &str| format!("/admin/api/v1/queues/adm-adl-q/jobs?state={state}");

    enqueue(
        &client,
        EnqueueRequest {
            payload: Some(Payload {
                data: b"poison".to_vec(),
                encoding: "text/plain".to_string(),
            }),
            ..enqueue_req("adm-adl-q")
        },
    )
    .await;
    enqueue(&client, enqueue_req("adm-adl-q")).await;
    enqueue(
        &client,
        EnqueueRequest {
            scheduled_at: Some(ts_ms(epoch_ms_in(Duration::from_secs(3600)))),
            ..enqueue_req("adm-adl-q")
        },
    )
    .await;

    // Dead-letter one ready job; a junk key counts as missing, not an error.
    let page = http(port, "GET", &jobs_path("ready"), &[], None)
        .await
        .json();
    let ready = page["jobs"].as_array().unwrap();
    assert_eq!(ready.len(), 2);
    let victim = ready
        .iter()
        .find(|j| {
            j["payload"]["data_b64"]
                .as_str()
                .is_some_and(|d| !d.is_empty())
        })
        .expect("the payload-carrying job is listed");
    let victim_id = victim["id"].as_str().unwrap().to_string();
    let victim_key = victim["key_b64"].as_str().unwrap().to_string();

    let body = json!({
        "state": "ready",
        "keys_b64": [victim_key, B64.encode(b"junk")],
        "reason": "poison payload",
    })
    .to_string();
    let resp = http(
        port,
        "POST",
        "/admin/api/v1/queues/adm-adl-q/jobs:dead-letter",
        &[],
        Some(&body),
    )
    .await;
    assert_eq!(resp.status, 200, "dead-letter failed: {}", resp.body);
    let outcome = resp.json();
    assert_eq!(outcome["dead_lettered"], json!(1));
    assert_eq!(outcome["missing"], json!(1));

    let page = http(port, "GET", &jobs_path("ready"), &[], None)
        .await
        .json();
    assert_eq!(
        page["jobs"].as_array().unwrap().len(),
        1,
        "one ready job remains"
    );

    let page = http(port, "GET", &jobs_path("dead_letter"), &[], None)
        .await
        .json();
    let records = page["jobs"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], victim_id.as_str());
    assert_eq!(records[0]["cause"], "admin");
    assert_eq!(records[0]["last_reason"], "poison payload");

    // The record kept its payload: requeue it and run it like any other job.
    let key_b64 = records[0]["key_b64"].as_str().unwrap().to_string();
    let body = json!({ "keys_b64": [key_b64] }).to_string();
    let resp = http(
        port,
        "POST",
        "/admin/api/v1/queues/adm-adl-q/dead-letters:requeue",
        &[],
        Some(&body),
    )
    .await;
    assert_eq!(resp.status, 200);
    let job = reserve(&client, "adm-adl-q", LEASE, WAIT)
        .await
        .expect("requeued job is reservable");
    let mut seen = vec![job];
    if seen[0].id != victim_id {
        seen.push(
            reserve(&client, "adm-adl-q", LEASE, WAIT)
                .await
                .expect("victim job is reservable"),
        );
    }
    let revived = seen
        .iter()
        .find(|j| j.id == victim_id)
        .expect("the dead-lettered job came back");
    assert_eq!(
        revived.payload.as_ref().map(|p| p.data.as_slice()),
        Some(&b"poison"[..])
    );

    // Scheduled jobs dead-letter the same way.
    let page = http(port, "GET", &jobs_path("scheduled"), &[], None)
        .await
        .json();
    let key = page["jobs"][0]["key_b64"].as_str().unwrap().to_string();
    let body = json!({ "state": "scheduled", "keys_b64": [key] }).to_string();
    let resp = http(
        port,
        "POST",
        "/admin/api/v1/queues/adm-adl-q/jobs:dead-letter",
        &[],
        Some(&body),
    )
    .await;
    assert_eq!(
        resp.status, 200,
        "scheduled dead-letter failed: {}",
        resp.body
    );
    assert_eq!(resp.json()["dead_lettered"], json!(1));
    let page = http(port, "GET", &jobs_path("scheduled"), &[], None)
        .await
        .json();
    assert!(page["jobs"].as_array().unwrap().is_empty());

    // In-flight jobs are refused outright.
    let body = json!({ "state": "inflight", "keys_b64": [] }).to_string();
    let resp = http(
        port,
        "POST",
        "/admin/api/v1/queues/adm-adl-q/jobs:dead-letter",
        &[],
        Some(&body),
    )
    .await;
    assert_eq!(resp.status, 400);
}

// ---------------------------------------------------------------------------
// 5. Queue creation, deletion + purge
// ---------------------------------------------------------------------------

#[tokio::test]
async fn put_queue_with_no_overrides_declares_it() {
    let (guard, _client, port) = start_admin_server("addq", ADMIN_CFG, &[]).await;

    let config = http(port, "GET", "/admin/api/v1/config", &[], None)
        .await
        .json();
    let etag = config["etag"].as_str().unwrap().to_string();

    let body = json!({ "etag": etag, "overrides": {} }).to_string();
    let resp = http(
        port,
        "PUT",
        "/admin/api/v1/queues/adm-new-q",
        &[],
        Some(&body),
    )
    .await;
    assert_eq!(resp.status, 200, "queue create failed: {}", resp.body);
    let put = resp.json();
    assert_eq!(put["applied"], json!(true));
    assert!(put["requires_restart"].as_array().unwrap().is_empty());

    let on_disk = std::fs::read_to_string(&guard.cfg_path).expect("config file still exists");
    assert!(
        on_disk.contains("name = \"adm-new-q\""),
        "the declaration landed in the file:\n{on_disk}"
    );

    // The watcher applied the write before the PUT returned, so the queue is
    // already declared in the running registry.
    let resp = http(port, "GET", "/admin/api/v1/queues/adm-new-q", &[], None).await;
    assert_eq!(resp.status, 200);
    assert_eq!(resp.json()["declared"], json!(true));

    // An invalid name never lands on disk: config validation rejects it.
    let etag = put["etag"].as_str().unwrap().to_string();
    let long_name = "q".repeat(600);
    let body = json!({ "etag": etag, "overrides": {} }).to_string();
    let resp = http(
        port,
        "PUT",
        &format!("/admin/api/v1/queues/{long_name}"),
        &[],
        Some(&body),
    )
    .await;
    assert_eq!(
        resp.status, 422,
        "an over-long name is rejected: {}",
        resp.body
    );
}

#[tokio::test]
async fn delete_queue_guards_then_purges_and_removes_declaration() {
    let cfg = format!(
        "{ADMIN_CFG}\n[storage]\ndead_letter_retention_ms = 600000\n\n\
         [[queues]]\nname = \"adm-del-q\"\nmax_payload_bytes = 2048\n"
    );
    let (guard, client, port) = start_admin_server("delq", &cfg, &[]).await;

    for _ in 0..4 {
        enqueue(&client, enqueue_req("adm-del-q")).await;
    }
    enqueue(
        &client,
        EnqueueRequest {
            scheduled_at: Some(ts_ms(epoch_ms_in(Duration::from_secs(3600)))),
            ..enqueue_req("adm-del-q")
        },
    )
    .await;
    enqueue(&client, enqueue_req("adm-del-q")).await;
    let doomed = reserve(&client, "adm-del-q", LEASE, WAIT)
        .await
        .expect("job reservable for dead-lettering");
    assert!(nack_dead_letter(&client, &doomed, "dead").await);

    let config = http(port, "GET", "/admin/api/v1/config", &[], None)
        .await
        .json();
    let etag = config["etag"].as_str().unwrap().to_string();

    // If-Match is mandatory.
    let resp = http(
        port,
        "DELETE",
        "/admin/api/v1/queues/adm-del-q?purge=true",
        &[],
        None,
    )
    .await;
    assert_eq!(resp.status, 400);
    assert_eq!(resp.json()["code"], "if_match_required");

    // A populated queue without purge=true is refused.
    let resp = http(
        port,
        "DELETE",
        "/admin/api/v1/queues/adm-del-q",
        &[("If-Match", etag.as_str())],
        None,
    )
    .await;
    assert_eq!(resp.status, 409);
    assert_eq!(resp.json()["code"], "not_empty");

    // An in-flight job blocks deletion outright.
    let held = reserve(&client, "adm-del-q", LEASE, WAIT)
        .await
        .expect("job reservable to hold in-flight");
    let resp = http(
        port,
        "DELETE",
        "/admin/api/v1/queues/adm-del-q?purge=true",
        &[("If-Match", etag.as_str())],
        None,
    )
    .await;
    assert_eq!(resp.status, 409);
    assert_eq!(resp.json()["code"], "inflight");
    ack(&client, &held).await;

    // purge=true deletes the jobs and the [[queues]] declaration.
    let resp = http(
        port,
        "DELETE",
        "/admin/api/v1/queues/adm-del-q?purge=true",
        &[("If-Match", etag.as_str())],
        None,
    )
    .await;
    assert_eq!(resp.status, 200, "delete failed: {}", resp.body);
    let outcome = resp.json();
    // 4 ready - 1 acked + 1 scheduled + 1 dead letter.
    assert_eq!(outcome["purged"], json!(5));
    assert_ne!(
        outcome["etag"].as_str().unwrap(),
        etag,
        "removing the declaration rewrote the config file"
    );

    let on_disk = std::fs::read_to_string(&guard.cfg_path).expect("config file still exists");
    assert!(
        !on_disk.contains("adm-del-q"),
        "the [[queues]] declaration is gone from the file:\n{on_disk}"
    );

    for state in ["ready", "scheduled", "inflight", "dead_letter"] {
        let page = http(
            port,
            "GET",
            &format!("/admin/api/v1/queues/adm-del-q/jobs?state={state}"),
            &[],
            None,
        )
        .await
        .json();
        assert!(
            page["jobs"].as_array().unwrap().is_empty(),
            "{state} still holds jobs after the purge"
        );
    }

    // The queue leaves the stats frames promptly: depths and admin totals are
    // dropped by the purge, and the declaration leaves the registry on reload.
    // Without the immediate totals eviction this would linger for 15 minutes.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let overview = http(port, "GET", "/admin/api/v1/overview", &[], None)
            .await
            .json();
        if overview["frame"]["queues"]
            .as_object()
            .is_some_and(|q| !q.contains_key("adm-del-q"))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the deleted queue never left the stats frame: {overview}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

// ---------------------------------------------------------------------------
// 6. Config editor
// ---------------------------------------------------------------------------

#[tokio::test]
async fn config_editor_applies_validates_and_guards() {
    let (_guard, _client, port) = start_admin_server("cfg", ADMIN_CFG, &[]).await;

    let config = http(port, "GET", "/admin/api/v1/config", &[], None)
        .await
        .json();
    let etag = config["etag"].as_str().unwrap().to_string();
    assert_eq!(etag.len(), 64, "the etag is a sha-256 hex digest");
    assert_eq!(config["effective"]["limits"]["default_priority"], json!(0));
    // The harness boots with SEPP_SERVER__LISTEN_ADDR set, so that path is
    // env-pinned in this process.
    let pinned = config["env_pinned"].to_string();
    assert!(
        pinned.contains("server.listen_addr"),
        "env_pinned: {pinned}"
    );
    let restart_only = config["restart_only"].to_string();
    assert!(restart_only.contains("admin.listen_addr"));
    assert!(
        config["pending_restart"].as_array().unwrap().is_empty(),
        "nothing pends a restart at boot: {config}"
    );

    // A hot-reloadable change applies live.
    let body = json!({
        "etag": etag,
        "changes": [{ "path": "limits.default_priority", "value": 3 }],
    })
    .to_string();
    let resp = http(port, "PUT", "/admin/api/v1/config", &[], Some(&body)).await;
    assert_eq!(resp.status, 200, "put config failed: {}", resp.body);
    let put = resp.json();
    assert_eq!(put["applied"], json!(true));
    assert!(put["requires_restart"].as_array().unwrap().is_empty());
    assert_ne!(put["etag"].as_str().unwrap(), etag);

    // The change is observable in the running config. The watcher applied it
    // before `applied` was reported; poll briefly to absorb scheduler jitter.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let config = http(port, "GET", "/admin/api/v1/config", &[], None)
            .await
            .json();
        if config["effective"]["limits"]["default_priority"] == json!(3) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "default_priority never became effective: {config}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The pre-write etag is now stale and refused.
    let body = json!({
        "etag": etag,
        "changes": [{ "path": "limits.default_priority", "value": 4 }],
    })
    .to_string();
    let resp = http(port, "PUT", "/admin/api/v1/config", &[], Some(&body)).await;
    assert_eq!(resp.status, 412);
    assert_eq!(resp.json()["code"], "etag_mismatch");

    // A restart-only field is written but flagged.
    let config = http(port, "GET", "/admin/api/v1/config", &[], None)
        .await
        .json();
    let etag = config["etag"].as_str().unwrap().to_string();
    let max_msg = config["effective"]["limits"]["max_message_bytes"]
        .as_u64()
        .unwrap();
    let body = json!({
        "etag": etag,
        "changes": [{ "path": "limits.max_message_bytes", "value": max_msg + 1 }],
    })
    .to_string();
    let resp = http(port, "PUT", "/admin/api/v1/config", &[], Some(&body)).await;
    assert_eq!(resp.status, 200, "restart-only put failed: {}", resp.body);
    let put = resp.json();
    assert_eq!(put["applied"], json!(true));
    assert_eq!(put["requires_restart"], json!(["limits.max_message_bytes"]));
    let etag = put["etag"].as_str().unwrap().to_string();

    // The drift between the running (boot) config and the file is reported
    // until a restart applies it.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let config = http(port, "GET", "/admin/api/v1/config", &[], None)
            .await
            .json();
        if config["pending_restart"] == json!(["limits.max_message_bytes"]) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "pending_restart never reported the drift: {config}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Env-pinned paths are rejected outright.
    let body = json!({
        "etag": etag,
        "changes": [{ "path": "server.listen_addr", "value": "127.0.0.1:1" }],
    })
    .to_string();
    let resp = http(port, "PUT", "/admin/api/v1/config", &[], Some(&body)).await;
    assert_eq!(resp.status, 400);
    assert_eq!(resp.json()["code"], "env_pinned");

    // A change that fails validation (priority > 9) never lands on disk.
    let body = json!({
        "etag": etag,
        "changes": [{ "path": "limits.default_priority", "value": 99 }],
    })
    .to_string();
    let resp = http(port, "PUT", "/admin/api/v1/config", &[], Some(&body)).await;
    assert_eq!(resp.status, 422);
    let config = http(port, "GET", "/admin/api/v1/config", &[], None)
        .await
        .json();
    assert_eq!(
        config["etag"].as_str().unwrap(),
        etag,
        "a rejected change leaves the file untouched"
    );
}

// ---------------------------------------------------------------------------
// 7. SSE smoke
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sse_streams_hello_then_stats_frames() {
    let (_guard, client, port) = start_admin_server("sse", ADMIN_CFG, &[]).await;

    let mut sse = sse_connect(port).await;
    sse.wait_for(Instant::now() + Duration::from_secs(10), |body| {
        body.contains("event: hello")
    })
    .await;

    // The hello carries the seed frame and the rate history.
    let hello_data = sse
        .body
        .lines()
        .skip_while(|l| l.trim() != "event: hello")
        .find_map(|l| l.strip_prefix("data: "))
        .expect("the hello event carries data");
    let hello: Value = serde_json::from_str(hello_data).expect("hello data is JSON");
    assert!(hello["frame"].is_object());
    assert!(hello["history"].is_object());

    enqueue(&client, enqueue_req("adm-sse-q")).await;

    // A subsequent stats frame must reflect the enqueue: depth and totals
    // both, since they publish in the same snapshot.
    let reflects_enqueue =
        |frame: &Value| frame["queues"]["adm-sse-q"]["totals"]["enqueued"].as_u64() == Some(1);
    sse.wait_for(Instant::now() + Duration::from_secs(15), |body| {
        stats_frames(body).iter().any(reflects_enqueue)
    })
    .await;

    let frame = stats_frames(&sse.body)
        .into_iter()
        .find(reflects_enqueue)
        .expect("matched by wait_for");
    assert_eq!(frame["queues"]["adm-sse-q"]["ready"], json!(1));
    assert!(frame["seq"].as_u64().unwrap() > 0);
    assert!(frame["ts_ms"].as_i64().unwrap() > 0);
    assert!(frame["server"]["command_queue_len"].is_number());
}
