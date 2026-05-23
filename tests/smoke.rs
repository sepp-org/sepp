//! End-to-end smoke tests that drive a real server process over gRPC, using
//! the generated protobuf client directly (no SDK wrapper). Each test owns its
//! own server process, port and database, so they run independently.

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime};

use sepp::pb::sepp::v1::{
    AckRequest, EnqueueBatchRequest, EnqueueRequest, EnqueueResponse, ExtendRequest,
    GetServerInfoRequest, Job, NackRequest, NackRetry, Payload, PrimitiveValue, ReserveRequest,
    enqueue_atomic_response, job_rejection, job_result, nack_retry, primitive_value,
    queue_service_client::QueueServiceClient,
};
use tonic::transport::Channel;

type Client = QueueServiceClient<Channel>;

/// How a nacked job should be retried, mirroring the proto `NackRetry` oneof.
enum Retry {
    Default,
    After(Duration),
    DeadLetter,
}

struct ServerGuard {
    child: Child,
    db_path: Option<std::path::PathBuf>,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(path) = &self.db_path {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn temp_db(tag: &str) -> std::path::PathBuf {
    let unique = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("sepp-smoke-{tag}-{unique}"))
}

async fn spawn_server(db_path: &std::path::Path) -> (Child, Client) {
    spawn_server_with_env(db_path, &[]).await
}

async fn spawn_server_with_env(
    db_path: &std::path::Path,
    extra_env: &[(&str, &str)],
) -> (Child, Client) {
    let port = free_port();
    let mut command = Command::new(env!("CARGO_BIN_EXE_sepp"));
    command
        .env("SEPP_SERVER__LISTEN_ADDR", format!("127.0.0.1:{port}"))
        .env("SEPP_SERVER__DB_PATH", db_path)
        .stdout(Stdio::null());
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let mut child = command.spawn().expect("spawn sepp server");

    let addr = format!("http://127.0.0.1:{port}");
    for _ in 0..100 {
        if let Ok(client) = QueueServiceClient::connect(addr.clone()).await {
            return (child, client);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("server did not become reachable on {addr}");
}

/// Spawns a fresh server with a throwaway database and returns a guard that
/// kills the process and deletes the database when dropped.
async fn start_server(tag: &str) -> (ServerGuard, Client) {
    let db_path = temp_db(tag);
    let (child, client) = spawn_server(&db_path).await;
    (
        ServerGuard {
            child,
            db_path: Some(db_path),
        },
        client,
    )
}

fn temp_config_path(tag: &str) -> std::path::PathBuf {
    let unique = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("sepp-smoke-{tag}-{unique}.toml"))
}

async fn start_server_with_config(tag: &str, toml: &str) -> (ServerGuard, Client) {
    let db_path = temp_db(tag);
    let cfg_path = temp_config_path(tag);
    std::fs::write(&cfg_path, toml).expect("write temp config");
    let cfg_str = cfg_path.to_str().expect("utf-8 config path");
    let (child, client) = spawn_server_with_env(&db_path, &[("SEPP_CONFIG", cfg_str)]).await;
    let _ = std::fs::remove_file(&cfg_path);
    (
        ServerGuard {
            child,
            db_path: Some(db_path),
        },
        client,
    )
}

fn enqueue_req(queue: &str) -> EnqueueRequest {
    EnqueueRequest {
        queue: queue.to_string(),
        job_type: "smoke-job".to_string(),
        ..Default::default()
    }
}

/// Enqueues a single job through `EnqueueBatch` and unwraps its success result.
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

async fn reserve_batch(
    client: &Client,
    queues: &[&str],
    lease: Duration,
    wait: Duration,
    max_jobs: Option<u32>,
) -> Vec<Job> {
    client
        .clone()
        .reserve(ReserveRequest {
            queues: queues.iter().map(|q| q.to_string()).collect(),
            wait_timeout_ms: wait.as_millis() as u64,
            lease_duration_ms: lease.as_millis() as u64,
            worker_id: None,
            max_jobs,
        })
        .await
        .expect("reserve RPC")
        .into_inner()
        .jobs
}

async fn reserve(client: &Client, queue: &str, lease: Duration, wait: Duration) -> Option<Job> {
    reserve_batch(client, &[queue], lease, wait, None)
        .await
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

/// Nacks a job and returns whether the server dead-lettered it.
async fn nack(client: &Client, job: &Job, retry: Retry, reason: &str) -> bool {
    let strategy = match retry {
        Retry::Default => nack_retry::Strategy::Default(()),
        Retry::After(delay) => nack_retry::Strategy::DelayMs(delay.as_millis() as u64),
        Retry::DeadLetter => nack_retry::Strategy::DeadLetter(()),
    };
    client
        .clone()
        .nack(NackRequest {
            job_id: job.id.clone(),
            attempt: job.attempt,
            reason: Some(reason.to_string()),
            retry: Some(NackRetry {
                strategy: Some(strategy),
            }),
            worker_id: None,
        })
        .await
        .expect("nack RPC")
        .into_inner()
        .dead_lettered
}

async fn extend(client: &Client, job: &Job, lease: Duration) {
    client
        .clone()
        .extend(ExtendRequest {
            job_id: job.id.clone(),
            attempt: job.attempt,
            lease_duration_ms: lease.as_millis() as u64,
            worker_id: None,
        })
        .await
        .expect("extend RPC");
}

fn prim_str(value: &str) -> PrimitiveValue {
    PrimitiveValue {
        value: Some(primitive_value::Value::StringValue(value.to_string())),
    }
}

fn prim_int(value: i64) -> PrimitiveValue {
    PrimitiveValue {
        value: Some(primitive_value::Value::IntValue(value)),
    }
}

fn epoch_ms_in(d: Duration) -> i64 {
    (SystemTime::now() + d)
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

const LEASE: Duration = Duration::from_secs(30);
const WAIT: Duration = Duration::from_secs(5);
const NO_WAIT: Duration = Duration::from_millis(200);

#[tokio::test]
async fn server_advertises_its_capabilities() {
    let (_guard, client) = start_server("info").await;

    let info = client
        .clone()
        .get_server_info(GetServerInfoRequest {})
        .await
        .expect("get_server_info RPC")
        .into_inner();

    assert!(!info.server_version.is_empty(), "server reports a version");
    assert!(
        info.max_payload_bytes > 0,
        "server advertises a payload limit"
    );
    // No `allowed_encodings` is configured, so the server restricts nothing.
    assert!(
        !info.restricts_encodings,
        "an unconfigured server does not restrict encodings"
    );
    assert!(info.allowed_encodings.is_empty());
}

#[tokio::test]
async fn enqueue_reserve_ack_roundtrip() {
    let (_guard, client) = start_server("basic").await;

    let ack_resp = enqueue(
        &client,
        EnqueueRequest {
            payload: Some(Payload {
                data: b"hello".to_vec(),
                encoding: "text/plain".to_string(),
            }),
            ..enqueue_req("smoke-basic")
        },
    )
    .await;
    assert!(!ack_resp.deduplicated);

    let job = reserve(&client, "smoke-basic", LEASE, WAIT)
        .await
        .expect("basic job is reservable");
    assert_eq!(job.id, ack_resp.job_id);
    assert_eq!(job.attempt, 1);
    assert_eq!(
        job.payload.as_ref().map(|p| p.data.as_slice()),
        Some(&b"hello"[..]),
    );

    ack(&client, &job).await;

    let gone = reserve(&client, "smoke-basic", LEASE, NO_WAIT).await;
    assert!(gone.is_none(), "acked job must not be redelivered");
}

#[tokio::test]
async fn idempotency_key_deduplicates_enqueue() {
    let (_guard, client) = start_server("dedup").await;

    let first = enqueue(
        &client,
        EnqueueRequest {
            idempotency_key: Some("k1".to_string()),
            ..enqueue_req("smoke-dedup")
        },
    )
    .await;
    assert!(!first.deduplicated);

    let second = enqueue(
        &client,
        EnqueueRequest {
            idempotency_key: Some("k1".to_string()),
            ..enqueue_req("smoke-dedup")
        },
    )
    .await;
    assert!(
        second.deduplicated,
        "second enqueue with the same key is deduplicated"
    );
    assert_eq!(second.job_id, first.job_id);
}

#[tokio::test]
async fn nack_redelivers_and_increments_attempt() {
    let (_guard, client) = start_server("nack").await;

    enqueue(&client, enqueue_req("smoke-nack")).await;
    let job = reserve(&client, "smoke-nack", LEASE, WAIT)
        .await
        .expect("nack job reservable");
    assert_eq!(job.attempt, 1);

    let dead = nack(&client, &job, Retry::Default, "smoke retry").await;
    assert!(!dead, "first nack should not dead-letter");

    let retried = reserve(&client, "smoke-nack", LEASE, WAIT)
        .await
        .expect("nacked job is redelivered");
    assert_eq!(retried.attempt, 2);
    ack(&client, &retried).await;
}

#[tokio::test]
async fn delayed_nack_defers_redelivery() {
    let (_guard, client) = start_server("delay").await;

    enqueue(&client, enqueue_req("smoke-delay")).await;
    let job = reserve(&client, "smoke-delay", LEASE, WAIT)
        .await
        .expect("delay job reservable");
    assert_eq!(job.attempt, 1);

    let dead = nack(
        &client,
        &job,
        Retry::After(Duration::from_secs(3)),
        "retry later",
    )
    .await;
    assert!(!dead, "a delayed nack retries, it does not dead-letter");

    let too_early = reserve(&client, "smoke-delay", LEASE, NO_WAIT).await;
    assert!(
        too_early.is_none(),
        "a delayed-retry job is not reservable before its delay elapses"
    );

    let retried = reserve(&client, "smoke-delay", LEASE, Duration::from_secs(15))
        .await
        .expect("a delayed-retry job becomes reservable after its delay");
    assert_eq!(
        retried.attempt, 2,
        "the delayed retry still increments the attempt"
    );
    ack(&client, &retried).await;
}

#[tokio::test]
async fn scheduled_job_waits_for_its_time() {
    let (_guard, client) = start_server("sched").await;

    enqueue(
        &client,
        EnqueueRequest {
            scheduled_at: Some(epoch_ms_in(Duration::from_secs(3))),
            ..enqueue_req("smoke-sched")
        },
    )
    .await;

    let early = reserve(&client, "smoke-sched", LEASE, NO_WAIT).await;
    assert!(
        early.is_none(),
        "scheduled job must not be reservable before its time"
    );

    let promoted = reserve(&client, "smoke-sched", LEASE, Duration::from_secs(15))
        .await
        .expect("scheduled job becomes reservable after its schedule");
    ack(&client, &promoted).await;
}

#[tokio::test]
async fn expired_lease_redelivers_job() {
    let (_guard, client) = start_server("lease").await;

    enqueue(&client, enqueue_req("smoke-lease")).await;
    let leased = reserve(&client, "smoke-lease", Duration::from_secs(1), WAIT)
        .await
        .expect("lease job reservable");
    assert_eq!(leased.attempt, 1);

    let held = reserve(&client, "smoke-lease", Duration::from_secs(1), NO_WAIT).await;
    assert!(held.is_none(), "leased job must not be reservable");

    let redelivered = reserve(&client, "smoke-lease", LEASE, Duration::from_secs(15))
        .await
        .expect("expired lease is redelivered");
    assert_eq!(redelivered.attempt, 2, "redelivery increments the attempt");
    ack(&client, &redelivered).await;
}

#[tokio::test]
async fn extend_keeps_lease_alive() {
    let (_guard, client) = start_server("extend").await;

    enqueue(&client, enqueue_req("smoke-extend")).await;
    let leased = reserve(&client, "smoke-extend", Duration::from_secs(1), WAIT)
        .await
        .expect("extend job reservable");
    assert_eq!(leased.attempt, 1);

    extend(&client, &leased, LEASE).await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let still_held = reserve(&client, "smoke-extend", Duration::from_secs(1), NO_WAIT).await;
    assert!(
        still_held.is_none(),
        "an extended lease keeps the job from being redelivered past its original deadline"
    );
    ack(&client, &leased).await;
}

#[tokio::test]
async fn batch_enqueue_accepts_every_job() {
    let (_guard, client) = start_server("batch").await;

    let results = client
        .clone()
        .enqueue_batch(EnqueueBatchRequest {
            jobs: vec![
                enqueue_req("smoke-batch"),
                enqueue_req("smoke-batch"),
                enqueue_req("smoke-batch"),
            ],
        })
        .await
        .expect("enqueue_batch RPC")
        .into_inner()
        .results;

    assert_eq!(
        results.len(),
        3,
        "the batch returns one result per submitted job"
    );
    assert!(
        results
            .iter()
            .all(|r| matches!(r.outcome, Some(job_result::Outcome::Success(_)))),
        "every job in the batch is accepted"
    );

    for _ in 0..3 {
        let job = reserve(&client, "smoke-batch", LEASE, WAIT)
            .await
            .expect("each batched job is reservable");
        ack(&client, &job).await;
    }
    let drained = reserve(&client, "smoke-batch", LEASE, NO_WAIT).await;
    assert!(drained.is_none(), "the batch holds exactly three jobs");
}

#[tokio::test]
async fn higher_priority_is_reserved_first() {
    let (_guard, client) = start_server("prio").await;

    enqueue(
        &client,
        EnqueueRequest {
            priority: Some(1),
            ..enqueue_req("smoke-prio")
        },
    )
    .await;
    let high = enqueue(
        &client,
        EnqueueRequest {
            priority: Some(9),
            ..enqueue_req("smoke-prio")
        },
    )
    .await;

    let first = reserve(&client, "smoke-prio", LEASE, WAIT)
        .await
        .expect("priority job reservable");
    assert_eq!(
        first.id, high.job_id,
        "the higher-priority job is reserved before the lower one, regardless of enqueue order"
    );
    ack(&client, &first).await;

    let second = reserve(&client, "smoke-prio", LEASE, WAIT)
        .await
        .expect("lower-priority job reservable");
    ack(&client, &second).await;
}

#[tokio::test]
async fn jobs_of_equal_priority_are_fifo() {
    let (_guard, client) = start_server("fifo").await;

    let early = enqueue(&client, enqueue_req("smoke-fifo")).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let late = enqueue(&client, enqueue_req("smoke-fifo")).await;

    let r1 = reserve(&client, "smoke-fifo", LEASE, WAIT)
        .await
        .expect("first fifo job reservable");
    assert_eq!(
        r1.id, early.job_id,
        "within one priority the earlier-enqueued job is reserved first"
    );
    let r2 = reserve(&client, "smoke-fifo", LEASE, WAIT)
        .await
        .expect("second fifo job reservable");
    assert_eq!(r2.id, late.job_id);

    ack(&client, &r1).await;
    ack(&client, &r2).await;
}

#[tokio::test]
async fn job_dead_letters_when_attempts_are_exhausted() {
    let (_guard, client) = start_server("dlq").await;

    enqueue(
        &client,
        EnqueueRequest {
            max_attempts: Some(2),
            ..enqueue_req("smoke-dlq")
        },
    )
    .await;

    let a1 = reserve(&client, "smoke-dlq", LEASE, WAIT)
        .await
        .expect("dlq job attempt 1 reservable");
    assert_eq!(a1.attempt, 1);
    let dead1 = nack(&client, &a1, Retry::Default, "fail 1").await;
    assert!(!dead1, "attempt 1 of 2 is retried, not dead-lettered");

    let a2 = reserve(&client, "smoke-dlq", LEASE, WAIT)
        .await
        .expect("dlq job attempt 2 reservable");
    assert_eq!(a2.attempt, 2);
    let dead2 = nack(&client, &a2, Retry::Default, "fail 2").await;
    assert!(
        dead2,
        "nacking the final attempt exhausts max_attempts and dead-letters the job"
    );

    let gone = reserve(&client, "smoke-dlq", LEASE, NO_WAIT).await;
    assert!(gone.is_none(), "a dead-lettered job is not redelivered");
}

#[tokio::test]
async fn dead_letter_directive_drops_job_immediately() {
    let (_guard, client) = start_server("dlq-force").await;

    enqueue(&client, enqueue_req("smoke-dlq-force")).await;
    let job = reserve(&client, "smoke-dlq-force", LEASE, WAIT)
        .await
        .expect("force-dlq job reservable");
    assert_eq!(job.attempt, 1);

    let dead = nack(&client, &job, Retry::DeadLetter, "drop it").await;
    assert!(
        dead,
        "a DeadLetter nack dead-letters immediately, before attempts are exhausted"
    );

    let gone = reserve(&client, "smoke-dlq-force", LEASE, NO_WAIT).await;
    assert!(
        gone.is_none(),
        "a force-dead-lettered job is not redelivered"
    );
}

#[tokio::test]
async fn reserve_spans_multiple_queues() {
    let (_guard, client) = start_server("mq").await;

    let mq_job = enqueue(&client, enqueue_req("smoke-mq-b")).await;

    let claimed = reserve_batch(&client, &["smoke-mq-a", "smoke-mq-b"], LEASE, WAIT, None)
        .await
        .into_iter()
        .next()
        .expect("a reserve over several queues claims from whichever has work");
    assert_eq!(
        claimed.id, mq_job.job_id,
        "the multi-queue reserve claims the job from the non-empty queue"
    );
    ack(&client, &claimed).await;
}

#[tokio::test]
async fn batch_reserve_returns_up_to_max_jobs() {
    let (_guard, client) = start_server("rbatch").await;

    for _ in 0..5 {
        enqueue(&client, enqueue_req("smoke-rbatch")).await;
    }

    let first_batch = reserve_batch(&client, &["smoke-rbatch"], LEASE, WAIT, Some(3)).await;
    assert_eq!(
        first_batch.len(),
        3,
        "a batch reserve claims at most max_jobs in one call"
    );
    for job in &first_batch {
        ack(&client, job).await;
    }

    let rest = reserve_batch(&client, &["smoke-rbatch"], LEASE, WAIT, Some(3)).await;
    assert_eq!(
        rest.len(),
        2,
        "a batch reserve returns whatever is available, up to max_jobs"
    );
    for job in &rest {
        ack(&client, job).await;
    }
}

#[tokio::test]
async fn payload_and_custom_fields_roundtrip() {
    let (_guard, client) = start_server("custom").await;

    let mut custom = HashMap::new();
    custom.insert("region".to_string(), prim_str("eu-west"));
    custom.insert("retries".to_string(), prim_int(7));

    enqueue(
        &client,
        EnqueueRequest {
            payload: Some(Payload {
                data: b"{}".to_vec(),
                encoding: "application/json".to_string(),
            }),
            custom,
            ..enqueue_req("smoke-custom")
        },
    )
    .await;

    let job = reserve(&client, "smoke-custom", LEASE, WAIT)
        .await
        .expect("custom job reservable");
    let payload = job.payload.as_ref().expect("payload round-trips");
    assert_eq!(
        payload.encoding, "application/json",
        "payload encoding round-trips intact"
    );
    assert_eq!(
        job.custom.get("region"),
        Some(&prim_str("eu-west")),
        "custom string field round-trips intact"
    );
    assert_eq!(
        job.custom.get("retries"),
        Some(&prim_int(7)),
        "custom int field round-trips intact"
    );
    ack(&client, &job).await;
}

/// Wraps a message in a request carrying an `authorization: Bearer <key>`
/// header, the way an authenticated client presents its API key.
fn with_key<T>(msg: T, key: &str) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    req
}

#[tokio::test]
async fn api_key_auth_gates_requests() {
    let db_path = temp_db("auth");
    let (child, client) =
        spawn_server_with_env(&db_path, &[("SEPP_AUTH__API_KEYS", r#"["smoke-secret"]"#)]).await;
    let _guard = ServerGuard {
        child,
        db_path: Some(db_path),
    };

    // No key at all is rejected.
    let status = client
        .clone()
        .get_server_info(GetServerInfoRequest {})
        .await
        .expect_err("a request without an API key is rejected");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    // A key that is not in the configured list is rejected.
    let status = client
        .clone()
        .get_server_info(with_key(GetServerInfoRequest {}, "wrong-key"))
        .await
        .expect_err("a request with an unknown API key is rejected");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    // The configured key is accepted.
    let info = client
        .clone()
        .get_server_info(with_key(GetServerInfoRequest {}, "smoke-secret"))
        .await
        .expect("a request with a valid API key is accepted")
        .into_inner();
    assert!(!info.server_version.is_empty());
}

async fn enqueue_one(client: &Client, req: EnqueueRequest) -> sepp::pb::sepp::v1::JobResult {
    client
        .clone()
        .enqueue_batch(EnqueueBatchRequest { jobs: vec![req] })
        .await
        .expect("enqueue_batch RPC")
        .into_inner()
        .results
        .into_iter()
        .next()
        .expect("one result per submitted job")
}

#[tokio::test]
async fn strict_mode_rejects_undeclared_queues() {
    let cfg = r#"
[server]
strict_queues = true

[[queues]]
name = "smoke-strict-emails"
"#;
    let (_guard, client) = start_server_with_config("strict", cfg).await;

    let ok = enqueue_one(&client, enqueue_req("smoke-strict-emails")).await;
    assert!(
        matches!(ok.outcome, Some(job_result::Outcome::Success(_))),
        "declared queue accepts enqueue: {:?}",
        ok.outcome
    );

    let bad = enqueue_one(&client, enqueue_req("smoke-strict-ghost")).await;
    match bad.outcome {
        Some(job_result::Outcome::Rejection(r)) => match r.reason {
            Some(job_rejection::Reason::UnknownQueue(uq)) => {
                assert_eq!(uq.queue, "smoke-strict-ghost");
            }
            other => panic!("expected UnknownQueue rejection, got {other:?}"),
        },
        other => panic!("undeclared queue was not rejected: {other:?}"),
    }

    let status = client
        .clone()
        .reserve(ReserveRequest {
            queues: vec!["smoke-strict-ghost".to_string()],
            wait_timeout_ms: NO_WAIT.as_millis() as u64,
            lease_duration_ms: LEASE.as_millis() as u64,
            worker_id: None,
            max_jobs: None,
        })
        .await
        .expect_err("reserve from undeclared queue is rejected");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn per_queue_max_payload_overrides_global() {
    let cfg = r#"
[[queues]]
name = "smoke-tiny"
max_payload_bytes = 16
"#;
    let (_guard, client) = start_server_with_config("override", cfg).await;

    let oversize = Payload {
        data: vec![0u8; 1024],
        encoding: "application/octet-stream".to_string(),
    };

    let rejected = enqueue_one(
        &client,
        EnqueueRequest {
            payload: Some(oversize.clone()),
            ..enqueue_req("smoke-tiny")
        },
    )
    .await;
    match rejected.outcome {
        Some(job_result::Outcome::Rejection(r)) => match r.reason {
            Some(job_rejection::Reason::PayloadTooLarge(p)) => {
                assert_eq!(p.limit, 16, "the per-queue limit is reported, not the global");
                assert_eq!(p.actual, 1024);
            }
            other => panic!("expected PayloadTooLarge rejection, got {other:?}"),
        },
        other => panic!("per-queue payload cap did not reject: {other:?}"),
    }

    // The same payload still fits the global default for an undeclared queue.
    let accepted = enqueue_one(
        &client,
        EnqueueRequest {
            payload: Some(oversize),
            ..enqueue_req("smoke-untyped")
        },
    )
    .await;
    assert!(
        matches!(accepted.outcome, Some(job_result::Outcome::Success(_))),
        "the same payload is accepted on a queue without an override: {:?}",
        accepted.outcome
    );
}

#[tokio::test]
async fn per_queue_allowed_job_types_filters_enqueues() {
    let cfg = r#"
[[queues]]
name = "smoke-typed"
allowed_job_types = ["send_email"]
"#;
    let (_guard, client) = start_server_with_config("typed", cfg).await;

    let rejected = enqueue_one(
        &client,
        EnqueueRequest {
            job_type: "render_report".to_string(),
            ..enqueue_req("smoke-typed")
        },
    )
    .await;
    match rejected.outcome {
        Some(job_result::Outcome::Rejection(r)) => match r.reason {
            Some(job_rejection::Reason::JobTypeNotAllowed(j)) => {
                assert_eq!(j.job_type, "render_report");
                assert_eq!(j.allowed, vec!["send_email".to_string()]);
            }
            other => panic!("expected JobTypeNotAllowed rejection, got {other:?}"),
        },
        other => panic!("disallowed job_type was not rejected: {other:?}"),
    }

    let accepted = enqueue_one(
        &client,
        EnqueueRequest {
            job_type: "send_email".to_string(),
            ..enqueue_req("smoke-typed")
        },
    )
    .await;
    assert!(
        matches!(accepted.outcome, Some(job_result::Outcome::Success(_))),
        "an allow-listed job_type is accepted: {:?}",
        accepted.outcome
    );

    // The restriction is per-queue: an undeclared queue accepts any job_type.
    let elsewhere = enqueue_one(
        &client,
        EnqueueRequest {
            job_type: "render_report".to_string(),
            ..enqueue_req("smoke-untyped")
        },
    )
    .await;
    assert!(
        matches!(elsewhere.outcome, Some(job_result::Outcome::Success(_))),
        "a queue without an allow-list accepts any job_type: {:?}",
        elsewhere.outcome
    );
}

#[tokio::test]
async fn durability_survives_restart() {
    let db_path = temp_db("restart");

    let job_id = {
        let (child, client) = spawn_server(&db_path).await;
        let _server = ServerGuard {
            child,
            db_path: None,
        };
        enqueue(&client, enqueue_req("smoke-restart")).await.job_id
    };

    let (child, client) = spawn_server(&db_path).await;
    let _server = ServerGuard {
        child,
        db_path: Some(db_path),
    };

    let job = reserve(&client, "smoke-restart", LEASE, WAIT)
        .await
        .expect("a job enqueued before a restart is still reservable after it");
    assert_eq!(job.id, job_id, "the same job survives a server restart");
    assert_eq!(job.attempt, 1);
    ack(&client, &job).await;
}

#[tokio::test]
async fn enqueue_atomic_commits_when_every_job_validates() {
    let (_guard, client) = start_server("atomic-ok").await;

    let response = client
        .clone()
        .enqueue_atomic(EnqueueBatchRequest {
            jobs: vec![
                enqueue_req("atomic-ok-q"),
                enqueue_req("atomic-ok-q"),
                enqueue_req("atomic-ok-q"),
            ],
        })
        .await
        .expect("enqueue_atomic RPC")
        .into_inner();

    let success = match response.outcome {
        Some(enqueue_atomic_response::Outcome::Success(s)) => s,
        other => panic!("expected Success outcome, got {other:?}"),
    };
    assert_eq!(success.responses.len(), 3, "one response per submitted job");
    for r in &success.responses {
        assert!(!r.job_id.is_empty());
        assert!(!r.deduplicated);
    }

    // All three jobs are actually enqueued.
    let reserved = reserve_batch(&client, &["atomic-ok-q"], LEASE, WAIT, Some(10)).await;
    assert_eq!(reserved.len(), 3, "every job in the atomic batch is reservable");
}

#[tokio::test]
async fn enqueue_atomic_rejects_whole_batch_when_any_job_fails_validation() {
    // strict_mode catches an unknown-queue rejection; the per-queue payload
    // cap catches a payload-too-large rejection. The first job is valid —
    // we use it to verify that nothing was enqueued when the batch fails.
    let cfg = r#"
[server]
strict_queues = true

[[queues]]
name = "atomic-good"
max_payload_bytes = 16
"#;
    let (_guard, client) = start_server_with_config("atomic-rej", cfg).await;

    let oversize = Payload {
        data: vec![0u8; 1024],
        encoding: "application/octet-stream".to_string(),
    };

    let response = client
        .clone()
        .enqueue_atomic(EnqueueBatchRequest {
            jobs: vec![
                enqueue_req("atomic-good"), // index 0 — valid
                enqueue_req("atomic-ghost"), // index 1 — unknown queue
                EnqueueRequest {
                    payload: Some(oversize),
                    ..enqueue_req("atomic-good")
                }, // index 2 — payload too large
            ],
        })
        .await
        .expect("enqueue_atomic RPC")
        .into_inner();

    let failure = match response.outcome {
        Some(enqueue_atomic_response::Outcome::Rejection(f)) => f,
        other => panic!("expected Rejection outcome, got {other:?}"),
    };
    assert_eq!(failure.errors.len(), 2, "both invalid jobs are reported");

    // Errors come back in input order with the correct indices.
    let unknown = &failure.errors[0];
    assert_eq!(unknown.index, 1);
    match unknown.rejection.as_ref().and_then(|r| r.reason.as_ref()) {
        Some(job_rejection::Reason::UnknownQueue(uq)) => {
            assert_eq!(uq.queue, "atomic-ghost");
        }
        other => panic!("expected UnknownQueue at index 1, got {other:?}"),
    }

    let oversized = &failure.errors[1];
    assert_eq!(oversized.index, 2);
    match oversized.rejection.as_ref().and_then(|r| r.reason.as_ref()) {
        Some(job_rejection::Reason::PayloadTooLarge(p)) => {
            assert_eq!(p.limit, 16);
            assert_eq!(p.actual, 1024);
        }
        other => panic!("expected PayloadTooLarge at index 2, got {other:?}"),
    }

    // The valid job at index 0 must NOT have been enqueued — that's the
    // whole point of atomic semantics.
    let reserved = reserve_batch(&client, &["atomic-good"], LEASE, NO_WAIT, Some(10)).await;
    assert!(
        reserved.is_empty(),
        "atomic rejection must not enqueue any job; reservable jobs: {reserved:?}"
    );
}
