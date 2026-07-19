//! End-to-end integration tests that drive a real server process over gRPC,
//! using the generated protobuf client directly (no SDK wrapper). Each test
//! owns its own server process, port and database, so they run independently.
//!
//! These exercise the full stack — process startup, config loading, the fjall
//! store, gRPC transport, TLS and auth — so they are intentionally heavier than
//! the in-crate unit tests under `src/`.

mod helpers;
use helpers::*;

use std::collections::BTreeMap;
use std::process::Child;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use sepp::pb::sepp::v1::{
    AckRequest, DeadLetterCause, DeadLetterRecord, DrainDeadLettersRequest, EnqueueBatchRequest,
    EnqueueRequest, ExtendRequest, GetServerInfoRequest, Job, NackRequest, NackRetry, Payload,
    PrimitiveValue, ReserveRequest, enqueue_atomic_response, job_rejection, job_result, nack_retry,
    primitive_value, queue_service_client::QueueServiceClient,
};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

/// How a nacked job should be retried, mirroring the proto `NackRetry` oneof.
enum Retry {
    Default,
    After(Duration),
    DeadLetter,
}

fn temp_db(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("sepp-it-{tag}-{}", unique_suffix()))
}

async fn spawn_server(db_path: &std::path::Path) -> (Child, Client) {
    spawn_server_with_env(db_path, &[]).await
}

async fn spawn_server_with_env(
    db_path: &std::path::Path,
    extra_env: &[(&str, &str)],
) -> (Child, Client) {
    let (mut child, book) = spawn_server_process(db_path, extra_env);
    let Some(port) = wait_for_port(&book, "grpc", STARTUP_TIMEOUT).await else {
        let _ = child.kill();
        let _ = child.wait();
        panic!("server did not report a listening port within 10s");
    };
    (child, connect_client(port).await)
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
            cfg_path: None,
        },
        client,
    )
}

fn temp_config_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("sepp-it-{tag}-{}.toml", unique_suffix()))
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
            cfg_path: None,
        },
        client,
    )
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
            wait_timeout: Some(dur(wait)),
            lease_duration: Some(dur(lease)),
            worker_id: None,
            max_jobs,
        })
        .await
        .expect("reserve RPC")
        .into_inner()
        .jobs
}

/// Nacks a job and returns whether the server dead-lettered it.
async fn nack(client: &Client, job: &Job, retry: Retry, reason: &str) -> bool {
    let strategy = match retry {
        Retry::Default => nack_retry::Strategy::Default(()),
        Retry::After(delay) => nack_retry::Strategy::Delay(dur(delay)),
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

async fn extend(client: &Client, job: &Job, lease: Duration) -> i64 {
    let resp = client
        .clone()
        .extend(ExtendRequest {
            job_id: job.id.clone(),
            attempt: job.attempt,
            lease_duration: Some(dur(lease)),
            worker_id: None,
        })
        .await
        .expect("extend RPC")
        .into_inner();
    ts_to_ms(
        &resp
            .lease_expires_at
            .expect("extend reports a lease expiry"),
    )
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
        !info.supported_protocol_versions.is_empty(),
        "server advertises at least one protocol version"
    );
    assert!(
        ts_to_ms(&info.server_time.expect("server reports a time")) > 0,
        "server reports its wall-clock time for skew detection"
    );
    assert!(
        info.max_payload_bytes > 0,
        "server advertises a payload limit"
    );
    // The custom-map limits a producer needs to size its jobs are all advertised.
    assert!(info.max_custom_entries > 0, "advertises a custom-entry cap");
    assert!(
        info.max_custom_total_bytes > 0,
        "advertises a custom-map byte cap"
    );
    assert!(
        info.max_custom_key_bytes > 0,
        "advertises a custom-key byte cap"
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
async fn default_nack_uses_queue_retry_policy() {
    let toml = r#"
[limits]
retry_delay_ms = 3000
retry_backoff = "exponential"
retry_delay_max_ms = 60000
"#;
    let (_guard, client) = start_server_with_config("retry-policy", toml).await;

    enqueue(&client, enqueue_req("smoke-retry-policy")).await;
    let job = reserve(&client, "smoke-retry-policy", LEASE, WAIT)
        .await
        .expect("policy job reservable");
    assert_eq!(job.attempt, 1);

    // No explicit retry directive: the queue policy defers the redelivery.
    let nacked_at = std::time::Instant::now();
    let dead = nack(&client, &job, Retry::Default, "no directive").await;
    assert!(!dead, "a policy retry does not dead-letter");

    let too_early = reserve(&client, "smoke-retry-policy", LEASE, NO_WAIT).await;
    // The policy delay is 3000ms minus up to 25% jitter; only assert
    // emptiness when the probe ran safely inside that window, so a stalled
    // CI runner cannot flake this.
    if nacked_at.elapsed() < Duration::from_millis(1500) {
        assert!(
            too_early.is_none(),
            "a policy-delayed retry is not reservable before its delay elapses"
        );
    }

    let retried = reserve(&client, "smoke-retry-policy", LEASE, Duration::from_secs(15))
        .await
        .expect("a policy-delayed retry becomes reservable after its delay");
    assert_eq!(retried.attempt, 2);
    ack(&client, &retried).await;
}

#[tokio::test]
async fn scheduled_job_waits_for_its_time() {
    let (_guard, client) = start_server("sched").await;

    enqueue(
        &client,
        EnqueueRequest {
            scheduled_at: Some(ts_ms(epoch_ms_in(Duration::from_secs(3)))),
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

    let before = epoch_ms_in(Duration::ZERO);
    let new_deadline = extend(&client, &leased, LEASE).await;
    assert!(
        new_deadline >= before + 25_000 && new_deadline <= before + 35_000,
        "extend should report a deadline ~{LEASE:?} out, got {}ms from now",
        new_deadline - before,
    );
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

// ---------------------------------------------------------------------------
// Dead-letter retention + drain
// ---------------------------------------------------------------------------

// Retention long enough that a test always drains its records before the
// sweeper could reclaim them.
const RETAIN_CFG: &str = r#"
[storage]
dead_letter_retention_ms = 600000
"#;

async fn drain(client: &Client, queue: Option<&str>, max: u32) -> Vec<DeadLetterRecord> {
    client
        .clone()
        .drain_dead_letters(DrainDeadLettersRequest {
            queue: queue.map(str::to_string),
            max: Some(max),
        })
        .await
        .expect("drain_dead_letters RPC")
        .into_inner()
        .records
}

/// Enqueues a job to `queue`, reserves it, and force-dead-letters it via a
/// `DeadLetter` nack. Returns the job id.
async fn dead_letter_one(client: &Client, queue: &str, reason: &str) -> String {
    let resp = enqueue(client, enqueue_req(queue)).await;
    let job = reserve(client, queue, LEASE, WAIT)
        .await
        .expect("job reservable before dead-lettering");
    assert!(
        nack(client, &job, Retry::DeadLetter, reason).await,
        "a DeadLetter nack dead-letters the job"
    );
    resp.job_id
}

#[tokio::test]
async fn dead_letters_are_retained_and_drained() {
    let (_guard, client) = start_server_with_config("dl-drain", RETAIN_CFG).await;

    // A job that exhausts its attempts via nacks, carrying a payload to replay.
    enqueue(
        &client,
        EnqueueRequest {
            max_attempts: Some(2),
            payload: Some(Payload {
                data: b"to-replay".to_vec(),
                encoding: "text/plain".to_string(),
            }),
            ..enqueue_req("dl-drain-q")
        },
    )
    .await;

    let a1 = reserve(&client, "dl-drain-q", LEASE, WAIT)
        .await
        .expect("attempt 1");
    assert!(!nack(&client, &a1, Retry::Default, "fail 1").await);
    let a2 = reserve(&client, "dl-drain-q", LEASE, WAIT)
        .await
        .expect("attempt 2");
    let job_id = a2.id.clone();
    assert!(
        nack(&client, &a2, Retry::Default, "fail 2").await,
        "the final nack dead-letters the job"
    );

    let records = drain(&client, None, 10).await;
    assert_eq!(
        records.len(),
        1,
        "the dead-lettered job is retained and drainable"
    );
    let r = &records[0];
    assert_eq!(r.cause, DeadLetterCause::AttemptsExhausted as i32);
    assert_eq!(r.final_attempt, 2);
    assert_eq!(
        r.last_reason.as_deref(),
        Some("fail 2"),
        "the last nack reason is captured"
    );
    let job = r.job.as_ref().expect("the record carries the job");
    assert_eq!(job.id, job_id);
    assert_eq!(
        job.queue, "dl-drain-q",
        "the record carries the origin queue"
    );
    assert_eq!(
        job.payload.as_ref().map(|p| p.data.as_slice()),
        Some(&b"to-replay"[..]),
        "the payload is preserved for replay",
    );

    // Drain is destructive: a second drain returns nothing.
    assert!(
        drain(&client, None, 10).await.is_empty(),
        "drained records are removed"
    );
}

#[tokio::test]
async fn force_dead_letter_is_drained_with_its_reason() {
    let (_guard, client) = start_server_with_config("dl-force-drain", RETAIN_CFG).await;

    let job_id = dead_letter_one(&client, "dl-force-q", "drop it").await;

    let records = drain(&client, None, 10).await;
    assert_eq!(records.len(), 1);
    let r = &records[0];
    assert_eq!(
        r.cause,
        DeadLetterCause::Rejected as i32,
        "a DeadLetter nack records the REJECTED cause"
    );
    assert_eq!(r.final_attempt, 1);
    assert_eq!(r.last_reason.as_deref(), Some("drop it"));
    assert_eq!(r.job.as_ref().unwrap().id, job_id);
}

#[tokio::test]
async fn lease_expiry_dead_letter_is_drainable() {
    let (_guard, client) = start_server_with_config("dl-lease", RETAIN_CFG).await;

    // max_attempts = 1: the first lease expiry dead-letters rather than redelivers.
    enqueue(
        &client,
        EnqueueRequest {
            max_attempts: Some(1),
            ..enqueue_req("dl-lease-q")
        },
    )
    .await;
    let job = reserve(&client, "dl-lease-q", Duration::from_secs(1), WAIT)
        .await
        .expect("reservable");
    let job_id = job.id.clone();

    // Let the 1s lease expire; the sweeper dead-letters the exhausted job.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let records = drain(&client, None, 10).await;
    assert_eq!(records.len(), 1, "a lease-expiry dead-letter is retained");
    let r = &records[0];
    assert_eq!(r.cause, DeadLetterCause::LeaseExpired as i32);
    assert_eq!(
        r.last_reason, None,
        "a lease-expiry death has no worker-reported reason"
    );
    assert_eq!(r.job.as_ref().unwrap().id, job_id);
}

#[tokio::test]
async fn drain_is_empty_without_retention() {
    // Default config: retention is 0, so dead jobs are deleted, not stored.
    let (_guard, client) = start_server("dl-off").await;

    let info = client
        .clone()
        .get_server_info(GetServerInfoRequest {})
        .await
        .expect("get_server_info RPC")
        .into_inner();
    assert!(
        !info.dead_letter_retention_enabled,
        "retention is disabled by default"
    );

    dead_letter_one(&client, "dl-off-q", "gone").await;
    assert!(
        drain(&client, None, 10).await.is_empty(),
        "with retention off there is nothing to drain"
    );
}

#[tokio::test]
async fn server_advertises_dead_letter_retention_when_enabled() {
    let (_guard, client) = start_server_with_config("dl-info", RETAIN_CFG).await;
    let info = client
        .clone()
        .get_server_info(GetServerInfoRequest {})
        .await
        .expect("get_server_info RPC")
        .into_inner();
    assert!(
        info.dead_letter_retention_enabled,
        "a server configured with retention advertises it"
    );
}

#[tokio::test]
async fn drain_respects_max_and_returns_oldest_first() {
    let (_guard, client) = start_server_with_config("dl-max", RETAIN_CFG).await;

    // Dead-letter three jobs in order, with gaps so their failed_at differs.
    let mut ids = Vec::new();
    for i in 0..3 {
        ids.push(dead_letter_one(&client, "dl-max-q", &format!("f{i}")).await);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // max bounds the batch; the oldest two come back first, in order.
    let first = drain(&client, None, 2).await;
    let got: Vec<&str> = first
        .iter()
        .map(|r| r.job.as_ref().unwrap().id.as_str())
        .collect();
    assert_eq!(
        got,
        vec![ids[0].as_str(), ids[1].as_str()],
        "drain returns the oldest records first, capped at max"
    );

    let rest = drain(&client, None, 2).await;
    assert_eq!(rest.len(), 1, "the next drain returns the remainder");
    assert_eq!(rest[0].job.as_ref().unwrap().id, ids[2]);

    assert!(drain(&client, None, 2).await.is_empty());
}

#[tokio::test]
async fn drain_filters_by_queue() {
    let (_guard, client) = start_server_with_config("dl-filter", RETAIN_CFG).await;

    let a1 = dead_letter_one(&client, "dl-filter-a", "a1").await;
    let _b1 = dead_letter_one(&client, "dl-filter-b", "b1").await;
    let a2 = dead_letter_one(&client, "dl-filter-a", "a2").await;

    let a = drain(&client, Some("dl-filter-a"), 10).await;
    let a_ids: std::collections::HashSet<&str> = a
        .iter()
        .map(|r| r.job.as_ref().unwrap().id.as_str())
        .collect();
    assert_eq!(a.len(), 2, "only queue a's dead-letters are drained");
    assert!(a_ids.contains(a1.as_str()) && a_ids.contains(a2.as_str()));
    assert!(
        a.iter()
            .all(|r| r.job.as_ref().unwrap().queue == "dl-filter-a"),
        "every drained record is from the filtered queue"
    );

    // Queue b's record was untouched by the filtered drain.
    let b = drain(&client, Some("dl-filter-b"), 10).await;
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].job.as_ref().unwrap().queue, "dl-filter-b");
}

#[tokio::test]
async fn retention_reclaims_expired_dead_letters_while_idle() {
    // Short retention so the sweeper reclaims within the test, and no further
    // RPCs after the dead-letter, to exercise the idle-tick reclaim path.
    let cfg = r#"
[storage]
dead_letter_retention_ms = 1500
"#;
    let (_guard, client) = start_server_with_config("dl-reclaim", cfg).await;

    dead_letter_one(&client, "dl-reclaim-q", "old").await;

    // Wait past the retention window plus a sweep interval, doing nothing else.
    tokio::time::sleep(Duration::from_secs(4)).await;

    assert!(
        drain(&client, None, 10).await.is_empty(),
        "an expired dead-letter is reclaimed by the retention sweep even while idle"
    );
}

#[tokio::test]
async fn reserve_populates_the_job_queue() {
    let (_guard, client) = start_server("q-field").await;

    enqueue(&client, enqueue_req("q-field-single")).await;
    let job = reserve(&client, "q-field-single", LEASE, WAIT)
        .await
        .expect("reservable");
    assert_eq!(
        job.queue, "q-field-single",
        "a reserved job carries its queue"
    );
    ack(&client, &job).await;

    // A multi-queue reserve lets the worker tell which queue a job came from.
    enqueue(&client, enqueue_req("q-field-b")).await;
    let job = reserve_batch(&client, &["q-field-a", "q-field-b"], LEASE, WAIT, None)
        .await
        .into_iter()
        .next()
        .expect("a job from the non-empty queue");
    assert_eq!(job.queue, "q-field-b");
    ack(&client, &job).await;
}

#[tokio::test]
async fn dead_letter_record_survives_restart() {
    // After a restart, the dead_letter in-memory index is rebuilt from the
    // persisted keys (rebuild_indexes reads them key-only), and the record body
    // is read back from the keyspace. Only a restart-then-drain proves both the
    // index repopulation and that the value survived the reopen.
    let db_path = temp_db("dl-restart");
    let cfg_path = temp_config_path("dl-restart");
    std::fs::write(&cfg_path, RETAIN_CFG).expect("write temp config");
    let cfg_str = cfg_path.to_str().expect("utf-8 config path").to_string();

    // First process: dead-letter a job with a payload, then die.
    let job_id = {
        let (child, client) = spawn_server_with_env(&db_path, &[("SEPP_CONFIG", &cfg_str)]).await;
        let _server = ServerGuard {
            child,
            db_path: None,
            cfg_path: None,
        };
        enqueue(
            &client,
            EnqueueRequest {
                payload: Some(Payload {
                    data: b"survive".to_vec(),
                    encoding: "text/plain".to_string(),
                }),
                ..enqueue_req("dl-restart-q")
            },
        )
        .await;
        let job = reserve(&client, "dl-restart-q", LEASE, WAIT)
            .await
            .expect("reservable");
        let id = job.id.clone();
        assert!(nack(&client, &job, Retry::DeadLetter, "before restart").await);
        id
    };

    // Second process on the same db: the record must drain off persisted data.
    let (child, client) = spawn_server_with_env(&db_path, &[("SEPP_CONFIG", &cfg_str)]).await;
    let _server = ServerGuard {
        child,
        db_path: Some(db_path),
        cfg_path: None,
    };
    let _ = std::fs::remove_file(&cfg_path);

    let records = drain(&client, None, 10).await;
    assert_eq!(records.len(), 1, "a dead-letter record survives a restart");
    let job = records[0].job.as_ref().expect("record carries the job");
    assert_eq!(job.id, job_id);
    assert_eq!(records[0].cause, DeadLetterCause::Rejected as i32);
    assert_eq!(records[0].last_reason.as_deref(), Some("before restart"));
    assert_eq!(job.queue, "dl-restart-q");
    assert_eq!(
        job.payload.as_ref().map(|p| p.data.as_slice()),
        Some(&b"survive"[..]),
        "the payload survives the restart",
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

    let mut custom = BTreeMap::new();
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
        cfg_path: None,
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

/// Like [`start_server_with_config`], but leaves the config file on disk and
/// returns its path so the test can rewrite it and exercise hot reloading.
async fn start_server_reloadable(
    tag: &str,
    toml: &str,
) -> (ServerGuard, Client, std::path::PathBuf) {
    let db_path = temp_db(tag);
    let cfg_path = temp_config_path(tag);
    std::fs::write(&cfg_path, toml).expect("write temp config");
    let cfg_str = cfg_path.to_str().expect("utf-8 config path");
    let (child, client) = spawn_server_with_env(&db_path, &[("SEPP_CONFIG", cfg_str)]).await;
    (
        ServerGuard {
            child,
            db_path: Some(db_path),
            cfg_path: None,
        },
        client,
        cfg_path,
    )
}

/// Enqueues one job to `queue` and reports whether it was rejected as an unknown
/// queue (the strict-mode rejection).
async fn rejected_as_unknown_queue(client: &Client, queue: &str) -> bool {
    let result = enqueue_one(client, enqueue_req(queue)).await;
    matches!(
        result.outcome,
        Some(job_result::Outcome::Rejection(r))
            if matches!(r.reason, Some(job_rejection::Reason::UnknownQueue(_)))
    )
}

/// Submits an `n`-job batch and reports whether the server accepted it (i.e. the
/// batch did not exceed `max_enqueue_batch`).
async fn enqueue_batch_accepted(client: &Client, n: usize) -> bool {
    let jobs = (0..n).map(|_| enqueue_req("hotreload-batch-q")).collect();
    client
        .clone()
        .enqueue_batch(EnqueueBatchRequest { jobs })
        .await
        .is_ok()
}

#[tokio::test]
async fn hot_reload_relaxes_strict_queues_live() {
    // Start strict: an undeclared queue is rejected up front.
    let strict = r#"
[server]
strict_queues = true

[[queues]]
name = "hotreload-declared"
"#;
    let (_guard, client, cfg_path) = start_server_reloadable("hotreload-strict", strict).await;
    assert!(
        rejected_as_unknown_queue(&client, "hotreload-ghost").await,
        "strict mode should reject an undeclared queue before reload"
    );

    // Relax strict mode on disk and wait for the watcher to apply it. We rewrite
    // periodically so a single missed filesystem event cannot wedge the test.
    let relaxed = r#"
[server]
strict_queues = false

[[queues]]
name = "hotreload-declared"
"#;
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut applied = false;
    while Instant::now() < deadline && !applied {
        std::fs::write(&cfg_path, relaxed).expect("rewrite config");
        let window = Instant::now() + Duration::from_secs(2);
        while Instant::now() < window {
            if !rejected_as_unknown_queue(&client, "hotreload-ghost").await {
                applied = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    assert!(
        applied,
        "relaxing strict_queues never took effect via hot reload"
    );

    let _ = std::fs::remove_file(&cfg_path);
}

#[tokio::test]
async fn hot_reload_changes_max_enqueue_batch_live() {
    // Start permissive: a 3-job batch is comfortably under the cap of 5.
    let (_guard, client, cfg_path) =
        start_server_reloadable("hotreload-batch", "[limits]\nmax_enqueue_batch = 5\n").await;
    assert!(
        enqueue_batch_accepted(&client, 3).await,
        "a 3-job batch is within the initial cap of 5"
    );

    // Tighten the cap below the batch size and wait for the reload to bite.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut applied = false;
    while Instant::now() < deadline && !applied {
        std::fs::write(&cfg_path, "[limits]\nmax_enqueue_batch = 2\n").expect("rewrite config");
        let window = Instant::now() + Duration::from_secs(2);
        while Instant::now() < window {
            if !enqueue_batch_accepted(&client, 3).await {
                applied = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    assert!(
        applied,
        "tightening max_enqueue_batch never took effect via hot reload"
    );

    let _ = std::fs::remove_file(&cfg_path);
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
            wait_timeout: Some(dur(NO_WAIT)),
            lease_duration: Some(dur(LEASE)),
            worker_id: None,
            max_jobs: None,
        })
        .await
        .expect_err("reserve from undeclared queue is rejected");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

async fn scrape_path(port: u16, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect to metrics endpoint");
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write scrape request");
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .expect("read scrape body");
    String::from_utf8_lossy(&buf).into_owned()
}

async fn scrape_metrics(port: u16) -> String {
    scrape_path(port, "/metrics").await
}

#[tokio::test]
async fn prometheus_endpoint_exposes_recorded_metrics() {
    // Bind the prometheus endpoint on :0 too, and read both bound ports back
    // from the server's startup logs.
    let cfg = r#"
[metrics]
prometheus_enabled = true
prometheus_listen_addr = "127.0.0.1:0"
"#;
    let db_path = temp_db("prom");
    let cfg_path = temp_config_path("prom");
    std::fs::write(&cfg_path, cfg).expect("write temp config");
    let cfg_str = cfg_path.to_str().expect("utf-8 config path");
    let (child, book) = spawn_server_process(&db_path, &[("SEPP_CONFIG", cfg_str)]);
    let _guard = ServerGuard {
        child,
        db_path: Some(db_path),
        cfg_path: None,
    };
    let grpc_port = wait_for_port(&book, "grpc", STARTUP_TIMEOUT)
        .await
        .expect("server reported a grpc port");
    let met_port = wait_for_port(&book, "prometheus", STARTUP_TIMEOUT)
        .await
        .expect("server reported a prometheus port");
    let _ = std::fs::remove_file(&cfg_path);
    let client = connect_client(grpc_port).await;

    // Drive some activity so the request/enqueue counters have data points.
    enqueue(&client, enqueue_req("smoke-prom")).await;

    let body = scrape_metrics(met_port).await;
    assert!(
        body.starts_with("HTTP/1.1 200"),
        "metrics endpoint should return 200, got:\n{body}"
    );
    assert!(
        body.contains("sepp_requests") || body.contains("sepp_jobs_enqueued"),
        "expected recorded sepp metrics in the scrape, got:\n{body}"
    );

    // Any path other than /metrics is a 404 that points the caller at /metrics.
    let other = scrape_path(met_port, "/not-metrics").await;
    assert!(
        other.starts_with("HTTP/1.1 404"),
        "a non-/metrics path should 404, got:\n{other}"
    );
    assert!(
        other.contains("try /metrics"),
        "the 404 body should hint at the right path, got:\n{other}"
    );
}

#[tokio::test]
async fn enqueue_is_rejected_when_queue_is_full() {
    let cfg = r#"
[limits]
max_queue_depth = 2
"#;
    let (_guard, client) = start_server_with_config("depth", cfg).await;

    // The first two jobs fit within the cap.
    enqueue(&client, enqueue_req("smoke-depth")).await;
    enqueue(&client, enqueue_req("smoke-depth")).await;

    // A third is rejected per-job with queue_full; the RPC itself succeeds.
    let result = enqueue_one(&client, enqueue_req("smoke-depth")).await;
    match result.outcome {
        Some(job_result::Outcome::Rejection(r)) => match r.reason {
            Some(job_rejection::Reason::QueueFull(qf)) => {
                assert_eq!(qf.queue, "smoke-depth");
                assert_eq!(qf.limit, 2);
            }
            other => panic!("expected QueueFull rejection, got {other:?}"),
        },
        other => panic!("enqueue beyond the cap was not rejected: {other:?}"),
    }

    // A different queue has its own independent budget.
    enqueue(&client, enqueue_req("smoke-depth-other")).await;

    // Completing a job frees capacity in its queue.
    let job = reserve(&client, "smoke-depth", LEASE, NO_WAIT)
        .await
        .expect("a job is available");
    ack(&client, &job).await;
    enqueue(&client, enqueue_req("smoke-depth")).await;
}

#[tokio::test]
async fn full_queue_rejects_only_its_own_jobs_in_a_batch() {
    let cfg = r#"
[limits]
max_queue_depth = 2
"#;
    let (_guard, client) = start_server_with_config("depth-batch", cfg).await;

    // Four jobs in one batch: two fit, the third exceeds the cap mid-batch,
    // and the job aimed at another queue is untouched.
    let results = client
        .clone()
        .enqueue_batch(EnqueueBatchRequest {
            jobs: vec![
                enqueue_req("smoke-depth-a"),
                enqueue_req("smoke-depth-a"),
                enqueue_req("smoke-depth-a"),
                enqueue_req("smoke-depth-b"),
            ],
        })
        .await
        .expect("enqueue_batch RPC")
        .into_inner()
        .results;

    assert!(matches!(
        results[0].outcome,
        Some(job_result::Outcome::Success(_))
    ));
    assert!(matches!(
        results[1].outcome,
        Some(job_result::Outcome::Success(_))
    ));
    assert!(matches!(
        &results[2].outcome,
        Some(job_result::Outcome::Rejection(r))
            if matches!(r.reason, Some(job_rejection::Reason::QueueFull(_)))
    ));
    assert!(matches!(
        results[3].outcome,
        Some(job_result::Outcome::Success(_))
    ));
}

#[tokio::test]
async fn dedup_hits_bypass_a_full_queue() {
    let cfg = r#"
[limits]
max_queue_depth = 1
"#;
    let (_guard, client) = start_server_with_config("depth-dedup", cfg).await;

    let first = enqueue(
        &client,
        EnqueueRequest {
            idempotency_key: Some("dd-1".into()),
            ..enqueue_req("smoke-depth-dedup")
        },
    )
    .await;

    // The queue is now full, but a duplicate adds nothing and still answers.
    let dup = enqueue(
        &client,
        EnqueueRequest {
            idempotency_key: Some("dd-1".into()),
            ..enqueue_req("smoke-depth-dedup")
        },
    )
    .await;
    assert!(dup.deduplicated);
    assert_eq!(dup.job_id, first.job_id);
}

#[tokio::test]
async fn atomic_enqueue_to_a_full_queue_commits_nothing() {
    let cfg = r#"
[limits]
max_queue_depth = 2
"#;
    let (_guard, client) = start_server_with_config("depth-atomic", cfg).await;

    // Three jobs against a cap of 2: the batch cannot fit as a whole, so every
    // job aimed at the full queue is reported and nothing commits, including
    // the job aimed at the unrelated queue.
    let response = client
        .clone()
        .enqueue_atomic(EnqueueBatchRequest {
            jobs: vec![
                enqueue_req("atomic-full"),
                enqueue_req("atomic-full"),
                enqueue_req("atomic-full"),
                enqueue_req("atomic-full-other"),
            ],
        })
        .await
        .expect("enqueue_atomic RPC")
        .into_inner();

    let failure = match response.outcome {
        Some(enqueue_atomic_response::Outcome::Rejection(f)) => f,
        other => panic!("expected Rejection outcome, got {other:?}"),
    };
    assert_eq!(
        failure.errors.iter().map(|e| e.index).collect::<Vec<_>>(),
        vec![0, 1, 2],
        "every job aimed at the full queue is reported"
    );
    for e in &failure.errors {
        match &e.rejection.as_ref().expect("rejection is set").reason {
            Some(job_rejection::Reason::QueueFull(qf)) => {
                assert_eq!(qf.queue, "atomic-full");
                assert_eq!(qf.limit, 2);
            }
            other => panic!("expected QueueFull, got {other:?}"),
        }
    }

    assert!(
        reserve(&client, "atomic-full", LEASE, NO_WAIT)
            .await
            .is_none(),
        "the full queue received nothing"
    );
    assert!(
        reserve(&client, "atomic-full-other", LEASE, NO_WAIT)
            .await
            .is_none(),
        "atomic rejection rolls back the whole batch"
    );
}

#[tokio::test]
async fn max_queue_depth_zero_rejects_every_enqueue() {
    // Omitting the key means unlimited; an explicit 0 rejects everything.
    let cfg = r#"
[limits]
max_queue_depth = 0
"#;
    let (_guard, client) = start_server_with_config("depth-zero", cfg).await;

    let result = enqueue_one(&client, enqueue_req("smoke-depth-zero")).await;
    assert!(matches!(
        &result.outcome,
        Some(job_result::Outcome::Rejection(r))
            if matches!(r.reason, Some(job_rejection::Reason::QueueFull(_)))
    ));
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
                assert_eq!(
                    p.limit, 16,
                    "the per-queue limit is reported, not the global"
                );
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
            cfg_path: None,
        };
        enqueue(&client, enqueue_req("smoke-restart")).await.job_id
    };

    let (child, client) = spawn_server(&db_path).await;
    let _server = ServerGuard {
        child,
        db_path: Some(db_path),
        cfg_path: None,
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
    assert_eq!(
        reserved.len(),
        3,
        "every job in the atomic batch is reservable"
    );
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
                enqueue_req("atomic-good"),  // index 0 — valid
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

#[tokio::test]
async fn tls_secures_the_connection() {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate self-signed cert for localhost");
    let cert_pem = cert.pem();
    let key_pem = signing_key.serialize_pem();

    let unique = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let cert_path = std::env::temp_dir().join(format!("sepp-it-tls-{unique}.crt"));
    let key_path = std::env::temp_dir().join(format!("sepp-it-tls-{unique}.key"));
    std::fs::write(&cert_path, &cert_pem).expect("write cert");
    std::fs::write(&key_path, &key_pem).expect("write key");

    let db_path = temp_db("tls");
    let cert_path_str = cert_path.to_str().expect("utf-8 cert path");
    let key_path_str = key_path.to_str().expect("utf-8 key path");
    let (child, book) = spawn_server_process(
        &db_path,
        &[
            ("SEPP_SERVER__TLS_CERT_PATH", cert_path_str),
            ("SEPP_SERVER__TLS_KEY_PATH", key_path_str),
        ],
    );
    let _guard = ServerGuard {
        child,
        db_path: Some(db_path),
        cfg_path: None,
    };
    let port = wait_for_port(&book, "grpc", STARTUP_TIMEOUT)
        .await
        .expect("server reported a listening port");

    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(cert_pem.as_bytes()))
        .domain_name("localhost");
    let url = format!("https://localhost:{port}");

    let mut last_err: Option<tonic::transport::Error> = None;
    let mut client: Option<Client> = None;
    for _ in 0..100 {
        let endpoint = Endpoint::from_shared(url.clone())
            .expect("valid endpoint URL")
            .tls_config(tls.clone())
            .expect("client TLS config");
        match endpoint.connect().await {
            Ok(channel) => {
                client = Some(QueueServiceClient::new(channel));
                break;
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
    let mut client = client
        .unwrap_or_else(|| panic!("TLS server did not become reachable on {url}: {last_err:?}"));

    // Round-trips a request to prove the TLS channel actually works end-to-end.
    let info = client
        .get_server_info(GetServerInfoRequest {})
        .await
        .expect("get_server_info over TLS")
        .into_inner();
    assert!(
        !info.server_version.is_empty(),
        "TLS roundtrip returned a usable response"
    );

    // A plaintext client must not be able to drive RPCs against a TLS-only
    // server. tonic only does TCP at connect time — the h2/TLS handshake
    // happens on first request — so we have to actually issue an RPC. The
    // timeout guards against a stalled handshake.
    let plaintext_url = format!("http://127.0.0.1:{port}");
    let rpc = async {
        let mut c = QueueServiceClient::connect(plaintext_url).await?;
        c.get_server_info(GetServerInfoRequest {}).await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let plain = tokio::time::timeout(Duration::from_secs(3), rpc).await;
    assert!(
        matches!(plain, Err(_) | Ok(Err(_))),
        "plaintext RPC unexpectedly succeeded against a TLS-only server"
    );
}

/// Blocking HTTPS roundtrip against `127.0.0.1:port` trusting only `ca_pem`,
/// returning the raw response text. Uses rustls's sync API so the test does not
/// need an HTTP client dependency.
fn https_roundtrip(
    port: u16,
    ca_pem: &str,
    request: &[u8],
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    use std::io::{Read, Write};

    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, ServerName};

    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from_pem_slice(ca_pem.as_bytes())?)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];

    let mut conn =
        rustls::ClientConnection::new(Arc::new(config), ServerName::try_from("localhost")?)?;
    let mut sock = std::net::TcpStream::connect(("127.0.0.1", port))?;
    sock.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut tls = rustls::Stream::new(&mut conn, &mut sock);
    tls.write_all(request)?;
    let mut response = Vec::new();
    // A missing close_notify after the response only matters for truncation
    // attacks, not for this assertion; keep whatever bytes arrived.
    let _ = tls.read_to_end(&mut response);
    Ok(String::from_utf8_lossy(&response).into_owned())
}

#[tokio::test]
async fn admin_ui_serves_https_when_tls_configured() {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate self-signed cert for localhost");
    let cert_pem = cert.pem();
    let key_pem = signing_key.serialize_pem();

    let unique = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let cert_path = std::env::temp_dir().join(format!("sepp-it-admin-tls-{unique}.crt"));
    let key_path = std::env::temp_dir().join(format!("sepp-it-admin-tls-{unique}.key"));
    std::fs::write(&cert_path, &cert_pem).expect("write cert");
    std::fs::write(&key_path, &key_pem).expect("write key");
    // Admin keys are a list of tables, which figment's env provider cannot
    // express, so this test brings its own config file.
    let config_path = std::env::temp_dir().join(format!("sepp-it-admin-tls-{unique}.toml"));
    std::fs::write(
        &config_path,
        "[[admin.keys]]\nname = \"ops\"\nkey = \"integration-secret\"\n",
    )
    .expect("write config");

    let db_path = temp_db("admin-tls");
    let (child, book) = spawn_server_process(
        &db_path,
        &[
            ("SEPP_CONFIG", config_path.to_str().expect("utf-8 path")),
            (
                "SEPP_ADMIN__TLS_CERT_PATH",
                cert_path.to_str().expect("utf-8 path"),
            ),
            (
                "SEPP_ADMIN__TLS_KEY_PATH",
                key_path.to_str().expect("utf-8 path"),
            ),
        ],
    );
    let _guard = ServerGuard {
        child,
        db_path: Some(db_path),
        cfg_path: None,
    };
    let port = wait_for_port(&book, "admin", STARTUP_TIMEOUT)
        .await
        .expect("admin UI reported a listening port");

    let body = r#"{"name":"ops","key":"integration-secret"}"#;
    let request = format!(
        "POST /admin/api/v1/session HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let ca = cert_pem.clone();
    let response =
        tokio::task::spawn_blocking(move || https_roundtrip(port, &ca, request.as_bytes()))
            .await
            .expect("https roundtrip task")
            .expect("login over admin HTTPS");
    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
    let _ = std::fs::remove_file(&config_path);

    assert!(
        response.starts_with("HTTP/1.1 200"),
        "login over HTTPS should succeed, got: {response}"
    );
    assert!(
        response.contains("sepp_admin=") && response.contains("; Secure"),
        "session cookie under TLS must carry the Secure attribute, got: {response}"
    );

    // A plaintext request against the TLS listener must not get an HTTP
    // response; the handshake failure surfaces as an alert or a dropped
    // connection.
    let plain = tokio::task::spawn_blocking(move || {
        use std::io::{Read, Write};
        let mut sock = std::net::TcpStream::connect(("127.0.0.1", port))?;
        sock.set_read_timeout(Some(Duration::from_secs(3)))?;
        sock.write_all(b"GET /admin/api/v1/server-info HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")?;
        let mut response = Vec::new();
        let _ = sock.read_to_end(&mut response);
        Ok::<_, std::io::Error>(String::from_utf8_lossy(&response).into_owned())
    })
    .await
    .expect("plaintext probe task");
    if let Ok(plain) = plain {
        assert!(
            !plain.starts_with("HTTP/1.1"),
            "plaintext request unexpectedly got an HTTP response from a TLS-only admin listener: {plain}"
        );
    }
}

/// A syntactically valid UUID that no enqueue ever assigns, for exercising the
/// not-found paths. It clears request validation, so the server reaches storage.
const FAKE_JOB_ID: &str = "00000000-0000-4000-8000-000000000000";

#[tokio::test]
async fn stale_attempt_is_fenced_off() {
    let (_guard, client) = start_server("fence").await;

    enqueue(&client, enqueue_req("smoke-fence")).await;
    let first = reserve(&client, "smoke-fence", Duration::from_secs(1), WAIT)
        .await
        .expect("first delivery reservable");
    assert_eq!(first.attempt, 1);

    // Let the 1s lease expire; the sweeper redelivers the job, and re-reserving
    // it makes the in-flight record carry attempt 2. The first worker's handle
    // (attempt 1) is now stale.
    let second = reserve(&client, "smoke-fence", LEASE, Duration::from_secs(15))
        .await
        .expect("expired lease is redelivered");
    assert_eq!(second.attempt, 2, "redelivery bumped the attempt");

    // ack / nack / extend with the stale attempt must all be fenced off with
    // FAILED_PRECONDITION rather than silently mutating the live attempt.
    let ack_err = client
        .clone()
        .ack(AckRequest {
            job_id: first.id.clone(),
            attempt: first.attempt,
            worker_id: None,
        })
        .await
        .expect_err("a stale ack is rejected");
    assert_eq!(ack_err.code(), tonic::Code::FailedPrecondition);

    let nack_err = client
        .clone()
        .nack(NackRequest {
            job_id: first.id.clone(),
            attempt: first.attempt,
            reason: Some("stale".to_string()),
            retry: Some(NackRetry {
                strategy: Some(nack_retry::Strategy::Default(())),
            }),
            worker_id: None,
        })
        .await
        .expect_err("a stale nack is rejected");
    assert_eq!(nack_err.code(), tonic::Code::FailedPrecondition);

    let extend_err = client
        .clone()
        .extend(ExtendRequest {
            job_id: first.id.clone(),
            attempt: first.attempt,
            lease_duration: Some(dur(LEASE)),
            worker_id: None,
        })
        .await
        .expect_err("a stale extend is rejected");
    assert_eq!(extend_err.code(), tonic::Code::FailedPrecondition);

    // The current holder (attempt 2) is unaffected and can complete the job.
    ack(&client, &second).await;
}

#[tokio::test]
async fn completing_an_unknown_job_is_not_found() {
    let (_guard, client) = start_server("notfound").await;

    // Acking a job that never existed is NOT_FOUND.
    let err = client
        .clone()
        .ack(AckRequest {
            job_id: FAKE_JOB_ID.to_string(),
            attempt: 1,
            worker_id: None,
        })
        .await
        .expect_err("acking an unknown job is rejected");
    assert_eq!(err.code(), tonic::Code::NotFound);

    // So is nacking one.
    let err = client
        .clone()
        .nack(NackRequest {
            job_id: FAKE_JOB_ID.to_string(),
            attempt: 1,
            reason: None,
            retry: Some(NackRetry {
                strategy: Some(nack_retry::Strategy::Default(())),
            }),
            worker_id: None,
        })
        .await
        .expect_err("nacking an unknown job is rejected");
    assert_eq!(err.code(), tonic::Code::NotFound);

    // A second ack of an already-completed job is NOT_FOUND too: the first ack
    // removed it from the in-flight set.
    enqueue(&client, enqueue_req("smoke-notfound")).await;
    let job = reserve(&client, "smoke-notfound", LEASE, WAIT)
        .await
        .expect("job reservable");
    ack(&client, &job).await;
    let err = client
        .clone()
        .ack(AckRequest {
            job_id: job.id.clone(),
            attempt: job.attempt,
            worker_id: None,
        })
        .await
        .expect_err("double-ack is rejected");
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn structurally_invalid_enqueue_is_rejected() {
    let (_guard, client) = start_server("invalid").await;

    // priority must be 0..=9 (documented in the proto). An out-of-range value
    // is surfaced as an InvalidRequest rejection carrying the violation message.
    let rejected = enqueue_one(
        &client,
        EnqueueRequest {
            priority: Some(42),
            ..enqueue_req("smoke-invalid")
        },
    )
    .await;
    match rejected.outcome {
        Some(job_result::Outcome::Rejection(r)) => match r.reason {
            Some(job_rejection::Reason::InvalidRequest(inv)) => {
                assert!(!inv.message.is_empty(), "the violation is described");
            }
            other => panic!("expected InvalidRequest, got {other:?}"),
        },
        other => panic!("an out-of-range priority was not rejected: {other:?}"),
    }

    // An empty job_type (min_len = 1) is likewise a structural rejection.
    let rejected = enqueue_one(
        &client,
        EnqueueRequest {
            job_type: String::new(),
            ..enqueue_req("smoke-invalid")
        },
    )
    .await;
    assert!(
        matches!(
            &rejected.outcome,
            Some(job_result::Outcome::Rejection(r))
                if matches!(r.reason, Some(job_rejection::Reason::InvalidRequest(_)))
        ),
        "an empty job_type is rejected: {:?}",
        rejected.outcome
    );
}

#[tokio::test]
async fn invalid_queue_names_are_rejected_on_the_grpc_plane() {
    let (_guard, client) = start_server("qname").await;

    // Same validity rule config validation enforces: "."/"..", '/', and
    // control characters are unaddressable through the admin REST API, so the
    // gRPC plane must not auto-create such queues.
    for bad in [".", "..", "a/b", "ctl\u{1}"] {
        let rejected = enqueue_one(&client, enqueue_req(bad)).await;
        assert!(
            matches!(
                &rejected.outcome,
                Some(job_result::Outcome::Rejection(r))
                    if matches!(r.reason, Some(job_rejection::Reason::InvalidRequest(_)))
            ),
            "queue name {bad:?} must be rejected: {:?}",
            rejected.outcome
        );

        let err = client
            .clone()
            .reserve(ReserveRequest {
                queues: vec![bad.to_string()],
                wait_timeout: None,
                lease_duration: Some(dur(LEASE)),
                worker_id: None,
                max_jobs: None,
            })
            .await
            .expect_err("reserve refuses the name outright");
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{bad:?}");
    }

    // A name that merely contains dots stays valid.
    enqueue(&client, enqueue_req("smoke.qname.ok")).await;
}

#[tokio::test]
async fn a_sub_millisecond_lease_is_floored_not_granted_expired() {
    let (_guard, client) = start_server("subms").await;
    let enqueued = enqueue(&client, enqueue_req("smoke-subms")).await;

    // 100µs is strictly positive (passes validation) but truncates to 0ms
    // internally; the server must floor it to 1ms rather than hand out a
    // lease that is expired on arrival.
    let job = client
        .clone()
        .reserve(ReserveRequest {
            queues: vec!["smoke-subms".to_string()],
            wait_timeout: Some(dur(WAIT)),
            lease_duration: Some(prost_types::Duration {
                seconds: 0,
                nanos: 100_000,
            }),
            worker_id: None,
            max_jobs: None,
        })
        .await
        .expect("a sub-ms lease duration is accepted")
        .into_inner()
        .jobs
        .into_iter()
        .next()
        .expect("the job is granted");
    assert_eq!(job.id, enqueued.job_id);
    assert_eq!(job.attempt, 1);

    // The 1ms lease lapses and the sweep redelivers the job rather than
    // losing it.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let again = reserve(&client, "smoke-subms", LEASE, WAIT)
        .await
        .expect("the lapsed job is redelivered");
    assert_eq!(again.id, enqueued.job_id);
    assert_eq!(again.attempt, 2);

    // Extend floors the same way: a sub-ms extension is accepted and reports
    // an expiry instead of failing or handing back an expired lease.
    let expires = client
        .clone()
        .extend(ExtendRequest {
            job_id: again.id.clone(),
            attempt: again.attempt,
            lease_duration: Some(prost_types::Duration {
                seconds: 0,
                nanos: 100_000,
            }),
            worker_id: None,
        })
        .await
        .expect("a sub-ms extend is accepted")
        .into_inner()
        .lease_expires_at
        .expect("extend reports a lease expiry");
    assert!(ts_to_ms(&expires) > 0);
}

#[tokio::test]
async fn a_blocked_reserve_wakes_on_enqueue() {
    let (_guard, client) = start_server("wake").await;

    // Park a reserve on an empty queue with a long timeout, then enqueue from a
    // separate task. The waiter must be woken promptly — long-poll's whole point.
    let waiter = {
        let client = client.clone();
        tokio::spawn(
            async move { reserve(&client, "smoke-wake", LEASE, Duration::from_secs(20)).await },
        )
    };

    // Give the reserve time to arm its waiter on the (still empty) queue.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let started = Instant::now();
    let enqueued = enqueue(&client, enqueue_req("smoke-wake")).await;

    let job = waiter
        .await
        .expect("reserve task joins")
        .expect("the parked reserve returns the freshly enqueued job");
    assert_eq!(job.id, enqueued.job_id);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "a blocked reserve should wake on enqueue, not ride out its 20s timeout (took {:?})",
        started.elapsed(),
    );
    ack(&client, &job).await;
}

#[tokio::test]
async fn reserve_validates_its_request() {
    let (_guard, client) = start_server("rvalid").await;

    // Empty queue list (proto repeated.min_items = 1).
    let err = client
        .clone()
        .reserve(ReserveRequest {
            queues: vec![],
            wait_timeout: None,
            lease_duration: Some(dur(LEASE)),
            worker_id: None,
            max_jobs: None,
        })
        .await
        .expect_err("an empty queue list is rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // Zero lease duration (proto duration.gt = {}: must be strictly positive).
    let err = client
        .clone()
        .reserve(ReserveRequest {
            queues: vec!["smoke-rvalid".to_string()],
            wait_timeout: None,
            lease_duration: Some(dur(Duration::ZERO)),
            worker_id: None,
            max_jobs: None,
        })
        .await
        .expect_err("a zero lease duration is rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // More queues than max_reserve_queues (default 32) — a server-side cap.
    let too_many: Vec<String> = (0..40).map(|i| format!("q{i}")).collect();
    let err = client
        .clone()
        .reserve(ReserveRequest {
            queues: too_many,
            wait_timeout: None,
            lease_duration: Some(dur(LEASE)),
            worker_id: None,
            max_jobs: None,
        })
        .await
        .expect_err("exceeding max_reserve_queues is rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn empty_and_oversized_batches_are_rejected() {
    let cfg = r#"
[limits]
max_enqueue_batch = 1
"#;
    let (_guard, client) = start_server_with_config("batchcap", cfg).await;

    // An empty batch is rejected by both batch RPCs.
    let err = client
        .clone()
        .enqueue_batch(EnqueueBatchRequest { jobs: vec![] })
        .await
        .expect_err("an empty EnqueueBatch is rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    let err = client
        .clone()
        .enqueue_atomic(EnqueueBatchRequest { jobs: vec![] })
        .await
        .expect_err("an empty EnqueueAtomic is rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // A batch over the configured cap of 1 is rejected by both.
    let two = vec![enqueue_req("smoke-batchcap"), enqueue_req("smoke-batchcap")];
    let err = client
        .clone()
        .enqueue_batch(EnqueueBatchRequest { jobs: two.clone() })
        .await
        .expect_err("a batch over max_enqueue_batch is rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    let err = client
        .clone()
        .enqueue_atomic(EnqueueBatchRequest { jobs: two })
        .await
        .expect_err("an atomic batch over max_enqueue_batch is rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn encoding_restriction_is_advertised_and_enforced() {
    let cfg = r#"
[limits]
allowed_encodings = ["json"]
"#;
    let (_guard, client) = start_server_with_config("enc", cfg).await;

    // The restriction is advertised through GetServerInfo.
    let info = client
        .clone()
        .get_server_info(GetServerInfoRequest {})
        .await
        .expect("get_server_info RPC")
        .into_inner();
    assert!(
        info.restricts_encodings,
        "a server with allowed_encodings restricts them"
    );
    assert_eq!(info.allowed_encodings, vec!["json".to_string()]);

    // An allowed encoding is accepted.
    let ok = enqueue_one(
        &client,
        EnqueueRequest {
            payload: Some(Payload {
                data: b"{}".to_vec(),
                encoding: "json".to_string(),
            }),
            ..enqueue_req("smoke-enc")
        },
    )
    .await;
    assert!(matches!(ok.outcome, Some(job_result::Outcome::Success(_))));

    // A disallowed encoding is rejected, and the rejection names what's allowed.
    let rejected = enqueue_one(
        &client,
        EnqueueRequest {
            payload: Some(Payload {
                data: b"<x/>".to_vec(),
                encoding: "xml".to_string(),
            }),
            ..enqueue_req("smoke-enc")
        },
    )
    .await;
    match rejected.outcome {
        Some(job_result::Outcome::Rejection(r)) => match r.reason {
            Some(job_rejection::Reason::EncodingNotAllowed(e)) => {
                assert_eq!(e.encoding, "xml");
                assert_eq!(e.allowed, vec!["json".to_string()]);
            }
            other => panic!("expected EncodingNotAllowed, got {other:?}"),
        },
        other => panic!("a disallowed encoding was not rejected: {other:?}"),
    }
}

#[tokio::test]
async fn limit_rejections_report_the_offending_value() {
    // One server, tight on every size limit; each request below violates exactly
    // one of them. check_enqueue_limits short-circuits on the first violation, so
    // every other field is kept within its bound (note the 1-byte job_type).
    let cfg = r#"
[limits]
max_queue_name_bytes = 8
max_job_type_bytes = 8
max_idempotency_key_bytes = 8
max_custom_entries = 2
max_custom_key_bytes = 4
max_custom_total_bytes = 16
max_schedule_horizon_ms = 1000
"#;
    let (_guard, client) = start_server_with_config("limits", cfg).await;

    fn tiny(queue: &str) -> EnqueueRequest {
        EnqueueRequest {
            queue: queue.to_string(),
            job_type: "j".to_string(),
            ..Default::default()
        }
    }
    fn rejection_reason(out: sepp::pb::sepp::v1::JobResult) -> job_rejection::Reason {
        match out.outcome {
            Some(job_result::Outcome::Rejection(r)) => {
                r.reason.expect("a rejection always carries a reason")
            }
            other => panic!("expected a rejection, got {other:?}"),
        }
    }
    use job_rejection::Reason;

    // Queue name too long.
    let name = "this-name-is-too-long";
    match rejection_reason(enqueue_one(&client, tiny(name)).await) {
        Reason::QueueNameTooLong(q) => {
            assert_eq!(q.limit, 8);
            assert_eq!(q.actual, name.len() as u64);
        }
        other => panic!("expected QueueNameTooLong, got {other:?}"),
    }

    // job_type too long.
    let r = rejection_reason(
        enqueue_one(
            &client,
            EnqueueRequest {
                job_type: "way-too-long-type".to_string(),
                ..tiny("ok")
            },
        )
        .await,
    );
    assert!(matches!(r, Reason::JobTypeNameTooLong(j) if j.limit == 8));

    // idempotency_key too long.
    let r = rejection_reason(
        enqueue_one(
            &client,
            EnqueueRequest {
                idempotency_key: Some("idemp-key-too-long".to_string()),
                ..tiny("ok")
            },
        )
        .await,
    );
    assert!(matches!(r, Reason::IdempotencyKeyTooLong(k) if k.limit == 8));

    // Too many custom entries (checked before any per-entry rule).
    let mut many = BTreeMap::new();
    many.insert("a".to_string(), prim_int(1));
    many.insert("b".to_string(), prim_int(2));
    many.insert("c".to_string(), prim_int(3));
    let r = rejection_reason(
        enqueue_one(
            &client,
            EnqueueRequest {
                custom: many,
                ..tiny("ok")
            },
        )
        .await,
    );
    assert!(matches!(r, Reason::CustomEntriesTooMany(c) if c.limit == 2 && c.actual == 3));

    // A single custom key longer than max_custom_key_bytes.
    let mut long_key = BTreeMap::new();
    long_key.insert("longkey".to_string(), prim_int(1));
    match rejection_reason(
        enqueue_one(
            &client,
            EnqueueRequest {
                custom: long_key,
                ..tiny("ok")
            },
        )
        .await,
    ) {
        Reason::CustomKeyTooLong(k) => {
            assert_eq!(k.key, "longkey");
            assert_eq!(k.limit, 4);
            assert_eq!(k.actual, 7);
        }
        other => panic!("expected CustomKeyTooLong, got {other:?}"),
    }

    // Aggregate custom bytes over the cap (key within limits; the value tips it).
    let mut big = BTreeMap::new();
    big.insert("k".to_string(), prim_str("0123456789abcdefghij")); // 1 + 20 = 21 bytes
    match rejection_reason(
        enqueue_one(
            &client,
            EnqueueRequest {
                custom: big,
                ..tiny("ok")
            },
        )
        .await,
    ) {
        Reason::CustomMapTooLarge(c) => {
            assert_eq!(c.limit, 16);
            assert_eq!(c.actual, 21);
        }
        other => panic!("expected CustomMapTooLarge, got {other:?}"),
    }

    // Scheduled further out than the horizon.
    let r = rejection_reason(
        enqueue_one(
            &client,
            EnqueueRequest {
                scheduled_at: Some(ts_ms(epoch_ms_in(Duration::from_secs(3600)))),
                ..tiny("ok")
            },
        )
        .await,
    );
    match r {
        Reason::ScheduledTooFar(s) => {
            let horizon = s.horizon.expect("rejection carries the horizon");
            assert_eq!(
                sepp::pb::duration_to_millis(&horizon),
                1000,
                "the rejection reports the configured horizon"
            );
        }
        other => panic!("expected ScheduledTooFar, got {other:?}"),
    }
}

#[tokio::test]
async fn dedup_lapses_after_the_window() {
    let cfg = r#"
[storage]
dedup_window_ms = 800
"#;
    let (_guard, client) = start_server_with_config("dedup-lapse", cfg).await;

    let req = || EnqueueRequest {
        idempotency_key: Some("k".to_string()),
        ..enqueue_req("smoke-dedup-lapse")
    };

    let first = enqueue(&client, req()).await;
    assert!(!first.deduplicated);

    // Within the window, the same key collapses onto the first job.
    let within = enqueue(&client, req()).await;
    assert!(within.deduplicated);
    assert_eq!(within.job_id, first.job_id);

    // Once the window lapses, the key is free again and a fresh job is created.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let after = enqueue(&client, req()).await;
    assert!(!after.deduplicated, "the dedup key expires with its window");
    assert_ne!(after.job_id, first.job_id);
}

#[tokio::test]
async fn health_service_reports_serving() {
    use tonic_health::pb::health_client::HealthClient;
    use tonic_health::pb::{HealthCheckRequest, health_check_response::ServingStatus};

    let db_path = temp_db("health");
    let (child, book) = spawn_server_process(&db_path, &[]);
    let _guard = ServerGuard {
        child,
        db_path: Some(db_path),
        cfg_path: None,
    };
    let port = wait_for_port(&book, "grpc", STARTUP_TIMEOUT)
        .await
        .expect("server reported a listening port");

    let addr = format!("http://127.0.0.1:{port}");
    let mut health = None;
    for _ in 0..100 {
        let endpoint = Endpoint::from_shared(addr.clone()).expect("valid endpoint URL");
        if let Ok(channel) = endpoint.connect().await {
            health = Some(HealthClient::new(channel));
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut health = health.expect("health endpoint became reachable");

    // The server registers the queue service as SERVING before it starts
    // accepting connections.
    let resp = health
        .check(HealthCheckRequest {
            service: "sepp.v1.QueueService".to_string(),
        })
        .await
        .expect("health check RPC")
        .into_inner();
    assert_eq!(resp.status, ServingStatus::Serving as i32);
}

#[tokio::test]
async fn reserve_lease_is_clamped_to_queue_max() {
    let cfg = r#"
[[queues]]
name = "smoke-capped"
max_lease_duration_ms = 2000
"#;
    let (_guard, client) = start_server_with_config("clamp", cfg).await;

    enqueue(&client, enqueue_req("smoke-capped")).await;

    // Ask for a 10-minute lease; the queue caps it at 2s.
    let before = epoch_ms_in(Duration::ZERO);
    let job = reserve(&client, "smoke-capped", Duration::from_secs(600), WAIT)
        .await
        .expect("capped job reservable");
    let lease_expires_ms = ts_to_ms(&job.lease_expires_at.expect("reserved job has a lease"));
    assert!(
        lease_expires_ms <= before + 2000 + 2000,
        "reserve clamps the lease to the queue max (got {}ms out)",
        lease_expires_ms - before,
    );
    assert!(
        lease_expires_ms >= before + 1000,
        "yet the deadline is genuinely in the future"
    );

    // Extend clamps the same way.
    let before = epoch_ms_in(Duration::ZERO);
    let extended = extend(&client, &job, Duration::from_secs(600)).await;
    assert!(
        extended <= before + 2000 + 2000,
        "extend clamps the lease to the queue max (got {}ms out)",
        extended - before,
    );

    ack(&client, &job).await;
}

#[tokio::test]
async fn inflight_job_survives_restart_and_is_redelivered() {
    let db_path = temp_db("restart-inflight");

    let job_id = {
        let (child, client) = spawn_server(&db_path).await;
        let _server = ServerGuard {
            child,
            db_path: None,
            cfg_path: None,
        };
        let id = enqueue(&client, enqueue_req("smoke-restart-inflight"))
            .await
            .job_id;
        // Lease it (briefly) so it is in-flight when the server is dropped.
        let job = reserve(
            &client,
            "smoke-restart-inflight",
            Duration::from_secs(1),
            WAIT,
        )
        .await
        .expect("job reservable before the restart");
        assert_eq!(job.attempt, 1);
        id
    };

    // A fresh server on the same database rebuilds the lease timer and, once the
    // (already expired) lease is swept, redelivers the job as attempt 2.
    let (child, client) = spawn_server(&db_path).await;
    let _server = ServerGuard {
        child,
        db_path: Some(db_path),
        cfg_path: None,
    };

    let job = reserve(
        &client,
        "smoke-restart-inflight",
        LEASE,
        Duration::from_secs(15),
    )
    .await
    .expect("an in-flight job is redelivered after a restart");
    assert_eq!(job.id, job_id, "the same job survives the restart");
    assert_eq!(
        job.attempt, 2,
        "the post-restart redelivery bumps the attempt"
    );
    ack(&client, &job).await;
}

#[tokio::test]
async fn scheduled_job_survives_restart() {
    let db_path = temp_db("restart-sched");

    let job_id = {
        let (child, client) = spawn_server(&db_path).await;
        let _server = ServerGuard {
            child,
            db_path: None,
            cfg_path: None,
        };
        enqueue(
            &client,
            EnqueueRequest {
                scheduled_at: Some(ts_ms(epoch_ms_in(Duration::from_secs(3)))),
                ..enqueue_req("smoke-restart-sched")
            },
        )
        .await
        .job_id
    };

    let (child, client) = spawn_server(&db_path).await;
    let _server = ServerGuard {
        child,
        db_path: Some(db_path),
        cfg_path: None,
    };

    let job = reserve(
        &client,
        "smoke-restart-sched",
        LEASE,
        Duration::from_secs(15),
    )
    .await
    .expect("a scheduled job is promoted after a restart");
    assert_eq!(
        job.id, job_id,
        "the same scheduled job survives the restart"
    );
    assert_eq!(job.attempt, 1);
    ack(&client, &job).await;
}

// Dedup deadlines are pinned when the record is written. A shrunk window on
// the next boot must not re-admit a key whose pinned deadline is still in the
// future — the old floating-window check did exactly that, and the resulting
// re-insert left an orphaned timer that could later delete the fresh record.
#[tokio::test]
async fn dedup_deadline_survives_window_shrink_across_restart() {
    let db_path = temp_db("dedup-shrink");
    let dedup_req = || EnqueueRequest {
        idempotency_key: Some("pinned".to_string()),
        ..enqueue_req("smoke-dedup-shrink")
    };

    let first_id = {
        let (child, client) =
            spawn_server_with_env(&db_path, &[("SEPP_STORAGE__DEDUP_WINDOW_MS", "60000")]).await;
        let _server = ServerGuard {
            child,
            db_path: None,
            cfg_path: None,
        };
        enqueue(&client, dedup_req()).await.job_id
    };

    let (child, client) =
        spawn_server_with_env(&db_path, &[("SEPP_STORAGE__DEDUP_WINDOW_MS", "1")]).await;
    let _server = ServerGuard {
        child,
        db_path: Some(db_path),
        cfg_path: None,
    };

    let second = enqueue(&client, dedup_req()).await;
    assert!(
        second.deduplicated,
        "a key inside its pinned 60s deadline deduplicates even after the window shrinks to 1ms"
    );
    assert_eq!(second.job_id, first_id);
}

// The reverse direction: growing the window across a restart must not
// retroactively resurrect a record that already expired at its pinned
// deadline.
#[tokio::test]
async fn dedup_expiry_is_pinned_at_enqueue_across_restart() {
    let db_path = temp_db("dedup-grow");
    let dedup_req = || EnqueueRequest {
        idempotency_key: Some("pinned".to_string()),
        ..enqueue_req("smoke-dedup-grow")
    };

    let first_id = {
        let (child, client) =
            spawn_server_with_env(&db_path, &[("SEPP_STORAGE__DEDUP_WINDOW_MS", "50")]).await;
        let _server = ServerGuard {
            child,
            db_path: None,
            cfg_path: None,
        };
        enqueue(&client, dedup_req()).await.job_id
    };

    // Wait out the pinned 50ms deadline before the second boot.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (child, client) =
        spawn_server_with_env(&db_path, &[("SEPP_STORAGE__DEDUP_WINDOW_MS", "60000")]).await;
    let _server = ServerGuard {
        child,
        db_path: Some(db_path),
        cfg_path: None,
    };

    let second = enqueue(&client, dedup_req()).await;
    assert!(
        !second.deduplicated,
        "a key whose pinned deadline has passed is re-admitted even after the window grows"
    );
    assert_ne!(second.job_id, first_id, "the re-admitted job is a new job");
}

#[tokio::test]
async fn reserve_polls_queues_in_listed_order() {
    let (_guard, client) = start_server("order").await;

    let a = enqueue(&client, enqueue_req("smoke-order-a")).await;
    let b = enqueue(&client, enqueue_req("smoke-order-b")).await;

    // With both queues non-empty, a single-job reserve drains the first-listed
    // queue before the second.
    let first = reserve_batch(
        &client,
        &["smoke-order-a", "smoke-order-b"],
        LEASE,
        WAIT,
        Some(1),
    )
    .await
    .into_iter()
    .next()
    .expect("a job from the first non-empty queue");
    assert_eq!(first.id, a.job_id, "queue at index 0 is polled first");
    ack(&client, &first).await;

    let second = reserve_batch(
        &client,
        &["smoke-order-a", "smoke-order-b"],
        LEASE,
        WAIT,
        Some(1),
    )
    .await
    .into_iter()
    .next()
    .expect("then the second-listed queue");
    assert_eq!(second.id, b.job_id);
    ack(&client, &second).await;
}
