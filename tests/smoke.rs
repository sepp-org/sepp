use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime};

use sepp_rs::client::{RetryDirective, SeppClient};
use sepp_rs::{EnqueueAck, EnqueueRequest, Job, Payload, Primitive, Priority, ReserveOptions};

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

async fn spawn_server(db_path: &std::path::Path) -> (Child, SeppClient) {
    let port = free_port();
    let child = Command::new(env!("CARGO_BIN_EXE_sepp"))
        .env("SEPP_SERVER__LISTEN_ADDR", format!("127.0.0.1:{port}"))
        .env("SEPP_SERVER__DB_PATH", db_path)
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn sepp server");

    let addr = format!("http://127.0.0.1:{port}");
    for _ in 0..100 {
        if let Ok(client) = SeppClient::connect(addr.clone()).await {
            return (child, client);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server did not become reachable on {addr}");
}

async fn start_server() -> (ServerGuard, SeppClient) {
    let db_path = temp_db("main");
    let (child, client) = spawn_server(&db_path).await;
    (
        ServerGuard {
            child,
            db_path: Some(db_path),
        },
        client,
    )
}

fn enqueue_req(queue: &str) -> EnqueueRequest {
    EnqueueRequest::new(queue, "smoke-job").expect("valid enqueue request")
}

async fn enqueue_one(client: &SeppClient, req: EnqueueRequest) -> EnqueueAck {
    client
        .enqueue(req)
        .await
        .expect("enqueue RPC")
        .expect("job accepted")
}

async fn reserve(client: &SeppClient, queue: &str, lease: Duration, wait: Duration) -> Option<Job> {
    let opts = ReserveOptions::new([queue], lease)
        .expect("valid reserve options")
        .with_wait_timeout(wait);
    client
        .reserve(&opts)
        .await
        .expect("reserve RPC")
        .and_then(|jobs| jobs.into_iter().next())
}

#[tokio::test]
async fn smoke() {
    let (_guard, client) = start_server().await;

    let info = client.get_server_info().await.expect("server info");
    assert!(!info.version.is_empty(), "server reports a version");
    assert!(info.max_payload_size > 0, "server advertises a payload limit");
    assert!(
        !info.allowed_encodings.is_empty(),
        "server advertises supported encodings"
    );

    let ack = enqueue_one(
        &client,
        enqueue_req("smoke-basic").with_payload(Payload {
            data: b"hello".to_vec(),
            encoding: "text/plain".to_string(),
        }),
    )
    .await;
    assert!(!ack.deduplicated);

    let job = reserve(
        &client,
        "smoke-basic",
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .await
    .expect("basic job is reservable");
    assert_eq!(job.ctx.id, ack.job_id);
    assert_eq!(job.ctx.attempt, 1);
    assert_eq!(
        job.payload.as_ref().map(|p| p.data.as_slice()),
        Some(&b"hello"[..]),
    );
    client.ack(&job.ctx).await.expect("ack");

    let gone = reserve(
        &client,
        "smoke-basic",
        Duration::from_secs(30),
        Duration::from_millis(200),
    )
    .await;
    assert!(gone.is_none(), "acked job must not be redelivered");

    let first = enqueue_one(
        &client,
        enqueue_req("smoke-dedup").with_idempotency_key("k1"),
    )
    .await;
    assert!(!first.deduplicated);
    let second = enqueue_one(
        &client,
        enqueue_req("smoke-dedup").with_idempotency_key("k1"),
    )
    .await;
    assert!(
        second.deduplicated,
        "second enqueue with the same key is deduplicated"
    );
    assert_eq!(second.job_id, first.job_id);

    enqueue_one(&client, enqueue_req("smoke-nack")).await;
    let job = reserve(
        &client,
        "smoke-nack",
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .await
    .expect("nack job reservable");
    assert_eq!(job.ctx.attempt, 1);
    let dead = client
        .nack(&job.ctx, RetryDirective::Default, "smoke retry")
        .await
        .expect("nack");
    assert!(!dead, "first nack should not dead-letter");
    let retried = reserve(
        &client,
        "smoke-nack",
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .await
    .expect("nacked job is redelivered");
    assert_eq!(retried.ctx.attempt, 2);
    client.ack(&retried.ctx).await.expect("ack retry");

    enqueue_one(&client, enqueue_req("smoke-delay")).await;
    let job = reserve(
        &client,
        "smoke-delay",
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .await
    .expect("delay job reservable");
    assert_eq!(job.ctx.attempt, 1);
    let dead = client
        .nack(
            &job.ctx,
            RetryDirective::After(Duration::from_secs(3)),
            "retry later",
        )
        .await
        .expect("nack with delay");
    assert!(!dead, "a delayed nack retries, it does not dead-letter");
    let too_early = reserve(
        &client,
        "smoke-delay",
        Duration::from_secs(30),
        Duration::from_millis(200),
    )
    .await;
    assert!(
        too_early.is_none(),
        "a delayed-retry job is not reservable before its delay elapses"
    );
    let retried = reserve(
        &client,
        "smoke-delay",
        Duration::from_secs(30),
        Duration::from_secs(15),
    )
    .await
    .expect("a delayed-retry job becomes reservable after its delay");
    assert_eq!(
        retried.ctx.attempt, 2,
        "the delayed retry still increments the attempt"
    );
    client.ack(&retried.ctx).await.expect("ack delayed retry");

    let scheduled_for = SystemTime::now() + Duration::from_secs(3);
    enqueue_one(
        &client,
        enqueue_req("smoke-sched").with_scheduled_at(scheduled_for),
    )
    .await;
    let early = reserve(
        &client,
        "smoke-sched",
        Duration::from_secs(30),
        Duration::from_millis(200),
    )
    .await;
    assert!(
        early.is_none(),
        "scheduled job must not be reservable before its time"
    );
    let promoted = reserve(
        &client,
        "smoke-sched",
        Duration::from_secs(30),
        Duration::from_secs(15),
    )
    .await
    .expect("scheduled job becomes reservable after its schedule");
    client.ack(&promoted.ctx).await.expect("ack scheduled");

    enqueue_one(&client, enqueue_req("smoke-lease")).await;
    let leased = reserve(
        &client,
        "smoke-lease",
        Duration::from_secs(1),
        Duration::from_secs(5),
    )
    .await
    .expect("lease job reservable");
    assert_eq!(leased.ctx.attempt, 1);
    let held = reserve(
        &client,
        "smoke-lease",
        Duration::from_secs(1),
        Duration::from_millis(200),
    )
    .await;
    assert!(held.is_none(), "leased job must not be reservable");
    let redelivered = reserve(
        &client,
        "smoke-lease",
        Duration::from_secs(30),
        Duration::from_secs(15),
    )
    .await
    .expect("expired lease is redelivered");
    assert_eq!(
        redelivered.ctx.attempt, 2,
        "redelivery increments the attempt"
    );
    client.ack(&redelivered.ctx).await.expect("ack redelivered");

    let batch = client
        .enqueue_batch([
            enqueue_req("smoke-batch"),
            enqueue_req("smoke-batch"),
            enqueue_req("smoke-batch"),
        ])
        .await
        .expect("enqueue_batch RPC");
    assert!(batch.all_succeeded(), "every job in the batch is accepted");
    assert_eq!(
        batch.results().len(),
        3,
        "the batch returns one result per submitted job"
    );
    for _ in 0..3 {
        let job = reserve(
            &client,
            "smoke-batch",
            Duration::from_secs(30),
            Duration::from_secs(5),
        )
        .await
        .expect("each batched job is reservable");
        client.ack(&job.ctx).await.expect("ack batched job");
    }
    let drained = reserve(
        &client,
        "smoke-batch",
        Duration::from_secs(30),
        Duration::from_millis(200),
    )
    .await;
    assert!(drained.is_none(), "the batch holds exactly three jobs");

    enqueue_one(
        &client,
        enqueue_req("smoke-prio").with_priority(Priority::new(1).expect("valid priority")),
    )
    .await;
    let high = enqueue_one(
        &client,
        enqueue_req("smoke-prio").with_priority(Priority::MAX),
    )
    .await;
    let first = reserve(
        &client,
        "smoke-prio",
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .await
    .expect("priority job reservable");
    assert_eq!(
        first.ctx.id, high.job_id,
        "the higher-priority job is reserved before the lower one, regardless of enqueue order"
    );
    client.ack(&first.ctx).await.expect("ack high priority");
    let second = reserve(
        &client,
        "smoke-prio",
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .await
    .expect("lower-priority job reservable");
    client.ack(&second.ctx).await.expect("ack low priority");

    let early = enqueue_one(&client, enqueue_req("smoke-fifo")).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let late = enqueue_one(&client, enqueue_req("smoke-fifo")).await;
    let r1 = reserve(
        &client,
        "smoke-fifo",
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .await
    .expect("first fifo job reservable");
    assert_eq!(
        r1.ctx.id, early.job_id,
        "within one priority the earlier-enqueued job is reserved first"
    );
    let r2 = reserve(
        &client,
        "smoke-fifo",
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .await
    .expect("second fifo job reservable");
    assert_eq!(r2.ctx.id, late.job_id);
    client.ack(&r1.ctx).await.expect("ack fifo 1");
    client.ack(&r2.ctx).await.expect("ack fifo 2");

    enqueue_one(
        &client,
        enqueue_req("smoke-dlq").with_max_attempts(2),
    )
    .await;
    let a1 = reserve(
        &client,
        "smoke-dlq",
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .await
    .expect("dlq job attempt 1 reservable");
    assert_eq!(a1.ctx.attempt, 1);
    let dead1 = client
        .nack(&a1.ctx, RetryDirective::Default, "fail 1")
        .await
        .expect("nack 1");
    assert!(!dead1, "attempt 1 of 2 is retried, not dead-lettered");
    let a2 = reserve(
        &client,
        "smoke-dlq",
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .await
    .expect("dlq job attempt 2 reservable");
    assert_eq!(a2.ctx.attempt, 2);
    let dead2 = client
        .nack(&a2.ctx, RetryDirective::Default, "fail 2")
        .await
        .expect("nack 2");
    assert!(
        dead2,
        "nacking the final attempt exhausts max_attempts and dead-letters the job"
    );
    let gone = reserve(
        &client,
        "smoke-dlq",
        Duration::from_secs(30),
        Duration::from_millis(200),
    )
    .await;
    assert!(gone.is_none(), "a dead-lettered job is not redelivered");

    enqueue_one(&client, enqueue_req("smoke-dlq-force")).await;
    let job = reserve(
        &client,
        "smoke-dlq-force",
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .await
    .expect("force-dlq job reservable");
    assert_eq!(job.ctx.attempt, 1);
    let dead = client
        .nack(&job.ctx, RetryDirective::DeadLetter, "drop it")
        .await
        .expect("nack");
    assert!(
        dead,
        "a DeadLetter nack dead-letters immediately, before attempts are exhausted"
    );
    let gone = reserve(
        &client,
        "smoke-dlq-force",
        Duration::from_secs(30),
        Duration::from_millis(200),
    )
    .await;
    assert!(
        gone.is_none(),
        "a force-dead-lettered job is not redelivered"
    );

    let mq_job = enqueue_one(&client, enqueue_req("smoke-mq-b")).await;
    let mq_opts = ReserveOptions::new(["smoke-mq-a", "smoke-mq-b"], Duration::from_secs(30))
        .expect("valid reserve options")
        .with_wait_timeout(Duration::from_secs(5));
    let claimed = client
        .reserve(&mq_opts)
        .await
        .expect("multi-queue reserve RPC")
        .and_then(|jobs| jobs.into_iter().next())
        .expect("a reserve over several queues claims from whichever has work");
    assert_eq!(
        claimed.ctx.id, mq_job.job_id,
        "the multi-queue reserve claims the job from the non-empty queue"
    );
    client.ack(&claimed.ctx).await.expect("ack multi-queue job");

    for _ in 0..5 {
        enqueue_one(&client, enqueue_req("smoke-rbatch")).await;
    }
    let batch_opts = ReserveOptions::new(["smoke-rbatch"], Duration::from_secs(30))
        .expect("valid reserve options")
        .with_wait_timeout(Duration::from_secs(5))
        .with_max_jobs(3);
    let first_batch = client
        .reserve(&batch_opts)
        .await
        .expect("batch reserve RPC")
        .expect("a non-empty queue yields a batch");
    assert_eq!(
        first_batch.len(),
        3,
        "a batch reserve claims at most max_jobs in one call"
    );
    for job in &first_batch {
        client.ack(&job.ctx).await.expect("ack batch-reserved job");
    }
    let rest = client
        .reserve(&batch_opts)
        .await
        .expect("batch reserve RPC")
        .expect("the queue still has jobs");
    assert_eq!(
        rest.len(),
        2,
        "a batch reserve returns whatever is available, up to max_jobs"
    );
    for job in &rest {
        client.ack(&job.ctx).await.expect("ack batch-reserved job");
    }

    let mut custom = HashMap::new();
    custom.insert(
        "region".to_string(),
        Primitive::String("eu-west".to_string()),
    );
    custom.insert("retries".to_string(), Primitive::Int(7));
    enqueue_one(
        &client,
        enqueue_req("smoke-custom")
            .with_payload(Payload {
                data: b"{}".to_vec(),
                encoding: "application/json".to_string(),
            })
            .with_custom(custom),
    )
    .await;
    let job = reserve(
        &client,
        "smoke-custom",
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .await
    .expect("custom job reservable");
    let payload = job.payload.as_ref().expect("payload round-trips");
    assert_eq!(
        payload.encoding, "application/json",
        "payload encoding round-trips intact"
    );
    assert_eq!(
        job.ctx.custom.get("region"),
        Some(&Primitive::String("eu-west".to_string())),
        "custom string field round-trips intact"
    );
    assert_eq!(
        job.ctx.custom.get("retries"),
        Some(&Primitive::Int(7)),
        "custom int field round-trips intact"
    );
    client.ack(&job.ctx).await.expect("ack custom job");

    enqueue_one(&client, enqueue_req("smoke-extend")).await;
    let leased = reserve(
        &client,
        "smoke-extend",
        Duration::from_secs(1),
        Duration::from_secs(5),
    )
    .await
    .expect("extend job reservable");
    assert_eq!(leased.ctx.attempt, 1);
    client
        .extend(&leased.ctx, Duration::from_secs(30))
        .await
        .expect("extend RPC");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let still_held = reserve(
        &client,
        "smoke-extend",
        Duration::from_secs(1),
        Duration::from_millis(200),
    )
    .await;
    assert!(
        still_held.is_none(),
        "an extended lease keeps the job from being redelivered past its original deadline"
    );
    client.ack(&leased.ctx).await.expect("ack extended job");
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
        enqueue_one(&client, enqueue_req("smoke-restart")).await.job_id
    };

    let (child, client) = spawn_server(&db_path).await;
    let _server = ServerGuard {
        child,
        db_path: Some(db_path),
    };

    let job = reserve(
        &client,
        "smoke-restart",
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .await
    .expect("a job enqueued before a restart is still reservable after it");
    assert_eq!(
        job.ctx.id, job_id,
        "the same job survives a server restart"
    );
    assert_eq!(job.ctx.attempt, 1);
    client.ack(&job.ctx).await.expect("ack restart job");
}
