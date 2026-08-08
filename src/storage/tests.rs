use super::*;
use crate::config::RetryBackoff;
use crate::keys::{
    DeadLetterKey, DedupTimerKey, ReadyKey, TimerKey, closing_key, deadline_of, queue_prefix,
};
use crate::metrics::CycleMetrics;
use crate::pb::sepp::v1::job_rejection;
use crate::queues::RetryPolicy;
use uuid::Uuid;

// So that each test doesnt have to spell out the entire struct
fn ready_key(queue: &str, priority: u32, enqueued_at: i64, job_id: &str) -> Vec<u8> {
    ReadyKey {
        queue,
        priority,
        enqueued_at,
        job_id,
    }
    .encode()
}

fn job_id_of<'a>(_queue: &str, ready_k: &'a [u8]) -> &'a str {
    ReadyKey::decode(ready_k).unwrap().job_id
}

fn timer_key(deadline: i64, job_id: &str) -> Vec<u8> {
    TimerKey { deadline, job_id }.encode()
}

fn dedup_timer_key(deadline: i64, dedup_key: &[u8]) -> Vec<u8> {
    DedupTimerKey {
        deadline,
        dedup_key,
    }
    .encode()
}

fn dead_letter_key(failed_at: i64, queue: &str, job_id: &[u8]) -> Vec<u8> {
    DeadLetterKey {
        failed_at,
        queue,
        job_id,
    }
    .encode()
}

fn retry_limits(base: u64, backoff: RetryBackoff, max: u64) -> RetryPolicy {
    RetryPolicy {
        retry_delay_ms: base,
        retry_backoff: backoff,
        retry_delay_max_ms: max,
        max_schedule_horizon_ms: crate::config::LimitsConfig::default().max_schedule_horizon_ms,
    }
}

#[test]
fn retry_policy_zero_base_is_immediate() {
    let limits = retry_limits(0, RetryBackoff::Exponential, 60_000);
    assert_eq!(policy_retry_delay_ms(&limits, 1, "job-1"), 0);
    assert_eq!(policy_retry_delay_ms(&limits, 7, "job-1"), 0);
}

#[test]
fn retry_policy_fixed_delay_jitters_within_a_quarter() {
    let limits = retry_limits(10_000, RetryBackoff::None, 60_000);
    for attempt in 1..=5 {
        let d = policy_retry_delay_ms(&limits, attempt, "job-1");
        assert!((7_500..=10_000).contains(&d), "attempt {attempt}: {d}");
        assert_eq!(
            d,
            policy_retry_delay_ms(&limits, attempt, "job-1"),
            "same (job, attempt) must always produce the same delay"
        );
    }
}

#[test]
fn retry_policy_exponential_doubles_and_caps() {
    let limits = retry_limits(1_000, RetryBackoff::Exponential, 10_000);
    let d1 = policy_retry_delay_ms(&limits, 1, "job-1");
    let d3 = policy_retry_delay_ms(&limits, 3, "job-1");
    assert!((750..=1_000).contains(&d1), "{d1}");
    assert!((3_000..=4_000).contains(&d3), "{d3}");
    // 2^59 growth saturates instead of overflowing, and the cap stays hard.
    for attempt in [5, 8, 60] {
        let d = policy_retry_delay_ms(&limits, attempt, "job-1");
        assert!((7_500..=10_000).contains(&d), "attempt {attempt}: {d}");
    }
    // At the cap the jitter must keep spreading jobs; a batch that failed
    // together must not retry in lockstep forever.
    let at_cap: std::collections::HashSet<u64> = (0..20)
        .map(|i| policy_retry_delay_ms(&limits, 60, &format!("job-{i}")))
        .collect();
    assert!(
        at_cap.len() > 10,
        "expected spread at the cap, got {at_cap:?}"
    );
}

#[test]
fn retry_policy_respects_schedule_horizon() {
    let mut limits = retry_limits(8_000, RetryBackoff::None, 60_000);
    limits.max_schedule_horizon_ms = 5_000;
    let d = policy_retry_delay_ms(&limits, 1, "job-1");
    assert!((3_750..=5_000).contains(&d), "{d}");
}

#[test]
fn retry_policy_jitter_spreads_jobs() {
    let limits = retry_limits(100_000, RetryBackoff::None, 1_000_000);
    let delays: std::collections::HashSet<u64> = (0..20)
        .map(|i| policy_retry_delay_ms(&limits, 1, &format!("job-{i}")))
        .collect();
    assert!(delays.len() > 10, "expected spread, got {delays:?}");
}

#[test]
fn reject_flags_only_storage_errors_as_fatal() {
    let (tx, _rx) = oneshot::channel::<Result<(), Status>>();
    assert!(reject(tx, Status::not_found("job not found")).is_ok());

    let (tx, _rx) = oneshot::channel::<Result<(), Status>>();
    assert!(reject(tx, Status::failed_precondition("attempt mismatch")).is_ok());

    let (tx, _rx) = oneshot::channel::<Result<(), Status>>();
    assert!(reject(tx, Status::resource_exhausted("queue full")).is_ok());

    let (tx, _rx) = oneshot::channel::<Result<(), Status>>();
    let fatal = reject(tx, Status::internal("storage error"));
    assert_eq!(fatal.unwrap_err().code(), tonic::Code::Internal);
}

#[test]
fn reject_answers_the_caller_with_the_original_error() {
    let (tx, mut rx) = oneshot::channel::<Result<(), Status>>();
    let _ = reject(tx, Status::internal("storage error"));
    let sent = rx.try_recv().expect("responder is answered immediately");
    assert_eq!(sent.unwrap_err().code(), tonic::Code::Internal);
}

#[test]
fn timer_index_iter_oldest_walks_in_order() {
    let mut idx = TimerIndex::default();
    idx.insert(dead_letter_key(300, "qb", b"c"), "qb");
    idx.insert(dead_letter_key(100, "qa", b"a"), "qa");
    idx.insert(dead_letter_key(200, "qa", b"b"), "qa");
    let order: Vec<(i64, &str)> = idx
        .iter_oldest()
        .map(|(k, q)| (deadline_of(k), q))
        .collect();
    assert_eq!(order, vec![(100, "qa"), (200, "qa"), (300, "qb")]);
}

#[test]
fn ready_index_pops_highest_priority_first() {
    let mut idx = ReadyIndex::default();
    idx.insert(ready_key("q", 0, 100, "low"), 1);
    idx.insert(ready_key("q", 9, 100, "high"), 1);
    idx.insert(ready_key("q", 5, 100, "mid"), 1);

    let prefix = queue_prefix("q");
    let (k, _) = idx.pop_front(&prefix).unwrap();
    assert_eq!(job_id_of("q", &k), "high");
    let (k, _) = idx.pop_front(&prefix).unwrap();
    assert_eq!(job_id_of("q", &k), "mid");
    let (k, _) = idx.pop_front(&prefix).unwrap();
    assert_eq!(job_id_of("q", &k), "low");
    assert!(idx.pop_front(&prefix).is_none());
}

#[test]
fn ready_index_is_fifo_within_a_priority() {
    let mut idx = ReadyIndex::default();
    idx.insert(ready_key("q", 5, 200, "second"), 1);
    idx.insert(ready_key("q", 5, 100, "first"), 1);

    let prefix = queue_prefix("q");
    let (k, _) = idx.pop_front(&prefix).unwrap();
    assert_eq!(job_id_of("q", &k), "first");
    let (k, _) = idx.pop_front(&prefix).unwrap();
    assert_eq!(job_id_of("q", &k), "second");
}

#[test]
fn ready_index_isolates_queues() {
    let mut idx = ReadyIndex::default();
    idx.insert(ready_key("qa", 5, 100, "a-job"), 1);
    idx.insert(ready_key("qb", 5, 100, "b-job"), 1);

    let (k, _) = idx.pop_front(&queue_prefix("qa")).unwrap();
    assert_eq!(job_id_of("qa", &k), "a-job");
    assert!(idx.pop_front(&queue_prefix("qa")).is_none());

    let (k, _) = idx.pop_front(&queue_prefix("qb")).unwrap();
    assert_eq!(job_id_of("qb", &k), "b-job");
}

#[test]
fn ready_index_does_not_leak_across_queue_name_prefixes() {
    let mut idx = ReadyIndex::default();
    idx.insert(ready_key("aa", 5, 100, "aa-job"), 1);

    assert!(idx.pop_front(&queue_prefix("a")).is_none());
    let (k, _) = idx.pop_front(&queue_prefix("aa")).unwrap();
    assert_eq!(job_id_of("aa", &k), "aa-job");
}

#[test]
fn ready_index_preserves_the_attempt() {
    let mut idx = ReadyIndex::default();
    idx.insert(ready_key("q", 5, 100, "j"), 7);
    let (_, attempt) = idx.pop_front(&queue_prefix("q")).unwrap();
    assert_eq!(attempt, 7);
}

#[test]
fn timer_index_pops_in_deadline_order() {
    let mut idx = TimerIndex::default();
    idx.insert(timer_key(300, "c"), "q");
    idx.insert(timer_key(100, "a"), "q");
    idx.insert(timer_key(200, "b"), "q");

    assert_eq!(
        idx.pop_due(i64::MAX),
        Some((timer_key(100, "a"), "q".to_string()))
    );
    assert_eq!(
        idx.pop_due(i64::MAX),
        Some((timer_key(200, "b"), "q".to_string()))
    );
    assert_eq!(
        idx.pop_due(i64::MAX),
        Some((timer_key(300, "c"), "q".to_string()))
    );
    assert_eq!(idx.pop_due(i64::MAX), None);
}

#[test]
fn timer_index_pop_due_respects_the_now_boundary() {
    let mut idx = TimerIndex::default();
    idx.insert(timer_key(100, "a"), "q");

    assert_eq!(idx.pop_due(99), None);
    assert_eq!(
        idx.pop_due(100),
        Some((timer_key(100, "a"), "q".to_string()))
    );
}

#[test]
fn timer_index_only_yields_due_entries() {
    let mut idx = TimerIndex::default();
    idx.insert(timer_key(100, "a"), "q");
    idx.insert(timer_key(500, "b"), "q");

    assert_eq!(
        idx.pop_due(200),
        Some((timer_key(100, "a"), "q".to_string()))
    );
    assert_eq!(idx.pop_due(200), None);
    assert_eq!(
        idx.pop_due(500),
        Some((timer_key(500, "b"), "q".to_string()))
    );
}

#[test]
fn timer_index_earliest_reports_the_lowest_deadline() {
    let mut idx = TimerIndex::default();
    assert_eq!(idx.earliest(), None);

    idx.insert(timer_key(300, "c"), "q");
    idx.insert(timer_key(100, "a"), "q");
    idx.insert(timer_key(200, "b"), "q");
    assert_eq!(idx.earliest(), Some(100));

    idx.remove(&timer_key(100, "a"));
    assert_eq!(idx.earliest(), Some(200));
}

#[test]
fn next_deadline_is_the_minimum_across_every_index() {
    let mut indexes = Indexes::default();
    assert_eq!(next_deadline(&indexes, 0), None);

    indexes.scheduled.insert(timer_key(500, "s"), "q");
    indexes.leases.insert(timer_key(200, "l"), "q");
    indexes.dedup_timers.insert(dedup_timer_key(800, b"d"), "q");
    assert_eq!(next_deadline(&indexes, 0), Some(200));

    indexes.leases.remove(&timer_key(200, "l"));
    assert_eq!(next_deadline(&indexes, 0), Some(500));

    // The oldest dead-letter contributes failed_at + retention to the min.
    indexes
        .dead_letter
        .insert(dead_letter_key(100, "q", b"d"), "q");
    assert_eq!(
        next_deadline(&indexes, 50),
        Some(150),
        "oldest failed_at (100) + retention (50) becomes the minimum"
    );
    // With retention disabled, the dead-letter term drops out entirely.
    assert_eq!(next_deadline(&indexes, 0), Some(500));
}

#[test]
fn timer_index_remove_drops_the_entry() {
    let mut idx = TimerIndex::default();
    idx.insert(timer_key(100, "a"), "q");
    idx.remove(&timer_key(100, "a"));
    assert_eq!(idx.pop_due(i64::MAX), None);
}

#[test]
fn timer_index_tracks_per_queue_depths() {
    let mut idx = TimerIndex::default();
    idx.insert(timer_key(100, "a"), "qa");
    idx.insert(timer_key(200, "b"), "qa");
    idx.insert(timer_key(300, "c"), "qb");
    assert_eq!(idx.by_queue.get("qa").copied(), Some(2));
    assert_eq!(idx.by_queue.get("qb").copied(), Some(1));

    idx.remove(&timer_key(100, "a"));
    assert_eq!(idx.by_queue.get("qa").copied(), Some(1));

    idx.pop_due(i64::MAX);
    assert_eq!(idx.by_queue.get("qa").copied(), None);
    assert_eq!(idx.by_queue.get("qb").copied(), Some(1));
}

#[test]
fn ready_index_tracks_per_queue_depths() {
    let mut idx = ReadyIndex::default();
    idx.insert(ready_key("qa", 5, 100, "j1"), 1);
    idx.insert(ready_key("qa", 5, 200, "j2"), 1);
    idx.insert(ready_key("qb", 5, 100, "j3"), 1);
    assert_eq!(idx.by_queue.get("qa").copied(), Some(2));
    assert_eq!(idx.by_queue.get("qb").copied(), Some(1));

    idx.pop_front(&queue_prefix("qa"));
    assert_eq!(idx.by_queue.get("qa").copied(), Some(1));
    idx.pop_front(&queue_prefix("qa"));
    assert_eq!(idx.by_queue.get("qa").copied(), None);
}

#[test]
fn peek_keys_pages_ready_with_a_cursor() {
    let mut indexes = Indexes::default();
    for i in 0..5i64 {
        indexes
            .ready
            .insert(ready_key("q", 5, 100 + i, &format!("j{i}")), 1);
    }
    indexes.ready.insert(ready_key("other", 5, 100, "x"), 1);

    let mut seen = Vec::new();
    let mut cursor = None;
    loop {
        let page = peek_keys(&indexes, PeekState::Ready, "q", cursor, 2);
        assert!(!page.truncated);
        assert!(page.keys.len() <= 2);
        seen.extend(page.keys);
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    let expected: Vec<Vec<u8>> = (0..5i64)
        .map(|i| ready_key("q", 5, 100 + i, &format!("j{i}")))
        .collect();
    assert_eq!(seen, expected, "pages walk every key once, in order");
}

#[test]
fn peek_keys_truncates_at_the_examined_cap() {
    let mut indexes = Indexes::default();
    for i in 0..PEEK_EXAMINE_CAP as i64 {
        indexes
            .dead_letter
            .insert(dead_letter_key(i, "noise", b"j"), "noise");
    }
    let wanted = dead_letter_key(PEEK_EXAMINE_CAP as i64, "wanted", b"j");
    indexes.dead_letter.insert(wanted.clone(), "wanted");

    let page = peek_keys(&indexes, PeekState::DeadLetter, "wanted", None, 10);
    assert!(page.truncated, "the cap hits before the page fills");
    assert!(page.keys.is_empty());
    let cursor = page
        .next_cursor
        .expect("a truncated page carries a resume cursor");

    let resumed = peek_keys(&indexes, PeekState::DeadLetter, "wanted", Some(cursor), 10);
    assert!(!resumed.truncated);
    assert_eq!(resumed.keys, vec![wanted]);
    assert_eq!(resumed.next_cursor, None);
}

#[test]
fn admin_totals_fold_only_the_five_rpc_counters() {
    let mut m = CycleMetrics::default();
    m.enqueued_by_queue.insert("qa".into(), 3);
    m.reserved_by_queue.insert("qa".into(), 2);
    m.acked_by_queue.insert("qa".into(), 1);
    m.nacked_by_queue.insert("qb".into(), 4);
    m.dead_lettered_by_queue_cause
        .insert(("qb".into(), "rejected"), 1);
    m.dead_lettered_by_queue_cause
        .insert(("qb".into(), "attempts_exhausted"), 2);
    m.sweep_promotions_by_queue.insert("qc".into(), 9);

    let mut totals = HashMap::new();
    let mut last_active = HashMap::new();
    fold_admin_totals(&mut totals, &mut last_active, &m);
    fold_admin_totals(&mut totals, &mut last_active, &m);

    let qa = &totals["qa"];
    assert_eq!(
        (
            qa.enqueued,
            qa.reserved,
            qa.acked,
            qa.nacked,
            qa.dead_lettered
        ),
        (6, 4, 2, 0, 0)
    );
    let qb = &totals["qb"];
    assert_eq!((qb.nacked, qb.dead_lettered), (8, 6), "causes are summed");
    assert!(
        !totals.contains_key("qc"),
        "sweep counters do not feed totals"
    );
    assert!(last_active.contains_key("qa") && last_active.contains_key("qb"));
}

#[test]
fn open_refuses_unknown_format_version() {
    let path = std::env::temp_dir().join(format!("sepp-storage-test-{}", Uuid::new_v4()));
    // Pre-stamp the database with a format version this binary doesn't know.
    {
        let db = TxDatabase::builder(&path).open().expect("open db");
        let meta = db
            .keyspace("meta", KeyspaceCreateOptions::default)
            .expect("create meta keyspace");
        let mut tx = db.write_tx();
        tx.insert(
            &meta,
            b"format_version".to_vec(),
            2u64.to_be_bytes().to_vec(),
        );
        tx.commit().expect("commit version stamp");
        db.persist(PersistMode::SyncAll).expect("persist");
    }

    let mut config = Config::default();
    config.server.db_path = path.to_str().expect("utf-8 temp path").to_string();
    let registry = crate::queues::QueueRegistry::from_config(&config).into_shared();
    let err = Storage::open(&config, registry, Metrics::new(false))
        .err()
        .expect("open must refuse an unknown format version");
    assert!(
        err.to_string().contains("format version"),
        "unexpected error: {err}"
    );

    let _ = std::fs::remove_dir_all(&path);
}

// Stamps a cluster identity the way a first cluster-enabled boot would.
fn stamp_identity(path: &std::path::Path, node_id: u64) {
    let db = TxDatabase::builder(path).open().expect("open db");
    let raft = db
        .keyspace("raft", KeyspaceCreateOptions::default)
        .expect("create raft keyspace");
    let mut tx = db.write_tx();
    tx.insert(&raft, b"node_id".to_vec(), node_id.to_be_bytes().to_vec());
    tx.insert(
        &raft,
        b"instance_uuid".to_vec(),
        Uuid::new_v4().as_bytes().to_vec(),
    );
    tx.commit().expect("commit identity");
    db.persist(PersistMode::SyncAll).expect("persist");
}

#[test]
fn open_refuses_a_cluster_node_id_mismatch() {
    let path = std::env::temp_dir().join(format!("sepp-storage-test-{}", Uuid::new_v4()));
    stamp_identity(&path, 7);

    let mut config = Config::default();
    config.server.db_path = path.to_str().expect("utf-8 temp path").to_string();
    config.cluster.enabled = true;
    let registry = crate::queues::QueueRegistry::from_config(&config).into_shared();
    let err = Storage::open(&config, registry, Metrics::new(false))
        .err()
        .expect("open must refuse a node_id mismatch");
    assert!(
        err.to_string().contains("node_id"),
        "unexpected error: {err}"
    );

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn cluster_identity_is_ignored_when_cluster_is_disabled() {
    let path = std::env::temp_dir().join(format!("sepp-storage-test-{}", Uuid::new_v4()));
    stamp_identity(&path, 7);

    // Disabled cluster mode never reads the raft keyspace, so the
    // mismatched stamp is invisible: exactly today's behavior.
    let mut config = Config::default();
    config.server.db_path = path.to_str().expect("utf-8 temp path").to_string();
    let registry = crate::queues::QueueRegistry::from_config(&config).into_shared();
    let _storage = Storage::open(&config, registry, Metrics::new(false))
        .expect("disabled cluster mode ignores identity stamps");

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn prepare_jobs_resolves_live_limits_and_boot_dedup_window() {
    let path = std::env::temp_dir().join(format!("sepp-storage-test-{}", Uuid::new_v4()));
    let mut boot_cfg = Config::default();
    boot_cfg.server.db_path = path.to_str().expect("utf-8 temp path").to_string();
    boot_cfg.storage.dedup_window_ms = 1;
    let registry = QueueRegistry::from_config(&boot_cfg).into_shared();
    let storage =
        Storage::open(&boot_cfg, registry.clone(), Metrics::new(false)).expect("open storage");

    // A hot reload changes both knobs; only the priority may follow it.
    let mut live_cfg = Config::default();
    live_cfg.storage.dedup_window_ms = 3_600_000;
    live_cfg.limits.default_priority = 7;
    crate::queues::publish(&registry, QueueRegistry::from_config(&live_cfg));

    let jobs = storage.prepare_jobs(vec![test_req("q")]);
    assert_eq!(jobs[0].limits.priority, 7, "live limits follow the reload");
    assert_eq!(
        jobs[0].limits.dedup_window_ms, 1,
        "the dedup window stays boot-pinned"
    );

    drop(storage);
    let _ = std::fs::remove_dir_all(&path);
}

fn open_test_store() -> Store {
    let path = std::env::temp_dir().join(format!("sepp-storage-test-{}", Uuid::new_v4()));
    let db = TxDatabase::builder(path)
        .temporary(true)
        .open()
        .expect("open temporary db");
    let opts = KeyspaceCreateOptions::default;

    Store {
        jobs: db.keyspace("jobs", opts).unwrap(),
        payloads: db.keyspace("payloads", opts).unwrap(),
        inflight: db.keyspace("inflight", opts).unwrap(),
        ready: db.keyspace("ready", opts).unwrap(),
        dedup: db.keyspace("dedup", opts).unwrap(),
        dedup_timers: db.keyspace("dedup_timers", opts).unwrap(),
        scheduled: db.keyspace("scheduled", opts).unwrap(),
        leases: db.keyspace("leases", opts).unwrap(),
        dead_letter: db.keyspace("dead_letter", opts).unwrap(),
        meta: db.keyspace("meta", opts).unwrap(),
        audit: db.keyspace("audit", opts).unwrap(),
        db,
        params: StorageParams {
            persist_mode: PersistMode::Buffer,
            sweep_limit: 1000,
            dead_letter_retention_ms: 0,
            admin_enabled: true,
        },
        metrics: Metrics::new(false),
    }
}

#[test]
fn purge_refuses_while_jobs_are_inflight() {
    let store = open_test_store();
    let mut indexes = Indexes::default();
    indexes.leases.insert(timer_key(500, "j1"), "q");

    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(false);
    let err = apply_purge_queue_chunk(&store, &mut indexes, &mut tx, &mut cycle, "q", 100)
        .expect_err("in-flight jobs must block a purge");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(!cycle.dirty, "a refused purge writes nothing");

    indexes.leases.remove(&timer_key(500, "j1"));
    let outcome = apply_purge_queue_chunk(&store, &mut indexes, &mut tx, &mut cycle, "q", 100)
        .expect("an idle queue purges");
    assert_eq!(outcome.purged, 0);
    assert!(!outcome.remaining);
}

fn test_req(queue: &str) -> EnqueueRequest {
    EnqueueRequest {
        queue: queue.to_string(),
        job_type: "t".to_string(),
        ..Default::default()
    }
}

fn test_job(queue: &str) -> PreparedJob {
    let registry = QueueRegistry::from_config(&Config::default());
    PreparedJob::new(test_req(queue), &registry, &registry)
}

#[test]
fn enqueue_rejects_jobs_for_a_closing_queue() {
    let store = open_test_store();
    let mut indexes = Indexes::default();
    indexes.closing.insert("q".into(), now_ms() + 60_000);

    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(false);
    let results = apply_enqueue(
        &store,
        &mut indexes,
        &mut tx,
        &mut cycle,
        vec![test_job("q"), test_job("other")],
        now_ms(),
    )
    .expect("enqueue applies");

    match &results[0] {
        Err(JobRejection {
            reason: Some(job_rejection::Reason::QueueClosing(r)),
        }) => assert_eq!(r.queue, "q"),
        other => panic!("expected a QueueClosing rejection, got {other:?}"),
    }
    assert!(results[1].is_ok(), "other queues are unaffected");

    // An expired tombstone (its delete handler died) no longer rejects.
    indexes.closing.insert("q".into(), now_ms() - 1);
    let results = apply_enqueue(
        &store,
        &mut indexes,
        &mut tx,
        &mut cycle,
        vec![test_job("q")],
        now_ms(),
    )
    .expect("enqueue applies");
    assert!(results[0].is_ok());
}

#[test]
fn atomic_enqueue_rejects_the_whole_batch_for_a_closing_queue() {
    let store = open_test_store();
    let mut indexes = Indexes::default();
    indexes.closing.insert("q".into(), now_ms() + 60_000);

    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(false);
    let outcome = apply_enqueue_atomic(
        &store,
        &mut indexes,
        &mut tx,
        &mut cycle,
        vec![test_job("other"), test_job("q")],
        now_ms(),
    )
    .expect("atomic enqueue applies");

    match outcome {
        AtomicEnqueueOutcome::Rejected(rejections) => {
            assert_eq!(rejections.len(), 1);
            assert_eq!(rejections[0].0, 1, "the offending index is reported");
            assert!(matches!(
                rejections[0].1.reason,
                Some(job_rejection::Reason::QueueClosing(_))
            ));
        }
        AtomicEnqueueOutcome::Committed(_) => panic!("the batch must not commit"),
    }
    assert_eq!(
        indexes.live_depth("other"),
        0,
        "nothing from the rejected batch is inserted"
    );
}

#[test]
fn sweep_drops_only_expired_close_tombstones() {
    let store = open_test_store();
    let mut indexes = Indexes::default();
    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(false);
    apply_close_queue(
        &store,
        &mut indexes,
        &mut tx,
        &mut cycle,
        "active".into(),
        now_ms(),
        CLOSE_GRACE_MS,
    );
    // An expired tombstone, as if its delete handler died a while ago.
    indexes.closing.insert("abandoned".into(), now_ms() - 1);
    tx.insert(
        &store.meta,
        closing_key("abandoned"),
        (now_ms() - 1).to_be_bytes().to_vec(),
    );

    apply_sweep(
        &store,
        &mut indexes,
        &mut tx,
        &mut cycle,
        now_ms(),
        1000,
        None,
        false,
    )
    .expect("sweep applies");

    assert!(!indexes.closing.contains_key("abandoned"));
    assert!(indexes.closing.contains_key("active"));
    let row = |queue| tx.get(&store.meta, closing_key(queue)).expect("meta reads");
    assert!(row("abandoned").is_none(), "the expired row is deleted");
    assert!(row("active").is_some(), "the live row survives");
}

#[test]
fn sweep_reads_the_retention_cutoff_from_the_op() {
    // The test store has dead_letter_retention_ms = 0; a cutoff riding in
    // the op must still expire dead letters, because a future follower
    // applies with the leader's scalars, not its own config.
    let store = open_test_store();
    let mut indexes = Indexes::default();
    let key = dead_letter_key(100, "q", b"j1");
    indexes.dead_letter.insert(key.clone(), "q");

    let mut tx = store.db.write_tx();
    tx.insert(&store.dead_letter, key.clone(), Vec::new());
    let mut cycle = Cycle::new(false);
    let processed = apply_sweep(
        &store,
        &mut indexes,
        &mut tx,
        &mut cycle,
        1_000,
        1000,
        Some(200),
        true,
    )
    .expect("sweep applies");

    assert_eq!(processed, 1);
    assert!(indexes.dead_letter.keys.is_empty());
    assert!(
        tx.get(&store.dead_letter, &key).expect("reads").is_none(),
        "the expired dead letter is deleted"
    );
}

#[test]
fn close_tombstone_survives_a_rebuild() {
    let store = open_test_store();
    let mut indexes = Indexes::default();
    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(false);
    apply_close_queue(
        &store,
        &mut indexes,
        &mut tx,
        &mut cycle,
        "q".into(),
        now_ms(),
        CLOSE_GRACE_MS,
    );
    assert!(cycle.dirty, "a close writes its tombstone durably");
    tx.commit().expect("commit");

    // As after a mid-purge restart: the rebuilt indexes still reject.
    let rebuilt = rebuild_indexes(&store).expect("rebuild");
    assert_eq!(rebuilt.closing.get("q"), indexes.closing.get("q"));

    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(false);
    apply_open_queue(&store, &mut indexes, &mut tx, &mut cycle, "q");
    assert!(cycle.dirty);
    tx.commit().expect("commit");

    let rebuilt = rebuild_indexes(&store).expect("rebuild");
    assert!(rebuilt.closing.is_empty(), "open deletes the tombstone row");

    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(false);
    apply_open_queue(&store, &mut indexes, &mut tx, &mut cycle, "q");
    assert!(
        !cycle.dirty,
        "opening a queue that isn't closing is a no-op"
    );
}

fn audit_record(actor: &str, action: &str) -> AuditRecord {
    AuditRecord {
        actor: actor.into(),
        role: "admin".into(),
        action: action.into(),
        details_json: "{}".into(),
    }
}

fn audit_read_handle(store: &Store) -> ReadHandle {
    ReadHandle {
        db: store.db.clone(),
        jobs: store.jobs.clone(),
        payloads: store.payloads.clone(),
        inflight: store.inflight.clone(),
        ready: store.ready.clone(),
        scheduled: store.scheduled.clone(),
        dead_letter: store.dead_letter.clone(),
        audit: store.audit.clone(),
    }
}

#[test]
fn audit_appends_read_back_newest_first_with_cursor() {
    let store = open_test_store();

    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(false);
    apply_audit_append(
        &store,
        &mut tx,
        &mut cycle,
        &audit_record("root", "a.one"),
        1_000,
    )
    .expect("applies");
    apply_audit_append(
        &store,
        &mut tx,
        &mut cycle,
        &audit_record("root", "a.two"),
        1_000,
    )
    .expect("applies");
    assert!(cycle.dirty);
    tx.commit().expect("commit");

    // A later cycle continues from the persisted counter.
    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(false);
    apply_audit_append(
        &store,
        &mut tx,
        &mut cycle,
        &audit_record("root", "a.three"),
        2_000,
    )
    .expect("applies");
    tx.commit().expect("commit");

    let read = audit_read_handle(&store);
    let all = AuditFilter::default();
    let brief = |page: AuditPage| {
        page.entries
            .into_iter()
            .map(|e| (e.seq, e.ts_ms, e.record.action))
            .collect::<Vec<_>>()
    };

    let first = read.list_audit(None, 2, &all);
    assert_eq!(
        first.next_before,
        Some(2),
        "a full page with rows left reports a resume cursor"
    );
    assert_eq!(
        brief(first),
        vec![(3, 2_000, "a.three".into()), (2, 1_000, "a.two".into())]
    );

    let rest = read.list_audit(Some(2), 10, &all);
    assert_eq!(rest.next_before, None, "the scan reached the oldest entry");
    assert_eq!(
        brief(rest),
        vec![(1, 1_000, "a.one".into())],
        "the cursor page excludes the cursor itself"
    );

    let exact = read.list_audit(None, 3, &all);
    assert_eq!(exact.entries.len(), 3);
    assert_eq!(
        exact.next_before, None,
        "a page ending exactly at the oldest entry has no cursor"
    );

    assert!(read.list_audit(Some(1), 10, &all).entries.is_empty());
}

#[test]
fn audit_listing_filters_by_actor_and_action_prefix() {
    let store = open_test_store();

    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(false);
    for (actor, action) in [
        ("root", "job.enqueue"),
        ("o", "session.login"),
        ("root", "job.delete"),
    ] {
        apply_audit_append(&store, &mut tx, &mut cycle, &audit_record(actor, action), 0)
            .expect("applies");
    }
    tx.commit().expect("commit");

    let read = audit_read_handle(&store);
    let seqs = |page: AuditPage| page.entries.into_iter().map(|e| e.seq).collect::<Vec<_>>();

    let by_actor = AuditFilter {
        actor: Some("root".into()),
        ..Default::default()
    };
    assert_eq!(seqs(read.list_audit(None, 10, &by_actor)), vec![3, 1]);

    let by_prefix = AuditFilter {
        action_prefix: Some("session.".into()),
        ..Default::default()
    };
    assert_eq!(seqs(read.list_audit(None, 10, &by_prefix)), vec![2]);

    let both = AuditFilter {
        actor: Some("o".into()),
        action_prefix: Some("job.".into()),
    };
    let page = read.list_audit(None, 10, &both);
    assert!(page.entries.is_empty());
    assert_eq!(
        page.next_before, None,
        "an exhausted scan reports no cursor even when nothing matched"
    );
}

#[test]
fn audit_listing_bounds_scan_work_under_a_selective_filter() {
    let store = open_test_store();

    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(false);
    for _ in 0..AUDIT_SCAN_CAP + 2 {
        apply_audit_append(
            &store,
            &mut tx,
            &mut cycle,
            &audit_record("root", "job.enqueue"),
            0,
        )
        .expect("applies");
    }
    tx.commit().expect("commit");

    let read = audit_read_handle(&store);
    let nobody = AuditFilter {
        actor: Some("nobody".into()),
        ..Default::default()
    };

    let page = read.list_audit(None, 10, &nobody);
    assert!(page.entries.is_empty());
    assert_eq!(
        page.next_before,
        Some(3),
        "the cap stops the walk with a resume cursor"
    );

    let page = read.list_audit(Some(3), 10, &nobody);
    assert!(page.entries.is_empty());
    assert_eq!(page.next_before, None);
}

#[test]
fn next_deadline_considers_close_tombstones() {
    let mut indexes = Indexes::default();
    assert_eq!(next_deadline(&indexes, 0), None);
    indexes.closing.insert("q".into(), 1234);
    assert_eq!(next_deadline(&indexes, 0), Some(1234));
}

#[test]
fn dedup_window_is_pinned_at_boot() {
    // The pin now lives at propose time: PreparedJob::new resolves the
    // dedup window from the boot registry, not the live (hot-reloaded)
    // one, and apply only ever sees the carried value.
    let store = open_test_store();
    let mut boot_cfg = Config::default();
    boot_cfg.storage.dedup_window_ms = 1;
    let boot = QueueRegistry::from_config(&boot_cfg);
    // An hour-long hot-reloaded window must not stretch the boot window.
    let mut live_cfg = Config::default();
    live_cfg.storage.dedup_window_ms = 3_600_000;
    let live = QueueRegistry::from_config(&live_cfg);

    let mut indexes = Indexes::default();
    let mut tx = store.db.write_tx();
    let mut cycle = Cycle::new(false);
    let req = EnqueueRequest {
        idempotency_key: Some("k".to_string()),
        ..test_req("q")
    };
    let first = apply_enqueue(
        &store,
        &mut indexes,
        &mut tx,
        &mut cycle,
        vec![PreparedJob::new(req.clone(), &live, &boot)],
        now_ms(),
    )
    .expect("enqueue applies")
    .remove(0)
    .expect("the first enqueue is accepted");

    // Outlive the 1ms boot window. The dedup record itself is still there
    // (no sweep has run), so a deadline from the live window would hit.
    std::thread::sleep(std::time::Duration::from_millis(10));
    let second = apply_enqueue(
        &store,
        &mut indexes,
        &mut tx,
        &mut cycle,
        vec![PreparedJob::new(req, &live, &boot)],
        now_ms(),
    )
    .expect("enqueue applies")
    .remove(0)
    .expect("the second enqueue is accepted");

    assert!(
        !second.deduplicated,
        "the boot window, not the live one, bounds the dedup deadline"
    );
    assert_ne!(second.job_id, first.job_id);
}
