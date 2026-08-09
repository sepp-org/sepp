use std::collections::BTreeMap;

use openraft::{EntryPayload, LeaderId};
use prost::Message;

use super::*;
use crate::config::QueueConfig;
use crate::keys::{
    APPLY_DIGEST_KEY, DeadLetterKey, LAST_APPLIED_KEY, MEMBERSHIP_KEY, ReadyKey,
    STAMP_HIGH_WATER_KEY, TimerKey,
};
use crate::op::{JobLimits, Op, PreparedJob};
use crate::pb::millis_to_timestamp;
use crate::pb::sepp::storage::v1 as op_proto;
use crate::pb::sepp::v1::{NackRetry, Payload, nack_retry};
use crate::pb::{duration_to_millis, millis_to_duration};
use crate::raft::{Entry, LogId, log_id_to_proto};
use uuid::Uuid;

const QUEUES: &[&str] = &["alpha", "beta", "gamma"];

fn replay_config() -> Config {
    let mut cfg = Config::default();
    cfg.storage.dedup_window_ms = 20_000;
    cfg.queues = vec![
        QueueConfig {
            name: "alpha".into(),
            max_queue_depth: Some(6),
            dedup_window_ms: Some(5_000),
            ..Default::default()
        },
        QueueConfig {
            name: "beta".into(),
            default_max_attempts: Some(2),
            default_priority: Some(3),
            ..Default::default()
        },
    ];
    cfg
}

fn base_params() -> StorageParams {
    StorageParams {
        persist_mode: PersistMode::Buffer,
        sweep_limit: 100,
        dead_letter_retention_ms: 60_000,
        admin_enabled: false,
    }
}

// Every field differs from base_params.
fn skewed_params() -> StorageParams {
    StorageParams {
        persist_mode: PersistMode::SyncAll,
        sweep_limit: 7,
        dead_letter_retention_ms: 0,
        admin_enabled: true,
    }
}

struct Harness {
    store: Store,
    indexes: Indexes,
}

impl Harness {
    fn open(params: StorageParams) -> Self {
        let path = std::env::temp_dir().join(format!("sepp-replay-test-{}", Uuid::new_v4()));
        let db = TxDatabase::builder(path)
            .temporary(true)
            .open()
            .expect("open temporary db");
        let ks = Keyspaces::open(&db).expect("open keyspaces");
        let store = Store::new(db, ks, params, Metrics::new(false));
        let indexes = rebuild_indexes(&store).expect("rebuild");

        Self { store, indexes }
    }

    fn apply(&mut self, op: &Op) -> Result<OpOutcome, Status> {
        let mut tx = ApplyTx::new(self.store.db.write_tx());
        let mut cycle = Cycle::new(false);
        let result = apply_op(
            &self.store,
            &mut self.indexes,
            &mut tx,
            &mut cycle,
            op.clone(),
        );
        if let Err(e) = &result {
            assert_ne!(
                e.code(),
                tonic::Code::Internal,
                "storage failure while applying {op:?}: {e}"
            );
        }
        // A rejected op never touches the transaction, so dropping an
        // undirtied tx mirrors the committer skipping the commit.
        if cycle.dirty {
            tx.commit().expect("commit");
        }
        result
    }

    fn replay(params: StorageParams, ops: &[Op]) -> (Self, Vec<String>) {
        let mut harness = Self::open(params);
        let outcomes = ops
            .iter()
            .map(|op| dbg_outcome(harness.apply(op)))
            .collect();
        (harness, outcomes)
    }
}

// Debug text as the comparison key: deterministic for every payload these
// outcomes carry, and a mismatch names the exact field that diverged.
fn dbg_outcome(outcome: Result<OpOutcome, Status>) -> String {
    format!("{outcome:?}")
}

fn assert_identical_state(a: &Harness, b: &Harness, label: &str) {
    let (a, b) = (logical_contents(&a.store), logical_contents(&b.store));
    for name in a.keys() {
        assert_eq!(
            a[name], b[name],
            "{label}: keyspace {name:?} diverged between stores"
        );
    }
}

fn assert_indexes_match_rebuild(store: &Store, live: &Indexes, label: &str) {
    let fresh = rebuild_indexes(store).expect("rebuild");
    assert_eq!(live.ready.keys, fresh.ready.keys, "{label}: ready keys");
    assert_eq!(
        live.ready.by_queue, fresh.ready.by_queue,
        "{label}: ready by_queue"
    );
    for (name, live, fresh) in [
        ("scheduled", &live.scheduled, &fresh.scheduled),
        ("leases", &live.leases, &fresh.leases),
        ("dedup_timers", &live.dedup_timers, &fresh.dedup_timers),
        ("dead_letter", &live.dead_letter, &fresh.dead_letter),
    ] {
        assert_eq!(live.keys, fresh.keys, "{label}: {name} keys");
        assert_eq!(live.by_queue, fresh.by_queue, "{label}: {name} by_queue");
    }
    assert_eq!(live.closing, fresh.closing, "{label}: closing");
}

// Applies a stream to a source store and to two fresh stores, then asserts
// the determinism contract on all three.
fn assert_stream_replays(source: &Harness, ops: &[Op], outcomes: &[String]) {
    // One replay runs the ops through their serialized form, so what's proven
    // deterministic is the recorded artifact itself, not just the in-memory
    // clones.
    let roundtripped: Vec<Op> = ops
        .iter()
        .map(|op| {
            let bytes = op.to_proto().encode_to_vec();
            let decoded = op_proto::Op::decode(bytes.as_slice()).expect("op decodes");
            Op::from_proto(decoded).expect("op converts")
        })
        .collect();

    let (replay_a, outcomes_a) = Harness::replay(base_params(), ops);
    let (replay_b, outcomes_b) = Harness::replay(base_params(), &roundtripped);

    for (i, (src, rep)) in outcomes.iter().zip(&outcomes_a).enumerate() {
        assert_eq!(
            src, rep,
            "op #{i} ({:?}) produced a different outcome",
            ops[i]
        );
    }
    assert_eq!(outcomes_a, outcomes_b);

    assert_identical_state(source, &replay_a, "source vs replay");
    assert_identical_state(&replay_a, &replay_b, "replay vs replay");
    assert_indexes_match_rebuild(&source.store, &source.indexes, "source");
    assert_indexes_match_rebuild(&replay_a.store, &replay_a.indexes, "replay");
}

fn job(id: &str, req: EnqueueRequest) -> PreparedJob {
    // Propose-time resolution, as the handle would do it, from the proposer's
    // config in replay_config.
    let registry = QueueRegistry::from_config(&replay_config());
    PreparedJob {
        id: id.into(),
        limits: JobLimits::resolve(&req, &registry, &registry),
        req,
    }
}

fn req(queue: &str) -> EnqueueRequest {
    EnqueueRequest {
        queue: queue.into(),
        job_type: "t".into(),
        ..Default::default()
    }
}

fn nack(
    job_id: &str,
    attempt: u32,
    strategy: nack_retry::Strategy,
    dead_letter_enabled: bool,
    now_ms: i64,
) -> Op {
    // Stand-in for construction-time resolution: Default gets a small
    // attempt-derived delay so directive-less nacks exercise the scheduled
    // path too.
    let retry_delay_ms = match &strategy {
        nack_retry::Strategy::Delay(d) => duration_to_millis(d),
        nack_retry::Strategy::Default(_) => u64::from(attempt) * 500,
        nack_retry::Strategy::DeadLetter(_) => 0,
    };
    Op::Nack {
        req: NackRequest {
            job_id: job_id.into(),
            attempt,
            reason: Some("replay".into()),
            retry: Some(NackRetry {
                strategy: Some(strategy),
            }),
            worker_id: None,
        },
        retry_delay_ms,
        dead_letter_enabled,
        now_ms,
    }
}

// A fixed scenario walking every op kind, including the key-addressed admin
// ops the random generator skips. Absolute outcomes are not hand-asserted;
// the contract under test is that three stores agree on all of them.
#[test]
fn recorded_stream_replays_identically() {
    let mut source = Harness::open(base_params());

    let t = 1_000_000;
    let dl_key = |failed_at: i64, queue: &str, job_id: &str| {
        DeadLetterKey {
            failed_at,
            queue,
            job_id: job_id.as_bytes(),
        }
        .encode()
    };
    let ops: Vec<Op> = vec![
        Op::Enqueue {
            jobs: vec![
                job("j01", req("alpha")),
                job(
                    "j02",
                    EnqueueRequest {
                        idempotency_key: Some("k1".into()),
                        ..req("alpha")
                    },
                ),
                job(
                    "j03",
                    EnqueueRequest {
                        scheduled_at: Some(millis_to_timestamp(t + 5_000)),
                        ..req("alpha")
                    },
                ),
                job(
                    "j04",
                    EnqueueRequest {
                        payload: Some(Payload {
                            data: b"payload".to_vec(),
                            encoding: "raw".into(),
                        }),
                        priority: Some(9),
                        ..req("beta")
                    },
                ),
            ],
            now_ms: t,
        },
        // Dedup hit: same key inside the window returns j02's id.
        Op::Enqueue {
            jobs: vec![job(
                "j05",
                EnqueueRequest {
                    idempotency_key: Some("k1".into()),
                    ..req("alpha")
                },
            )],
            now_ms: t + 100,
        },
        Op::EnqueueAtomic {
            jobs: vec![
                job("j06", req("alpha")),
                job(
                    "j07",
                    EnqueueRequest {
                        max_attempts: Some(1),
                        ..req("gamma")
                    },
                ),
            ],
            now_ms: t + 200,
        },
        Op::Reserve {
            queues: vec!["alpha".into(), "beta".into()],
            lease_ms: 10_000,
            max_jobs: 3,
            now_ms: t + 1_000,
        },
        Op::Ack {
            job_id: "j04".into(),
            attempt: 1,
        },
        nack(
            "j01",
            1,
            nack_retry::Strategy::Delay(millis_to_duration(2_000)),
            true,
            t + 2_000,
        ),
        // Stale attempt: rejected, and the rejection must replay too.
        Op::Ack {
            job_id: "j02".into(),
            attempt: 7,
        },
        Op::Extend {
            req: ExtendRequest {
                job_id: "j02".into(),
                attempt: 1,
                lease_duration: Some(millis_to_duration(30_000)),
                worker_id: None,
            },
            lease_ms: 30_000,
            now_ms: t + 3_000,
        },
        // Rejected via max_attempts = 1: lands in dead_letter at t + 4_000.
        Op::Reserve {
            queues: vec!["gamma".into()],
            lease_ms: 10_000,
            max_jobs: 1,
            now_ms: t + 3_500,
        },
        nack(
            "j07",
            1,
            nack_retry::Strategy::DeadLetter(()),
            true,
            t + 4_000,
        ),
        // Promotes j03 and the delayed j01; dedup for "k1" expires later.
        Op::Sweep {
            now_ms: t + 6_000,
            budget: 100,
            retention_cutoff_ms: Some(t - 54_000),
            dead_letter_enabled: true,
        },
        Op::CloseQueue {
            queue: "alpha".into(),
            now_ms: t + 6_500,
            grace_ms: 30_000,
        },
        // Rejected: the queue is closing.
        Op::Enqueue {
            jobs: vec![job("j08", req("alpha"))],
            now_ms: t + 6_600,
        },
        Op::PurgeQueueChunk {
            queue: "alpha".into(),
            max: 100,
        },
        Op::OpenQueue {
            queue: "alpha".into(),
        },
        // Fresh jobs whose ready and scheduled keys are constructible, so the
        // key-addressed admin ops below actually mutate.
        Op::Enqueue {
            jobs: vec![
                job(
                    "j09",
                    EnqueueRequest {
                        priority: Some(4),
                        ..req("beta")
                    },
                ),
                job(
                    "j10",
                    EnqueueRequest {
                        scheduled_at: Some(millis_to_timestamp(t + 30_000)),
                        ..req("beta")
                    },
                ),
            ],
            now_ms: t + 6_800,
        },
        Op::DeadLetterJobs {
            queue: "beta".into(),
            state: PeekState::Ready,
            keys: vec![
                ReadyKey {
                    queue: "beta",
                    priority: 4,
                    enqueued_at: t + 6_800,
                    job_id: "j09",
                }
                .encode(),
            ],
            reason: Some("manual".into()),
            dead_letter_enabled: true,
            now_ms: t + 7_000,
        },
        Op::DeadLetterJobs {
            queue: "beta".into(),
            state: PeekState::Scheduled,
            keys: vec![
                TimerKey {
                    deadline: t + 30_000,
                    job_id: "j10",
                }
                .encode(),
            ],
            reason: None,
            dead_letter_enabled: true,
            now_ms: t + 7_100,
        },
        Op::DeleteDeadLetters {
            queue: "beta".into(),
            keys: vec![dl_key(t + 7_000, "beta", "j09")],
        },
        Op::RequeueDeadLetters {
            queue: "gamma".into(),
            keys: vec![dl_key(t + 4_000, "gamma", "j07")],
            now_ms: t + 8_000,
        },
        Op::DrainDeadLetters {
            queue: None,
            max: 10,
            scan_cap: 100,
        },
        // Already requeued and drained away: the missing branch.
        Op::DeleteDeadLetters {
            queue: "gamma".into(),
            keys: vec![dl_key(t + 4_000, "gamma", "j07")],
        },
        // Wall clock stepping backwards must not break anything.
        Op::Sweep {
            now_ms: t + 5_900,
            budget: 100,
            retention_cutoff_ms: Some(t - 54_100),
            dead_letter_enabled: true,
        },
        // Two in a row so the replayed seq allocation is exercised, not just
        // the first-row default.
        Op::AuditAppend {
            record: op_proto::AuditRecord {
                actor: "root".into(),
                role: "admin".into(),
                action: "queue.open".into(),
                details_json: r#"{"queue":"alpha"}"#.into(),
            },
            now_ms: t + 9_000,
        },
        Op::AuditAppend {
            record: op_proto::AuditRecord {
                actor: "ops-bot".into(),
                role: "operator".into(),
                action: "dead_letter.requeue".into(),
                details_json: r#"{"queue":"gamma","count":1}"#.into(),
            },
            now_ms: t + 9_100,
        },
    ];

    let outcomes: Vec<String> = ops.iter().map(|op| dbg_outcome(source.apply(op))).collect();

    let all = outcomes.join("\n");
    for marker in ["dead_lettered: 1", "deleted: 1", "requeued: 1"] {
        assert!(
            all.contains(marker),
            "scenario no longer exercises {marker:?}: {all}"
        );
    }

    assert_stream_replays(&source, &ops, &outcomes);
}

// xorshift64: deterministic per seed, no dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + self.below((hi - lo) as u64) as i64
    }

    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }

    fn queue(&mut self) -> String {
        QUEUES[self.below(QUEUES.len() as u64) as usize].into()
    }
}

fn random_job(rng: &mut Rng, next_id: &mut u32, now: i64) -> PreparedJob {
    let mut req = req(&rng.queue());
    if rng.chance(25) {
        req.idempotency_key = Some(format!("k{}", rng.below(4)));
    }
    if rng.chance(30) {
        req.scheduled_at = Some(millis_to_timestamp(now + rng.range(1, 3_000)));
    }
    if rng.chance(50) {
        req.priority = Some(rng.below(10) as u32);
    }
    req.max_attempts = Some(1 + rng.below(2) as u32);
    if rng.chance(20) {
        req.payload = Some(Payload {
            data: vec![0xab; 8],
            encoding: "raw".into(),
        });
    }

    *next_id += 1;
    job(&format!("j{next_id:05}"), req)
}

fn random_op(
    rng: &mut Rng,
    next_id: &mut u32,
    leases: &mut Vec<(String, u32)>,
    dead_letter_enabled: bool,
    now: i64,
) -> Op {
    let lease_target = |rng: &mut Rng, leases: &mut Vec<(String, u32)>| {
        if leases.is_empty() || rng.chance(15) {
            return ("j-bogus".to_string(), 1);
        }
        let i = rng.below(leases.len() as u64) as usize;
        if rng.chance(20) {
            let (id, attempt) = leases[i].clone();
            (id, attempt + 1) // stale: must reject identically on replay
        } else {
            leases.swap_remove(i)
        }
    };

    match rng.below(100) {
        0..=29 => Op::Enqueue {
            jobs: (0..1 + rng.below(3))
                .map(|_| random_job(rng, next_id, now))
                .collect(),
            now_ms: now,
        },
        30..=37 => Op::EnqueueAtomic {
            jobs: (0..1 + rng.below(3))
                .map(|_| random_job(rng, next_id, now))
                .collect(),
            now_ms: now,
        },
        38..=57 => Op::Reserve {
            queues: vec![rng.queue(), rng.queue()],
            lease_ms: 1 + rng.below(3_000),
            max_jobs: 1 + rng.below(3) as usize,
            now_ms: now,
        },
        58..=69 => {
            let (job_id, attempt) = lease_target(rng, leases);
            Op::Ack { job_id, attempt }
        }
        70..=79 => {
            let (job_id, attempt) = lease_target(rng, leases);
            let strategy = match rng.below(3) {
                0 => nack_retry::Strategy::Default(()),
                1 => nack_retry::Strategy::Delay(millis_to_duration(1 + rng.below(2_000))),
                _ => nack_retry::Strategy::DeadLetter(()),
            };
            nack(&job_id, attempt, strategy, dead_letter_enabled, now)
        }
        80..=84 => {
            let (job_id, attempt) = lease_target(rng, leases);
            // Stand-in for construction-time resolution, like nack's delay.
            let lease = 1 + rng.below(3_000);
            Op::Extend {
                req: ExtendRequest {
                    job_id,
                    attempt,
                    lease_duration: Some(millis_to_duration(lease)),
                    worker_id: None,
                },
                lease_ms: lease,
                now_ms: now,
            }
        }
        85..=92 => Op::Sweep {
            now_ms: now,
            budget: 1 + rng.below(8) as usize,
            retention_cutoff_ms: Some(now - 60_000),
            dead_letter_enabled,
        },
        93..=94 => Op::CloseQueue {
            queue: rng.queue(),
            now_ms: now,
            grace_ms: 30_000,
        },
        95 => Op::OpenQueue { queue: rng.queue() },
        96..=97 => Op::PurgeQueueChunk {
            queue: rng.queue(),
            max: 1 + rng.below(5) as usize,
        },
        98 => Op::DrainDeadLetters {
            queue: rng.chance(50).then(|| rng.queue()),
            max: 1 + rng.below(3) as usize,
            scan_cap: 100,
        },
        _ => Op::AuditAppend {
            record: op_proto::AuditRecord {
                actor: format!("op-{}", rng.below(3)),
                role: "operator".into(),
                action: "random.action".into(),
                details_json: format!(r#"{{"n":{}}}"#, rng.below(1_000)),
            },
            now_ms: now,
        },
    }
}

// Applies `steps` random ops to a fresh store and returns the store, the op
// stream and the outcomes. `step_now` drives the clock; `dead_letter_enabled`
// is what the stream's proposer would emit from its retention config.
fn generate_stream(
    rng: &mut Rng,
    steps: usize,
    dead_letter_enabled: bool,
    mut step_now: impl FnMut(&mut Rng) -> i64,
) -> (Harness, Vec<Op>, Vec<String>) {
    let mut source = Harness::open(base_params());
    let mut now: i64 = 1_000_000;
    let mut next_id = 0u32;
    let mut leases: Vec<(String, u32)> = Vec::new();
    let mut ops = Vec::new();
    let mut outcomes = Vec::new();

    for _ in 0..steps {
        now += step_now(rng);
        let op = random_op(rng, &mut next_id, &mut leases, dead_letter_enabled, now);
        let outcome = source.apply(&op);
        if let Ok(OpOutcome::Reserve(jobs)) = &outcome {
            leases.extend(jobs.iter().map(|j| (j.id.clone(), j.attempt)));
        }
        outcomes.push(dbg_outcome(outcome));
        ops.push(op);
    }

    (source, ops, outcomes)
}

#[test]
fn random_interleavings_replay_identically() {
    // Coverage markers over all seeds: the interleavings must actually hit
    // the paths the spec calls out, or the whole test is a placebo.
    let markers = [
        "deduplicated: true",
        "QueueFull",
        "QueueClosing",
        "dead_lettered: true",
        "attempt mismatch",
        "job not found",
    ];
    let mut seen = [false; 6];

    for seed in 1..=6u64 {
        let mut rng = Rng(seed);
        // Half the seeds propose with the DLQ disabled so both branches of
        // maybe_store_dead_letter replay through serialization.
        let (source, ops, outcomes) =
            generate_stream(&mut rng, 300, seed % 2 == 1, |rng| match rng.below(100) {
                0..=69 => rng.range(1, 500),
                70..=84 => 0,
                85..=94 => rng.range(500, 5_000),
                _ => -rng.range(1, 100),
            });

        for outcome in &outcomes {
            for (i, marker) in markers.iter().enumerate() {
                seen[i] |= outcome.contains(marker);
            }
        }
        eprintln!("seed {seed}: {} ops applied", ops.len());
        assert_stream_replays(&source, &ops, &outcomes);
    }

    for (i, marker) in markers.iter().enumerate() {
        assert!(seen[i], "no seed exercised the {marker:?} path");
    }
}

// Ops carry every config-derived value their apply path needs: queue limits,
// dedup windows, retry delays, lease clamps and the dead-letter flag all
// resolve at propose time. The registry config can no longer leak into apply
// at all — Store holds none — so the one node-local knob left is the params
// struct: a store where every params field differs (most interestingly the
// DLQ disabled) must still apply the stream identically, because whether a
// dead-lettered job is stored rides in each op.
#[test]
fn replay_is_blind_to_node_params() {
    let mut rng = Rng(42);
    let (source, ops, outcomes) = generate_stream(&mut rng, 300, true, |rng| rng.range(1, 500));

    assert!(
        outcomes.iter().any(|o| o.contains("dead_lettered: true")),
        "stream never stored a dead letter; change the seed"
    );

    let (skewed_replay, skewed_outcomes) = Harness::replay(skewed_params(), &ops);
    assert_eq!(
        outcomes, skewed_outcomes,
        "an outcome depended on node-local params"
    );
    assert_identical_state(&source, &skewed_replay, "base vs skewed params");
}

// The drop branch of the flag: a retention-disabled proposer emits
// dead_letter_enabled: false, so dead-lettered jobs are deleted outright —
// outcomes still report them and the stream must replay identically.
#[test]
fn dead_letter_disabled_stream_drops_jobs_and_replays() {
    let mut source = Harness::open(base_params());
    let t = 1_000_000;
    let ops: Vec<Op> = vec![
        Op::Enqueue {
            jobs: vec![
                job("j1", req("gamma")),
                job(
                    "j2",
                    EnqueueRequest {
                        priority: Some(4),
                        ..req("alpha")
                    },
                ),
            ],
            now_ms: t,
        },
        Op::Reserve {
            queues: vec!["gamma".into()],
            lease_ms: 10_000,
            max_jobs: 1,
            now_ms: t + 100,
        },
        nack(
            "j1",
            1,
            nack_retry::Strategy::DeadLetter(()),
            false,
            t + 200,
        ),
        Op::DeadLetterJobs {
            queue: "alpha".into(),
            state: PeekState::Ready,
            keys: vec![
                ReadyKey {
                    queue: "alpha",
                    priority: 4,
                    enqueued_at: t,
                    job_id: "j2",
                }
                .encode(),
            ],
            reason: None,
            dead_letter_enabled: false,
            now_ms: t + 300,
        },
    ];

    let outcomes: Vec<String> = ops.iter().map(|op| dbg_outcome(source.apply(op))).collect();
    assert!(
        outcomes[2].contains("dead_lettered: true"),
        "the nack still reports the dead-letter: {}",
        outcomes[2]
    );
    assert!(
        outcomes[3].contains("dead_lettered: 1"),
        "the admin op still reports the dead-letter: {}",
        outcomes[3]
    );

    let contents = logical_contents(&source.store);
    assert!(contents["dead_letter"].is_empty(), "no DLQ rows are stored");
    assert!(
        contents["jobs"].is_empty() && contents["payloads"].is_empty(),
        "dropped jobs are still deleted"
    );

    assert_stream_replays(&source, &ops, &outcomes);
}

// --- Raft-path equivalence -------------------------------------------------

fn log_id(index: u64) -> LogId {
    LogId {
        leader_id: LeaderId::new(1, 1),
        index,
    }
}

fn op_entries(ops: &[Op]) -> Vec<Entry> {
    ops.iter()
        .enumerate()
        .map(|(i, op)| Entry {
            log_id: log_id(i as u64 + 1),
            payload: EntryPayload::Normal(op.clone()),
        })
        .collect()
}

// Applies the stream through the real raft apply path in `batch`-sized
// batches and returns the core plus direct-path-comparable outcome strings.
fn raft_replay(params: StorageParams, ops: &[Op], batch: usize) -> (ApplyCore, Vec<String>) {
    let harness = Harness::open(params);
    let mut core = ApplyCore::new(
        harness.store,
        harness.indexes,
        QueueNotifiers::default(),
        StampClamp::new(0),
    )
    .expect("apply core");

    let mut outcomes = Vec::new();
    for chunk in op_entries(ops).chunks(batch.max(1)) {
        let (chunk_outcomes, _) = core.apply_entries(chunk.to_vec());
        outcomes.extend(chunk_outcomes.into_iter().map(|outcome| match outcome {
            OpOutcome::Rejected(status) => dbg_outcome(Err(status)),
            other => dbg_outcome(Ok(other)),
        }));
    }

    (core, outcomes)
}

// The adapter's bookkeeping rows are raft-path-only; everything else must
// match the direct path byte for byte.
fn strip_adapter_rows(
    mut all: BTreeMap<&'static str, BTreeMap<Vec<u8>, Vec<u8>>>,
) -> BTreeMap<&'static str, BTreeMap<Vec<u8>, Vec<u8>>> {
    let meta = all.get_mut("meta").expect("meta keyspace");
    for key in [
        LAST_APPLIED_KEY,
        MEMBERSHIP_KEY,
        STAMP_HIGH_WATER_KEY,
        APPLY_DIGEST_KEY,
    ] {
        meta.remove(key);
    }
    all
}

// PR 6's replay gate: the raft apply path is the same state machine. The
// same stream through ApplyCore::apply_entries yields identical outcomes
// and identical keyspace bytes however entries are batched, and the digest
// is a function of the entry sequence, never of batch boundaries.
#[test]
fn raft_apply_matches_direct_apply() {
    let mut rng = Rng(7);
    let (direct, ops, direct_outcomes) =
        generate_stream(&mut rng, 300, true, |rng| rng.range(1, 500));
    assert!(
        direct_outcomes.iter().any(|o| o.starts_with("Err")),
        "stream never exercised a rejection; change the seed"
    );

    let (raft_single, single_outcomes) = raft_replay(base_params(), &ops, 1);

    // The batched replay applies ops that took a wire round-trip, so the
    // digest equality below also proves a follower's re-encoded entry bytes
    // hash identically to the leader's originals.
    let roundtripped: Vec<Op> = ops
        .iter()
        .map(|op| {
            let bytes = op.to_proto().encode_to_vec();
            Op::from_proto(op_proto::Op::decode(bytes.as_slice()).expect("op decodes"))
                .expect("op converts")
        })
        .collect();
    let (raft_batched, batched_outcomes) = raft_replay(base_params(), &roundtripped, 64);

    assert_eq!(
        direct_outcomes, single_outcomes,
        "raft outcomes diverge from direct apply"
    );
    assert_eq!(single_outcomes, batched_outcomes);

    assert_eq!(
        strip_adapter_rows(logical_contents(&direct.store)),
        strip_adapter_rows(logical_contents(raft_single.store())),
        "raft-applied state diverges from direct apply"
    );
    // Between the raft replays nothing is stripped: last_applied,
    // membership, stamp high-water and digest must all be
    // batch-split-invariant.
    assert_eq!(
        logical_contents(raft_single.store()),
        logical_contents(raft_batched.store())
    );
    assert_eq!(raft_single.digest(), raft_batched.digest());
    assert_ne!(raft_single.digest(), [0u8; 32]);

    assert_indexes_match_rebuild(raft_single.store(), raft_single.indexes(), "raft");

    let meta = &logical_contents(raft_single.store())["meta"];
    assert_eq!(
        meta.get(LAST_APPLIED_KEY),
        Some(&log_id_to_proto(&log_id(ops.len() as u64)).encode_to_vec()),
        "last_applied must name the final entry"
    );
    let max_stamp = ops
        .iter()
        .filter_map(|op| op.stamp_ms())
        .max()
        .expect("the stream carries stamped ops");
    assert_eq!(
        meta.get(STAMP_HIGH_WATER_KEY),
        Some(&max_stamp.to_be_bytes().to_vec()),
        "the high-water mark is the max stamp applied"
    );
}

#[test]
fn raft_apply_answers_blank_and_membership_entries() {
    let harness = Harness::open(base_params());
    let mut core = ApplyCore::new(
        harness.store,
        harness.indexes,
        QueueNotifiers::default(),
        StampClamp::new(0),
    )
    .expect("apply core");

    let node = |n: u64| crate::raft::ClusterNode {
        peer_addr: format!("sepp-{n}.internal:50052"),
        client_addr: format!("sepp-{n}.example.com:50051"),
    };
    let membership = crate::raft::Membership::new(
        vec![std::collections::BTreeSet::from([1u64, 2])],
        std::collections::BTreeMap::from([(1u64, node(1)), (2, node(2))]),
    );

    let entries = vec![
        Entry {
            log_id: log_id(1),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: log_id(2),
            payload: EntryPayload::Membership(membership.clone()),
        },
        Entry {
            log_id: log_id(3),
            payload: EntryPayload::Normal(Op::Enqueue {
                jobs: vec![job("j1", req("alpha"))],
                now_ms: 1_000_000,
            }),
        },
    ];
    let (outcomes, _) = core.apply_entries(entries);

    // One response per entry, blank and membership included: openraft zips
    // responses with entries by index.
    assert_eq!(outcomes.len(), 3);
    assert!(matches!(outcomes[0], OpOutcome::NonOp));
    assert!(matches!(outcomes[1], OpOutcome::NonOp));
    assert!(matches!(&outcomes[2], OpOutcome::Enqueue(results) if results[0].is_ok()));

    let meta = &logical_contents(core.store())["meta"];
    assert_eq!(
        meta.get(LAST_APPLIED_KEY),
        Some(&log_id_to_proto(&log_id(3)).encode_to_vec()),
        "blank and membership entries advance last_applied too"
    );
    let stored = crate::raft::StoredMembership::new(Some(log_id(2)), membership);
    assert_eq!(
        meta.get(MEMBERSHIP_KEY),
        Some(&crate::raft::stored_membership_to_proto(&stored).encode_to_vec()),
        "the applied membership rides in meta"
    );
}

// Re-apply is not idempotent, so an already-applied index must fail-stop
// (the committer's abort wrapper turns this panic into a process abort).
#[test]
#[should_panic(expected = "re-apply is not idempotent")]
fn raft_apply_panics_on_a_reapplied_index() {
    let harness = Harness::open(base_params());
    let mut core = ApplyCore::new(
        harness.store,
        harness.indexes,
        QueueNotifiers::default(),
        StampClamp::new(0),
    )
    .expect("apply core");

    let blank = |index| Entry {
        log_id: log_id(index),
        payload: EntryPayload::Blank,
    };
    core.apply_entries(vec![blank(1), blank(2)]);
    core.apply_entries(vec![blank(2)]);
}
