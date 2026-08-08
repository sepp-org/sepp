use tonic::Status;
use uuid::Uuid;

use crate::pb::sepp::storage::v1::{self as proto, AuditRecord};
use crate::pb::sepp::v1::{EnqueueRequest, ExtendRequest, NackRequest};
use crate::queues::QueueRegistry;
use crate::storage::PeekState;

// A mutating committer operation.
// The serialized form is proto/sepp/storage/v1/op.proto, pinned by the golden test below.
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Enqueue {
        jobs: Vec<PreparedJob>,
        now_ms: i64,
    },
    EnqueueAtomic {
        jobs: Vec<PreparedJob>,
        now_ms: i64,
    },
    Reserve {
        queues: Vec<String>,
        lease_ms: u64,
        max_jobs: usize,
        now_ms: i64,
    },
    Ack {
        job_id: String,
        attempt: u32,
    },
    Nack {
        req: NackRequest,
        retry_delay_ms: u64,
        dead_letter_enabled: bool,
        now_ms: i64,
    },
    Extend {
        req: ExtendRequest,
        lease_ms: u64,
        now_ms: i64,
    },
    DrainDeadLetters {
        queue: Option<String>,
        max: usize,
        scan_cap: usize,
    },
    CloseQueue {
        queue: String,
        now_ms: i64,
        grace_ms: i64,
    },
    OpenQueue {
        queue: String,
    },
    RequeueDeadLetters {
        queue: String,
        keys: Vec<Vec<u8>>,
        now_ms: i64,
    },
    DeadLetterJobs {
        queue: String,
        state: PeekState,
        keys: Vec<Vec<u8>>,
        reason: Option<String>,
        dead_letter_enabled: bool,
        now_ms: i64,
    },
    DeleteDeadLetters {
        queue: String,
        keys: Vec<Vec<u8>>,
    },
    PurgeQueueChunk {
        queue: String,
        max: usize,
    },
    Sweep {
        now_ms: i64,
        budget: usize,
        retention_cutoff_ms: Option<i64>,
        dead_letter_enabled: bool,
    },
    AuditAppend {
        record: AuditRecord,
        now_ms: i64,
    },
}

impl Op {
    pub fn stamp(&mut self, now: i64) {
        match self {
            Op::Enqueue { now_ms, .. }
            | Op::EnqueueAtomic { now_ms, .. }
            | Op::Reserve { now_ms, .. }
            | Op::Nack { now_ms, .. }
            | Op::Extend { now_ms, .. }
            | Op::CloseQueue { now_ms, .. }
            | Op::RequeueDeadLetters { now_ms, .. }
            | Op::DeadLetterJobs { now_ms, .. }
            | Op::Sweep { now_ms, .. }
            | Op::AuditAppend { now_ms, .. } => *now_ms = now,
            Op::Ack { .. }
            | Op::DrainDeadLetters { .. }
            | Op::OpenQueue { .. }
            | Op::DeleteDeadLetters { .. }
            | Op::PurgeQueueChunk { .. } => {}
        }
    }
}

// An EnqueueRequest plus its pre-assigned job ID and resolved limits,
// generated before the op is built so applying it is deterministic.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedJob {
    pub id: String,
    pub req: EnqueueRequest,
    pub limits: JobLimits,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobLimits {
    pub priority: u32,
    pub max_attempts: u32,
    pub dedup_window_ms: i64,
    pub max_queue_depth: Option<u64>,
}

impl JobLimits {
    pub fn resolve(req: &EnqueueRequest, live: &QueueRegistry, boot: &QueueRegistry) -> Self {
        let eff = live.effective(&req.queue);
        Self {
            priority: req.priority.unwrap_or(eff.default_priority),
            max_attempts: req
                .max_attempts
                .unwrap_or(eff.default_max_attempts)
                .min(eff.max_attempts_ceiling),
            dedup_window_ms: boot.dedup_window_ms(&req.queue),
            max_queue_depth: eff.max_queue_depth,
        }
    }

    fn to_proto(&self) -> proto::JobLimits {
        proto::JobLimits {
            priority: self.priority,
            max_attempts: self.max_attempts,
            dedup_window_ms: self.dedup_window_ms,
            max_queue_depth: self.max_queue_depth,
        }
    }

    fn from_proto(msg: proto::JobLimits) -> Self {
        Self {
            priority: msg.priority,
            max_attempts: msg.max_attempts,
            dedup_window_ms: msg.dedup_window_ms,
            max_queue_depth: msg.max_queue_depth,
        }
    }
}

impl PreparedJob {
    pub fn new(req: EnqueueRequest, live: &QueueRegistry, boot: &QueueRegistry) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            limits: JobLimits::resolve(&req, live, boot),
            req,
        }
    }

    fn to_proto(&self) -> proto::PreparedJob {
        proto::PreparedJob {
            request: Some(self.req.clone()),
            job_id: self.id.clone(),
            limits: Some(self.limits.to_proto()),
        }
    }

    fn from_proto(msg: proto::PreparedJob) -> Result<Self, Status> {
        Ok(Self {
            id: msg.job_id,
            req: msg.request.ok_or_else(|| corrupt("job without request"))?,
            limits: JobLimits::from_proto(msg.limits.ok_or_else(|| corrupt("job without limits"))?),
        })
    }
}

fn corrupt(what: &str) -> Status {
    Status::internal(format!("corrupt op record: {what}"))
}

fn state_to_proto(state: PeekState) -> proto::JobState {
    match state {
        PeekState::Ready => proto::JobState::Ready,
        PeekState::Scheduled => proto::JobState::Scheduled,
        PeekState::Inflight => proto::JobState::Inflight,
        PeekState::DeadLetter => proto::JobState::DeadLetter,
    }
}

fn state_from_proto(value: i32) -> Result<PeekState, Status> {
    match proto::JobState::try_from(value) {
        Ok(proto::JobState::Ready) => Ok(PeekState::Ready),
        Ok(proto::JobState::Scheduled) => Ok(PeekState::Scheduled),
        Ok(proto::JobState::Inflight) => Ok(PeekState::Inflight),
        Ok(proto::JobState::DeadLetter) => Ok(PeekState::DeadLetter),
        Ok(proto::JobState::Unspecified) | Err(_) => Err(corrupt("unknown job state")),
    }
}

impl Op {
    pub fn to_proto(&self) -> proto::Op {
        use proto::op::Op as P;
        let op = match self {
            Op::Enqueue { jobs, now_ms } => P::Enqueue(proto::EnqueueOp {
                jobs: jobs.iter().map(PreparedJob::to_proto).collect(),
                now_ms: *now_ms,
            }),
            Op::EnqueueAtomic { jobs, now_ms } => P::EnqueueAtomic(proto::EnqueueAtomicOp {
                jobs: jobs.iter().map(PreparedJob::to_proto).collect(),
                now_ms: *now_ms,
            }),
            Op::Reserve {
                queues,
                lease_ms,
                max_jobs,
                now_ms,
            } => P::Reserve(proto::ReserveOp {
                queues: queues.clone(),
                lease_ms: *lease_ms,
                max_jobs: *max_jobs as u64,
                now_ms: *now_ms,
            }),
            Op::Ack { job_id, attempt } => P::Ack(proto::AckOp {
                job_id: job_id.clone(),
                attempt: *attempt,
            }),
            Op::Nack {
                req,
                retry_delay_ms,
                dead_letter_enabled,
                now_ms,
            } => P::Nack(proto::NackOp {
                request: Some(req.clone()),
                now_ms: *now_ms,
                retry_delay_ms: *retry_delay_ms,
                dead_letter_enabled: *dead_letter_enabled,
            }),
            Op::Extend {
                req,
                lease_ms,
                now_ms,
            } => P::Extend(proto::ExtendOp {
                request: Some(req.clone()),
                now_ms: *now_ms,
                lease_ms: *lease_ms,
            }),
            Op::DrainDeadLetters {
                queue,
                max,
                scan_cap,
            } => P::DrainDeadLetters(proto::DrainDeadLettersOp {
                queue: queue.clone(),
                max: *max as u64,
                scan_cap: *scan_cap as u64,
            }),
            Op::CloseQueue {
                queue,
                now_ms,
                grace_ms,
            } => P::CloseQueue(proto::CloseQueueOp {
                queue: queue.clone(),
                now_ms: *now_ms,
                grace_ms: *grace_ms,
            }),
            Op::OpenQueue { queue } => P::OpenQueue(proto::OpenQueueOp {
                queue: queue.clone(),
            }),
            Op::RequeueDeadLetters {
                queue,
                keys,
                now_ms,
            } => P::RequeueDeadLetters(proto::RequeueDeadLettersOp {
                queue: queue.clone(),
                keys: keys.clone(),
                now_ms: *now_ms,
            }),
            Op::DeadLetterJobs {
                queue,
                state,
                keys,
                reason,
                dead_letter_enabled,
                now_ms,
            } => P::DeadLetterJobs(proto::DeadLetterJobsOp {
                queue: queue.clone(),
                state: state_to_proto(*state) as i32,
                keys: keys.clone(),
                reason: reason.clone(),
                now_ms: *now_ms,
                dead_letter_enabled: *dead_letter_enabled,
            }),
            Op::DeleteDeadLetters { queue, keys } => {
                P::DeleteDeadLetters(proto::DeleteDeadLettersOp {
                    queue: queue.clone(),
                    keys: keys.clone(),
                })
            }
            Op::PurgeQueueChunk { queue, max } => P::PurgeQueueChunk(proto::PurgeQueueChunkOp {
                queue: queue.clone(),
                max: *max as u64,
            }),
            Op::Sweep {
                now_ms,
                budget,
                retention_cutoff_ms,
                dead_letter_enabled,
            } => P::Sweep(proto::SweepOp {
                now_ms: *now_ms,
                budget: *budget as u64,
                retention_cutoff_ms: *retention_cutoff_ms,
                dead_letter_enabled: *dead_letter_enabled,
            }),
            Op::AuditAppend { record, now_ms } => P::AuditAppend(proto::AuditAppendOp {
                record: Some(record.clone()),
                now_ms: *now_ms,
            }),
        };

        proto::Op { op: Some(op) }
    }

    pub fn from_proto(msg: proto::Op) -> Result<Op, Status> {
        use proto::op::Op as P;
        let jobs = |jobs: Vec<proto::PreparedJob>| {
            jobs.into_iter()
                .map(PreparedJob::from_proto)
                .collect::<Result<Vec<_>, _>>()
        };

        Ok(match msg.op.ok_or_else(|| corrupt("empty oneof"))? {
            P::Enqueue(o) => Op::Enqueue {
                jobs: jobs(o.jobs)?,
                now_ms: o.now_ms,
            },
            P::EnqueueAtomic(o) => Op::EnqueueAtomic {
                jobs: jobs(o.jobs)?,
                now_ms: o.now_ms,
            },
            P::Reserve(o) => Op::Reserve {
                queues: o.queues,
                lease_ms: o.lease_ms,
                max_jobs: o.max_jobs as usize,
                now_ms: o.now_ms,
            },
            P::Ack(o) => Op::Ack {
                job_id: o.job_id,
                attempt: o.attempt,
            },
            P::Nack(o) => Op::Nack {
                req: o.request.ok_or_else(|| corrupt("nack without request"))?,
                retry_delay_ms: o.retry_delay_ms,
                dead_letter_enabled: o.dead_letter_enabled,
                now_ms: o.now_ms,
            },
            P::Extend(o) => Op::Extend {
                req: o.request.ok_or_else(|| corrupt("extend without request"))?,
                lease_ms: o.lease_ms,
                now_ms: o.now_ms,
            },
            P::DrainDeadLetters(o) => Op::DrainDeadLetters {
                queue: o.queue,
                max: o.max as usize,
                scan_cap: o.scan_cap as usize,
            },
            P::CloseQueue(o) => Op::CloseQueue {
                queue: o.queue,
                now_ms: o.now_ms,
                grace_ms: o.grace_ms,
            },
            P::OpenQueue(o) => Op::OpenQueue { queue: o.queue },
            P::RequeueDeadLetters(o) => Op::RequeueDeadLetters {
                queue: o.queue,
                keys: o.keys,
                now_ms: o.now_ms,
            },
            P::DeadLetterJobs(o) => Op::DeadLetterJobs {
                queue: o.queue,
                state: state_from_proto(o.state)?,
                keys: o.keys,
                reason: o.reason,
                dead_letter_enabled: o.dead_letter_enabled,
                now_ms: o.now_ms,
            },
            P::DeleteDeadLetters(o) => Op::DeleteDeadLetters {
                queue: o.queue,
                keys: o.keys,
            },
            P::PurgeQueueChunk(o) => Op::PurgeQueueChunk {
                queue: o.queue,
                max: o.max as usize,
            },
            P::Sweep(o) => Op::Sweep {
                now_ms: o.now_ms,
                budget: o.budget as usize,
                retention_cutoff_ms: o.retention_cutoff_ms,
                dead_letter_enabled: o.dead_letter_enabled,
            },
            P::AuditAppend(o) => Op::AuditAppend {
                record: o
                    .record
                    .ok_or_else(|| corrupt("audit append without record"))?,
                now_ms: o.now_ms,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use prost::Message;

    use super::*;
    use crate::pb::sepp::v1::{
        NackRetry, Payload, PrimitiveValue, TraceContext, nack_retry, primitive_value,
    };
    use crate::pb::{millis_to_duration, millis_to_timestamp};

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn sample_enqueue_request() -> EnqueueRequest {
        let prim = |v: primitive_value::Value| PrimitiveValue { value: Some(v) };
        let mut custom = BTreeMap::new();
        custom.insert(
            "region".to_string(),
            prim(primitive_value::Value::StringValue("eu".into())),
        );
        custom.insert(
            "retries".to_string(),
            prim(primitive_value::Value::IntValue(2)),
        );

        EnqueueRequest {
            queue: "orders".into(),
            job_type: "send-email".into(),
            payload: Some(Payload {
                data: b"{}".to_vec(),
                encoding: "application/json".into(),
            }),
            idempotency_key: Some("idem-1".into()),
            priority: Some(7),
            max_attempts: Some(3),
            trace_context: Some(TraceContext {
                traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into(),
                tracestate: None,
            }),
            custom,
            scheduled_at: Some(millis_to_timestamp(1_700_000_000_000)),
        }
    }

    const NOW: i64 = 1_700_000_000_000;

    // One op per variant, every field populated, in oneof field order.
    fn sample_ops() -> Vec<Op> {
        let job = PreparedJob {
            id: "01234567-89ab-cdef-0123-456789abcdef".into(),
            req: sample_enqueue_request(),
            limits: JobLimits {
                priority: 7,
                max_attempts: 3,
                dedup_window_ms: 86_400_000,
                max_queue_depth: Some(10_000),
            },
        };

        vec![
            Op::Enqueue {
                jobs: vec![job.clone()],
                now_ms: NOW,
            },
            Op::EnqueueAtomic {
                jobs: vec![job],
                now_ms: NOW,
            },
            Op::Reserve {
                queues: vec!["orders".into(), "emails".into()],
                lease_ms: 30_000,
                max_jobs: 5,
                now_ms: NOW,
            },
            Op::Ack {
                job_id: "job-1".into(),
                attempt: 2,
            },
            Op::Nack {
                req: NackRequest {
                    job_id: "job-1".into(),
                    attempt: 2,
                    reason: Some("boom".into()),
                    retry: Some(NackRetry {
                        strategy: Some(nack_retry::Strategy::Delay(millis_to_duration(5_000))),
                    }),
                    worker_id: Some("w-1".into()),
                },
                retry_delay_ms: 5_000,
                dead_letter_enabled: true,
                now_ms: NOW,
            },
            Op::Extend {
                req: ExtendRequest {
                    job_id: "job-1".into(),
                    attempt: 2,
                    lease_duration: Some(millis_to_duration(30_000)),
                    worker_id: Some("w-1".into()),
                },
                lease_ms: 30_000,
                now_ms: NOW,
            },
            Op::DrainDeadLetters {
                queue: Some("orders".into()),
                max: 10,
                scan_cap: 500,
            },
            Op::CloseQueue {
                queue: "orders".into(),
                now_ms: NOW,
                grace_ms: 30_000,
            },
            Op::OpenQueue {
                queue: "orders".into(),
            },
            Op::RequeueDeadLetters {
                queue: "orders".into(),
                keys: vec![b"k1".to_vec(), b"k2".to_vec()],
                now_ms: NOW,
            },
            Op::DeadLetterJobs {
                queue: "orders".into(),
                state: PeekState::Inflight,
                keys: vec![b"k1".to_vec()],
                reason: Some("manual".into()),
                dead_letter_enabled: true,
                now_ms: NOW,
            },
            Op::DeleteDeadLetters {
                queue: "orders".into(),
                keys: vec![b"k1".to_vec()],
            },
            Op::PurgeQueueChunk {
                queue: "orders".into(),
                max: 1000,
            },
            Op::Sweep {
                now_ms: NOW,
                budget: 1000,
                retention_cutoff_ms: Some(NOW - 86_400_000),
                dead_letter_enabled: true,
            },
            Op::AuditAppend {
                record: AuditRecord {
                    actor: "root".into(),
                    role: "admin".into(),
                    action: "config.edit".into(),
                    details_json: r#"{"path":"limits.default_priority"}"#.into(),
                },
                now_ms: NOW,
            },
        ]
    }

    #[test]
    fn stamp_sets_the_drain_time_on_time_bearing_ops() {
        let mut close = Op::CloseQueue {
            queue: "q".into(),
            now_ms: 0,
            grace_ms: 30_000,
        };
        close.stamp(NOW);
        assert_eq!(
            close,
            Op::CloseQueue {
                queue: "q".into(),
                now_ms: NOW,
                grace_ms: 30_000,
            }
        );

        let mut open = Op::OpenQueue { queue: "q".into() };
        open.stamp(NOW);
        assert_eq!(open, Op::OpenQueue { queue: "q".into() });
    }

    // The serialized form is the future log entry: a byte change here breaks
    // every recorded op stream. Update the golden bytes only for a deliberate
    // format change, never to make the test pass after a refactor.
    #[test]
    fn golden_op_encoding_is_pinned() {
        const GOLDEN: &[&str] = &[
            "0adb010ad1010a9a010a066f7264657273120a73656e642d656d61696c1a160a027b7d12106170706c69636174696f6e2f6a736f6e22066964656d2d31280730033a390a3730302d30616637363531393136636434336464383434386562323131633830333139632d623761643662373136393230333333312d3031420e0a06726567696f6e12040a026575420d0a0772657472696573120218024a060880e2cfaa06122430313233343536372d383961622d636465662d303132332d3435363738396162636465661a0c080710031880b8992920904e1080d095ffbc31",
            "12db010ad1010a9a010a066f7264657273120a73656e642d656d61696c1a160a027b7d12106170706c69636174696f6e2f6a736f6e22066964656d2d31280730033a390a3730302d30616637363531393136636434336464383434386562323131633830333139632d623761643662373136393230333333312d3031420e0a06726567696f6e12040a026575420d0a0772657472696573120218024a060880e2cfaa06122430313233343536372d383961622d636465662d303132332d3435363738396162636465661a0c080710031880b8992920904e1080d095ffbc31",
            "1a1d0a066f72646572730a06656d61696c7310b0ea0118052080d095ffbc31",
            "22090a056a6f622d311002",
            "2a280a1a0a056a6f622d3110021a04626f6f6d2204120208052a03772d311080d095ffbc311888272001",
            "321f0a120a056a6f622d3110021a02081e2203772d311080d095ffbc3118b0ea01",
            "3a0d0a066f7264657273100a18f403",
            "42130a066f72646572731080d095ffbc3118b0ea01",
            "4a080a066f7264657273",
            "52170a066f726465727312026b3112026b321880d095ffbc31",
            "5a1f0a066f726465727310031a026b3122066d616e75616c2880d095ffbc313001",
            "620c0a066f726465727312026b31",
            "6a0b0a066f726465727310e807",
            "72130880d095ffbc3110e807188098fcd5bc312001",
            "7a470a3e0a04726f6f74120561646d696e1a0b636f6e6669672e6564697422227b2270617468223a226c696d6974732e64656661756c745f7072696f72697479227d1080d095ffbc31",
        ];

        let ops = sample_ops();
        assert_eq!(ops.len(), GOLDEN.len(), "one golden entry per op variant");
        for (op, expected) in ops.iter().zip(GOLDEN) {
            let encoded = hex(&op.to_proto().encode_to_vec());
            assert_eq!(&encoded, expected, "encoding changed for {op:?}");
        }
    }

    #[test]
    fn ops_round_trip_through_proto() {
        for op in sample_ops() {
            let bytes = op.to_proto().encode_to_vec();
            let decoded = proto::Op::decode(bytes.as_slice()).expect("decodes");
            assert_eq!(Op::from_proto(decoded).expect("converts"), op);
        }
    }

    #[test]
    fn from_proto_rejects_corrupt_records() {
        assert!(Op::from_proto(proto::Op { op: None }).is_err());

        let no_request = proto::Op {
            op: Some(proto::op::Op::Nack(proto::NackOp {
                request: None,
                now_ms: NOW,
                retry_delay_ms: 0,
                dead_letter_enabled: false,
            })),
        };
        assert!(Op::from_proto(no_request).is_err());

        let no_limits = proto::Op {
            op: Some(proto::op::Op::Enqueue(proto::EnqueueOp {
                jobs: vec![proto::PreparedJob {
                    request: Some(sample_enqueue_request()),
                    job_id: "j1".into(),
                    limits: None,
                }],
                now_ms: NOW,
            })),
        };
        assert!(Op::from_proto(no_limits).is_err());

        let unknown_state = proto::Op {
            op: Some(proto::op::Op::DeadLetterJobs(proto::DeadLetterJobsOp {
                queue: "q".into(),
                state: 99,
                keys: vec![],
                reason: None,
                now_ms: NOW,
                dead_letter_enabled: false,
            })),
        };
        assert!(Op::from_proto(unknown_state).is_err());

        let no_record = proto::Op {
            op: Some(proto::op::Op::AuditAppend(proto::AuditAppendOp {
                record: None,
                now_ms: NOW,
            })),
        };
        assert!(Op::from_proto(no_record).is_err());
    }
}
