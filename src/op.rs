use tonic::Status;
use uuid::Uuid;

use crate::pb::sepp::storage::v1 as proto;
use crate::pb::sepp::v1::{EnqueueRequest, ExtendRequest, NackRequest};
use crate::storage::PeekState;

// A mutating committer operation: the future replicated-log entry. Carries no
// channels or handles; response delivery stays in the Command envelope. The
// serialized form is proto/sepp/storage/v1/op.proto, pinned by the golden test
// below.
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Enqueue {
        jobs: Vec<PreparedJob>,
    },
    EnqueueAtomic {
        jobs: Vec<PreparedJob>,
    },
    Reserve {
        queues: Vec<String>,
        lease_ms: u64,
        max_jobs: usize,
    },
    Ack {
        job_id: String,
        attempt: u32,
    },
    Nack {
        req: NackRequest,
    },
    Extend {
        req: ExtendRequest,
    },
    DrainDeadLetters {
        queue: Option<String>,
        max: usize,
    },
    CloseQueue {
        queue: String,
    },
    OpenQueue {
        queue: String,
    },
    RequeueDeadLetters {
        queue: String,
        keys: Vec<Vec<u8>>,
    },
    DeadLetterJobs {
        queue: String,
        state: PeekState,
        keys: Vec<Vec<u8>>,
        reason: Option<String>,
    },
    DeleteDeadLetters {
        queue: String,
        keys: Vec<Vec<u8>>,
    },
    PurgeQueueChunk {
        queue: String,
        max: usize,
    },
}

// An EnqueueRequest plus its pre-assigned job ID, generated before the op is
// built so applying it is deterministic.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedJob {
    pub id: String,
    pub req: EnqueueRequest,
}

impl PreparedJob {
    pub fn new(req: EnqueueRequest) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            req,
        }
    }

    fn to_proto(&self) -> proto::PreparedJob {
        proto::PreparedJob {
            request: Some(self.req.clone()),
            job_id: self.id.clone(),
        }
    }

    fn from_proto(msg: proto::PreparedJob) -> Result<Self, Status> {
        Ok(Self {
            id: msg.job_id,
            req: msg.request.ok_or_else(|| corrupt("job without request"))?,
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
            Op::Enqueue { jobs } => P::Enqueue(proto::EnqueueOp {
                jobs: jobs.iter().map(PreparedJob::to_proto).collect(),
            }),
            Op::EnqueueAtomic { jobs } => P::EnqueueAtomic(proto::EnqueueAtomicOp {
                jobs: jobs.iter().map(PreparedJob::to_proto).collect(),
            }),
            Op::Reserve {
                queues,
                lease_ms,
                max_jobs,
            } => P::Reserve(proto::ReserveOp {
                queues: queues.clone(),
                lease_ms: *lease_ms,
                max_jobs: *max_jobs as u32,
            }),
            Op::Ack { job_id, attempt } => P::Ack(proto::AckOp {
                job_id: job_id.clone(),
                attempt: *attempt,
            }),
            Op::Nack { req } => P::Nack(proto::NackOp {
                request: Some(req.clone()),
            }),
            Op::Extend { req } => P::Extend(proto::ExtendOp {
                request: Some(req.clone()),
            }),
            Op::DrainDeadLetters { queue, max } => P::DrainDeadLetters(proto::DrainDeadLettersOp {
                queue: queue.clone(),
                max: *max as u32,
            }),
            Op::CloseQueue { queue } => P::CloseQueue(proto::CloseQueueOp {
                queue: queue.clone(),
            }),
            Op::OpenQueue { queue } => P::OpenQueue(proto::OpenQueueOp {
                queue: queue.clone(),
            }),
            Op::RequeueDeadLetters { queue, keys } => {
                P::RequeueDeadLetters(proto::RequeueDeadLettersOp {
                    queue: queue.clone(),
                    keys: keys.clone(),
                })
            }
            Op::DeadLetterJobs {
                queue,
                state,
                keys,
                reason,
            } => P::DeadLetterJobs(proto::DeadLetterJobsOp {
                queue: queue.clone(),
                state: state_to_proto(*state) as i32,
                keys: keys.clone(),
                reason: reason.clone(),
            }),
            Op::DeleteDeadLetters { queue, keys } => {
                P::DeleteDeadLetters(proto::DeleteDeadLettersOp {
                    queue: queue.clone(),
                    keys: keys.clone(),
                })
            }
            Op::PurgeQueueChunk { queue, max } => P::PurgeQueueChunk(proto::PurgeQueueChunkOp {
                queue: queue.clone(),
                max: *max as u32,
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
            },
            P::EnqueueAtomic(o) => Op::EnqueueAtomic {
                jobs: jobs(o.jobs)?,
            },
            P::Reserve(o) => Op::Reserve {
                queues: o.queues,
                lease_ms: o.lease_ms,
                max_jobs: o.max_jobs as usize,
            },
            P::Ack(o) => Op::Ack {
                job_id: o.job_id,
                attempt: o.attempt,
            },
            P::Nack(o) => Op::Nack {
                req: o.request.ok_or_else(|| corrupt("nack without request"))?,
            },
            P::Extend(o) => Op::Extend {
                req: o.request.ok_or_else(|| corrupt("extend without request"))?,
            },
            P::DrainDeadLetters(o) => Op::DrainDeadLetters {
                queue: o.queue,
                max: o.max as usize,
            },
            P::CloseQueue(o) => Op::CloseQueue { queue: o.queue },
            P::OpenQueue(o) => Op::OpenQueue { queue: o.queue },
            P::RequeueDeadLetters(o) => Op::RequeueDeadLetters {
                queue: o.queue,
                keys: o.keys,
            },
            P::DeadLetterJobs(o) => Op::DeadLetterJobs {
                queue: o.queue,
                state: state_from_proto(o.state)?,
                keys: o.keys,
                reason: o.reason,
            },
            P::DeleteDeadLetters(o) => Op::DeleteDeadLetters {
                queue: o.queue,
                keys: o.keys,
            },
            P::PurgeQueueChunk(o) => Op::PurgeQueueChunk {
                queue: o.queue,
                max: o.max as usize,
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

    // One op per variant, every field populated, in oneof field order.
    fn sample_ops() -> Vec<Op> {
        let job = PreparedJob {
            id: "01234567-89ab-cdef-0123-456789abcdef".into(),
            req: sample_enqueue_request(),
        };

        vec![
            Op::Enqueue {
                jobs: vec![job.clone()],
            },
            Op::EnqueueAtomic { jobs: vec![job] },
            Op::Reserve {
                queues: vec!["orders".into(), "emails".into()],
                lease_ms: 30_000,
                max_jobs: 5,
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
            },
            Op::Extend {
                req: ExtendRequest {
                    job_id: "job-1".into(),
                    attempt: 2,
                    lease_duration: Some(millis_to_duration(30_000)),
                    worker_id: Some("w-1".into()),
                },
            },
            Op::DrainDeadLetters {
                queue: Some("orders".into()),
                max: 10,
            },
            Op::CloseQueue {
                queue: "orders".into(),
            },
            Op::OpenQueue {
                queue: "orders".into(),
            },
            Op::RequeueDeadLetters {
                queue: "orders".into(),
                keys: vec![b"k1".to_vec(), b"k2".to_vec()],
            },
            Op::DeadLetterJobs {
                queue: "orders".into(),
                state: PeekState::Inflight,
                keys: vec![b"k1".to_vec()],
                reason: Some("manual".into()),
            },
            Op::DeleteDeadLetters {
                queue: "orders".into(),
                keys: vec![b"k1".to_vec()],
            },
            Op::PurgeQueueChunk {
                queue: "orders".into(),
                max: 1000,
            },
        ]
    }

    // The serialized form is the future log entry: a byte change here breaks
    // every recorded op stream. Update the golden bytes only for a deliberate
    // format change, never to make the test pass after a refactor.
    #[test]
    fn golden_op_encoding_is_pinned() {
        const GOLDEN: &[&str] = &[
            "0ac6010ac3010a9a010a066f7264657273120a73656e642d656d61696c1a160a027b7d12106170706c69636174696f6e2f6a736f6e22066964656d2d31280730033a390a3730302d30616637363531393136636434336464383434386562323131633830333139632d623761643662373136393230333333312d3031420e0a06726567696f6e12040a026575420d0a0772657472696573120218024a060880e2cfaa06122430313233343536372d383961622d636465662d303132332d343536373839616263646566",
            "12c6010ac3010a9a010a066f7264657273120a73656e642d656d61696c1a160a027b7d12106170706c69636174696f6e2f6a736f6e22066964656d2d31280730033a390a3730302d30616637363531393136636434336464383434386562323131633830333139632d623761643662373136393230333333312d3031420e0a06726567696f6e12040a026575420d0a0772657472696573120218024a060880e2cfaa06122430313233343536372d383961622d636465662d303132332d343536373839616263646566",
            "1a160a066f72646572730a06656d61696c7310b0ea011805",
            "22090a056a6f622d311002",
            "2a1c0a1a0a056a6f622d3110021a04626f6f6d2204120208052a03772d31",
            "32140a120a056a6f622d3110021a02081e2203772d31",
            "3a0a0a066f7264657273100a",
            "42080a066f7264657273",
            "4a080a066f7264657273",
            "52100a066f726465727312026b3112026b32",
            "5a160a066f726465727310031a026b3122066d616e75616c",
            "620c0a066f726465727312026b31",
            "6a0b0a066f726465727310e807",
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
            op: Some(proto::op::Op::Nack(proto::NackOp { request: None })),
        };
        assert!(Op::from_proto(no_request).is_err());

        let unknown_state = proto::Op {
            op: Some(proto::op::Op::DeadLetterJobs(proto::DeadLetterJobsOp {
                queue: "q".into(),
                state: 99,
                keys: vec![],
                reason: None,
            })),
        };
        assert!(Op::from_proto(unknown_state).is_err());
    }
}
