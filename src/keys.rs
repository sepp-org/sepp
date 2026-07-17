use prost::Message;
use tonic::Status;

use crate::pb::sepp::v1::{Job, TraceContext};

struct KeyWriter(Vec<u8>);

impl KeyWriter {
    fn with_capacity(n: usize) -> Self {
        KeyWriter(Vec::with_capacity(n))
    }

    fn u8(&mut self, v: u8) -> &mut Self {
        self.0.push(v);
        self
    }

    fn u32(&mut self, v: u32) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }

    fn i64(&mut self, v: i64) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }

    // A length-prefixed (u16) field
    fn prefixed(&mut self, bytes: &[u8]) -> &mut Self {
        self.0
            .extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        self.0.extend_from_slice(bytes);
        self
    }

    fn tail(&mut self, bytes: &[u8]) -> &mut Self {
        self.0.extend_from_slice(bytes);
        self
    }

    fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

struct KeyReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> KeyReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        KeyReader { bytes, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let slice = self.bytes.get(self.pos..self.pos + n)?;
        self.pos += n;
        Some(slice)
    }

    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn i64(&mut self) -> Option<i64> {
        Some(i64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    fn prefixed(&mut self) -> Option<&'a [u8]> {
        let len = u16::from_be_bytes(self.take(2)?.try_into().ok()?) as usize;
        self.take(len)
    }

    fn prefixed_str(&mut self) -> Option<&'a str> {
        std::str::from_utf8(self.prefixed()?).ok()
    }

    fn tail(self) -> &'a [u8] {
        &self.bytes[self.pos..]
    }

    fn tail_str(self) -> Option<&'a str> {
        std::str::from_utf8(self.tail()).ok()
    }
}

pub(crate) fn queue_prefix(queue: &str) -> Vec<u8> {
    let mut w = KeyWriter::with_capacity(2 + queue.len());
    w.prefixed(queue.as_bytes());
    w.into_vec()
}

pub(crate) fn read_queue(bytes: &[u8]) -> Option<&str> {
    KeyReader::new(bytes).prefixed_str()
}

pub(crate) fn deadline_of(key: &[u8]) -> i64 {
    KeyReader::new(key).i64().unwrap_or(0)
}

// Key into the `ready` keyspace: `queue | inverted_priority | enqueued_at | job_id`.
pub(crate) struct ReadyKey<'a> {
    pub queue: &'a str,
    pub priority: u32,
    pub enqueued_at: i64,
    pub job_id: &'a str,
}

impl<'a> ReadyKey<'a> {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = KeyWriter::with_capacity(2 + self.queue.len() + 1 + 8 + self.job_id.len());
        w.prefixed(self.queue.as_bytes())
            .u8(9u8.saturating_sub(self.priority.min(9) as u8))
            .i64(self.enqueued_at)
            .tail(self.job_id.as_bytes());

        w.into_vec()
    }

    pub fn decode(bytes: &'a [u8]) -> Option<Self> {
        let mut r = KeyReader::new(bytes);
        let queue = r.prefixed_str()?;
        let priority = 9u32.saturating_sub(r.u8()? as u32);
        let enqueued_at = r.i64()?;
        let job_id = r.tail_str()?;

        Some(ReadyKey {
            queue,
            priority,
            enqueued_at,
            job_id,
        })
    }
}

// Key into the `scheduled` and `leases` keyspaces: `deadline | job_id`.
pub(crate) struct TimerKey<'a> {
    pub deadline: i64,
    pub job_id: &'a str,
}

impl TimerKey<'_> {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = KeyWriter::with_capacity(8 + self.job_id.len());
        w.i64(self.deadline).tail(self.job_id.as_bytes());

        w.into_vec()
    }

    pub fn job_id(key: &[u8]) -> Option<&[u8]> {
        let mut r = KeyReader::new(key);
        r.i64()?;

        Some(r.tail())
    }
}

// Key into the `dedup` keyspace: `queue | idempotency_key`.
pub(crate) struct DedupKey<'a> {
    pub queue: &'a str,
    pub idempotency_key: &'a str,
}

impl DedupKey<'_> {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = KeyWriter::with_capacity(2 + self.queue.len() + self.idempotency_key.len());
        w.prefixed(self.queue.as_bytes())
            .tail(self.idempotency_key.as_bytes());

        w.into_vec()
    }
}

// Value in the `dedup` keyspace: `enqueued_at | deadline | job_id`.
pub(crate) struct DedupValue<'a> {
    pub enqueued_at: i64,
    pub deadline: i64,
    pub job_id: &'a str,
}

impl<'a> DedupValue<'a> {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = KeyWriter::with_capacity(16 + self.job_id.len());
        w.i64(self.enqueued_at)
            .i64(self.deadline)
            .tail(self.job_id.as_bytes());

        w.into_vec()
    }

    pub fn decode(bytes: &'a [u8]) -> Option<Self> {
        let mut r = KeyReader::new(bytes);
        let enqueued_at = r.i64()?;
        let deadline = r.i64()?;
        let job_id = r.tail_str()?;

        Some(DedupValue {
            enqueued_at,
            deadline,
            job_id,
        })
    }
}

// Key into the `dedup_timers` keyspace: `deadline | dedup_key`.
pub(crate) struct DedupTimerKey<'a> {
    pub deadline: i64,
    pub dedup_key: &'a [u8],
}

impl DedupTimerKey<'_> {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = KeyWriter::with_capacity(8 + self.dedup_key.len());
        w.i64(self.deadline).tail(self.dedup_key);

        w.into_vec()
    }

    pub fn dedup_key(key: &[u8]) -> Option<&[u8]> {
        let mut r = KeyReader::new(key);
        r.i64()?;

        Some(r.tail())
    }

    pub fn queue(key: &[u8]) -> Option<&str> {
        read_queue(Self::dedup_key(key)?)
    }
}

// Key into the `dead_letter` keyspace: `failed_at | queue | job_id`.
pub(crate) struct DeadLetterKey<'a> {
    pub failed_at: i64,
    pub queue: &'a str,
    pub job_id: &'a [u8],
}

impl DeadLetterKey<'_> {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = KeyWriter::with_capacity(8 + 2 + self.queue.len() + self.job_id.len());
        w.i64(self.failed_at)
            .prefixed(self.queue.as_bytes())
            .tail(self.job_id);

        w.into_vec()
    }

    pub fn queue(key: &[u8]) -> Option<&str> {
        let mut r = KeyReader::new(key);
        r.i64()?;

        read_queue(r.tail())
    }
}

// Value in the `jobs` keyspace: `queue | protobuf(job)`.
pub(crate) struct JobValue<'a> {
    pub queue: &'a str,
    pub job: &'a Job,
}

impl JobValue<'_> {
    pub fn encode(&self) -> Vec<u8> {
        let mut v = queue_prefix(self.queue);
        self.job
            .encode(&mut v)
            .expect("Vec buffer never runs out of space");
        v
    }

    pub fn decode(bytes: &[u8]) -> Result<(String, Job), Status> {
        let corrupt = || Status::internal("corrupt job record");
        let mut r = KeyReader::new(bytes);
        let queue = r.prefixed_str().ok_or_else(corrupt)?.to_owned();
        let job = Job::decode(r.tail()).map_err(|_| corrupt())?;

        Ok((queue, job))
    }
}

// Value in the `inflight` keyspace.
pub(crate) struct Inflight {
    pub attempt: u32,
    pub lease_expires_at: i64,
    pub enqueued_at: i64,
    pub priority: u32,
    pub max_attempts: u32,
    pub queue: String,
    pub trace_context: Option<TraceContext>,
}

impl Inflight {
    pub fn encode(&self) -> Vec<u8> {
        let tc_bytes = self
            .trace_context
            .as_ref()
            .map(Message::encode_to_vec)
            .unwrap_or_default();
        let mut w = KeyWriter::with_capacity(30 + self.queue.len() + tc_bytes.len());
        w.u32(self.attempt)
            .i64(self.lease_expires_at)
            .i64(self.enqueued_at)
            .u32(self.priority)
            .u32(self.max_attempts)
            .prefixed(self.queue.as_bytes())
            .tail(&tc_bytes);

        w.into_vec()
    }

    pub fn decode(bytes: &[u8]) -> Result<Inflight, Status> {
        let corrupt = || Status::internal("corrupt inflight record");
        let parse = || -> Option<Inflight> {
            let mut r = KeyReader::new(bytes);
            let attempt = r.u32()?;
            let lease_expires_at = r.i64()?;
            let enqueued_at = r.i64()?;
            let priority = r.u32()?;
            let max_attempts = r.u32()?;
            let queue = r.prefixed_str()?.to_owned();
            let tc_bytes = r.tail();
            let trace_context = if tc_bytes.is_empty() {
                None
            } else {
                Some(TraceContext::decode(tc_bytes).ok()?)
            };

            Some(Inflight {
                attempt,
                lease_expires_at,
                enqueued_at,
                priority,
                max_attempts,
                queue,
                trace_context,
            })
        };

        parse().ok_or_else(corrupt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::sepp::v1::{Job, TraceContext};

    #[test]
    fn queue_prefix_is_length_prefixed() {
        assert_eq!(queue_prefix("ab"), vec![0, 2, b'a', b'b']);
        assert_eq!(queue_prefix(""), vec![0, 0]);
        assert_eq!(read_queue(&queue_prefix("orders")), Some("orders"));
    }

    #[test]
    fn ready_key_round_trips() {
        let key = ReadyKey {
            queue: "orders",
            priority: 3,
            enqueued_at: 55,
            job_id: "the-job-id",
        }
        .encode();

        let decoded = ReadyKey::decode(&key).expect("decodes");
        assert_eq!(decoded.queue, "orders");
        assert_eq!(decoded.priority, 3);
        assert_eq!(decoded.enqueued_at, 55);
        assert_eq!(decoded.job_id, "the-job-id");
    }

    #[test]
    fn ready_key_priority_clamps_above_nine() {
        let mk = |priority| {
            ReadyKey {
                queue: "q",
                priority,
                enqueued_at: 0,
                job_id: "j",
            }
            .encode()
        };
        // priority 9 and 1000 both clamp to the same inverted byte, so the
        // whole key is identical.
        assert_eq!(mk(9), mk(1000));
        assert_eq!(ReadyKey::decode(&mk(1000)).unwrap().priority, 9);
    }

    #[test]
    fn ready_key_orders_high_priority_then_fifo() {
        let mk = |priority, enqueued_at, job_id| {
            ReadyKey {
                queue: "q",
                priority,
                enqueued_at,
                job_id,
            }
            .encode()
        };

        // Higher priority sorts before lower priority.
        assert!(mk(9, 100, "high") < mk(0, 100, "low"));
        // Within a priority, older enqueued_at sorts first (FIFO).
        assert!(mk(5, 100, "first") < mk(5, 200, "second"));
    }

    #[test]
    fn ready_key_decode_rejects_truncated_input() {
        assert!(ReadyKey::decode(&[]).is_none());
        assert!(ReadyKey::decode(&[0, 2, b'a']).is_none()); // claims 2-byte queue, has 1
    }

    #[test]
    fn timer_key_carries_deadline_and_job_id() {
        let k = TimerKey {
            deadline: 12345,
            job_id: "x",
        }
        .encode();
        assert_eq!(deadline_of(&k), 12345);
        assert_eq!(TimerKey::job_id(&k), Some(b"x".as_slice()));

        let zero = TimerKey {
            deadline: 0,
            job_id: "x",
        }
        .encode();
        assert_eq!(deadline_of(&zero), 0);
        // deadline leads, so ascending key order is earliest-first.
        assert!(zero < k);
    }

    #[test]
    fn dedup_timer_key_carries_deadline_and_queue() {
        let dkey = DedupKey {
            queue: "orders",
            idempotency_key: "abc",
        }
        .encode();
        let k = DedupTimerKey {
            deadline: 777,
            dedup_key: &dkey,
        }
        .encode();
        assert_eq!(deadline_of(&k), 777);
        assert_eq!(DedupTimerKey::queue(&k), Some("orders"));
    }

    #[test]
    fn dead_letter_key_embeds_failed_at_and_queue() {
        let k = DeadLetterKey {
            failed_at: 777,
            queue: "orders",
            job_id: b"job-9",
        }
        .encode();
        assert_eq!(deadline_of(&k), 777);
        assert_eq!(DeadLetterKey::queue(&k), Some("orders"));

        let mk = |failed_at, queue, job_id| {
            DeadLetterKey {
                failed_at,
                queue,
                job_id,
            }
            .encode()
        };
        // failed_at leads, so ascending key order is oldest-first regardless of
        // queue or job id.
        assert!(mk(100, "zzz", b"zzz") < mk(200, "aaa", b"aaa"));
    }

    #[test]
    fn dedup_value_round_trips() {
        let bytes = DedupValue {
            enqueued_at: 42,
            deadline: 99,
            job_id: "job-7",
        }
        .encode();
        let decoded = DedupValue::decode(&bytes).expect("decodes");
        assert_eq!(decoded.enqueued_at, 42);
        assert_eq!(decoded.deadline, 99);
        assert_eq!(decoded.job_id, "job-7");
    }

    #[test]
    fn dedup_value_decode_rejects_short_and_invalid_input() {
        assert!(DedupValue::decode(&[]).is_none());
        assert!(DedupValue::decode(&[0, 0, 0, 1]).is_none());
        let mut bad = 1i64.to_be_bytes().to_vec();
        bad.extend_from_slice(&[0xff, 0xff]);
        assert!(DedupValue::decode(&bad).is_none());
    }

    fn sample_job(id: &str) -> Job {
        Job {
            id: id.to_string(),
            job_type: "unit-test".to_string(),
            enqueued_at: Some(crate::pb::millis_to_timestamp(1_700_000_000_000)),
            priority: 5,
            attempt: 1,
            max_attempts: 3,
            ..Default::default()
        }
    }

    #[test]
    fn job_value_round_trips_with_queue() {
        let job = sample_job("job-42");
        let bytes = JobValue {
            queue: "orders",
            job: &job,
        }
        .encode();
        let (queue, decoded) = JobValue::decode(&bytes).expect("decodes");
        assert_eq!(queue, "orders");
        assert_eq!(decoded, job);
    }

    #[test]
    fn job_value_custom_map_encodes_canonically() {
        // Guards the btree_map setting in build.rs: the same logical job must
        // persist as the same bytes regardless of custom-map insertion order.
        use crate::pb::sepp::v1::{PrimitiveValue, primitive_value::Value};

        let entries: Vec<_> = (b'a'..=b'h')
            .map(|k| {
                let value = Some(Value::IntValue(k as i64));
                ((k as char).to_string(), PrimitiveValue { value })
            })
            .collect();

        let mut forward = sample_job("job-42");
        forward.custom.extend(entries.iter().cloned());
        let mut reverse = sample_job("job-42");
        reverse.custom.extend(entries.iter().rev().cloned());

        let encode = |job: &Job| {
            JobValue {
                queue: "orders",
                job,
            }
            .encode()
        };
        assert_eq!(encode(&forward), encode(&reverse));
    }

    #[test]
    fn job_value_decode_rejects_corrupt_input() {
        assert!(JobValue::decode(&[]).is_err());
        assert!(JobValue::decode(&[0]).is_err());
        assert!(JobValue::decode(&[0, 5, 1, 2]).is_err());
    }

    fn sample_inflight(queue: &str, trace_context: Option<TraceContext>) -> Inflight {
        Inflight {
            attempt: 4,
            lease_expires_at: 1_700_000_999_000,
            enqueued_at: 1_700_000_000_000,
            priority: 7,
            max_attempts: 10,
            queue: queue.to_string(),
            trace_context,
        }
    }

    #[test]
    fn inflight_round_trips() {
        let s = sample_inflight("my-queue", None);
        let d = Inflight::decode(&s.encode()).expect("decodes");
        assert_eq!(d.attempt, s.attempt);
        assert_eq!(d.lease_expires_at, s.lease_expires_at);
        assert_eq!(d.enqueued_at, s.enqueued_at);
        assert_eq!(d.priority, s.priority);
        assert_eq!(d.max_attempts, s.max_attempts);
        assert_eq!(d.queue, s.queue);
        assert_eq!(d.trace_context, None);
    }

    #[test]
    fn inflight_round_trips_with_trace_context() {
        let tc = TraceContext {
            traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01".to_string(),
            tracestate: Some("vendor=abc".to_string()),
        };
        let s = sample_inflight("orders", Some(tc.clone()));
        let d = Inflight::decode(&s.encode()).expect("decodes");
        assert_eq!(d.queue, "orders");
        assert_eq!(d.trace_context, Some(tc));
    }

    #[test]
    fn inflight_round_trips_empty_queue() {
        let tc = TraceContext {
            traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01".to_string(),
            tracestate: None,
        };
        let s = sample_inflight("", Some(tc.clone()));
        let d = Inflight::decode(&s.encode()).expect("decodes");
        assert_eq!(d.queue, "");
        assert_eq!(d.trace_context, Some(tc));
    }

    #[test]
    fn inflight_decode_rejects_truncated_input() {
        assert!(Inflight::decode(&[]).is_err());
        assert!(Inflight::decode(&[0u8; 20]).is_err());
        let bytes = sample_inflight("q", None).encode();
        assert!(Inflight::decode(&bytes[..10]).is_err());
    }

    #[test]
    fn inflight_decode_rejects_invalid_queue_utf8() {
        let mut bytes = sample_inflight("ab", None).encode();
        bytes[30] = 0xff;
        bytes[31] = 0xff;
        assert!(Inflight::decode(&bytes).is_err());
    }
}
