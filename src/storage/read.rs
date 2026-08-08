use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use fjall::{Readable, SingleWriterTxDatabase as TxDatabase, SingleWriterTxKeyspace as TxKeyspace};
use prost::Message;
use tokio::sync::{Notify, futures::Notified};
use tonic::Status;

use crate::keys::{AuditValue, Inflight, JobValue, ReadyKey, TimerKey, deadline_of};
use crate::pb::sepp::storage::v1::AuditRecord;
use crate::pb::sepp::v1::{DeadLetterRecord, Job, Payload};
use crate::pb::{millis_to_timestamp, timestamp_to_millis};

use super::*;

// Past this many distinct queue names, `get` prunes notifiers that no Reserve
// is parked on; otherwise every queue name ever reserved leaks an Arc<Notify>
// for the process lifetime.
pub(crate) const NOTIFIER_PRUNE_THRESHOLD: usize = 4096;

#[derive(Clone, Default)]
pub(crate) struct QueueNotifiers {
    pub(crate) map: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
}

impl QueueNotifiers {
    pub(crate) fn get(&self, queue: &str) -> Arc<Notify> {
        let mut map = self.map.lock().unwrap();
        // An entry is safe to drop only when no JobWaiter holds it, i.e.
        // strong_count == 1 (just the map); every access takes this one mutex,
        // so the count read can't race a concurrent clone. Pruning before the
        // insert keeps the O(n) retain amortized.
        if map.len() >= NOTIFIER_PRUNE_THRESHOLD {
            map.retain(|_, n| Arc::strong_count(n) > 1);
        }
        Arc::clone(
            map.entry(queue.to_owned())
                .or_insert_with(|| Arc::new(Notify::new())),
        )
    }

    pub(crate) fn wake(&self, queue: &str) {
        if let Some(notify) = self.map.lock().unwrap().get(queue) {
            notify.notify_waiters();
        }
    }
}

pub struct JobWaiter {
    pub(crate) notifies: Vec<Arc<Notify>>,
}

impl JobWaiter {
    pub fn arm(&self) -> Armed<'_> {
        let waiters = self
            .notifies
            .iter()
            .map(|notify| {
                let mut waiter = Box::pin(notify.notified());
                waiter.as_mut().enable();
                waiter
            })
            .collect();

        Armed { waiters }
    }
}

pub struct Armed<'a> {
    pub(crate) waiters: Vec<Pin<Box<Notified<'a>>>>,
}

impl Future for Armed<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        for waiter in &mut self.get_mut().waiters {
            if waiter.as_mut().poll(cx).is_ready() {
                return Poll::Ready(());
            }
        }

        Poll::Pending
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AdminJobState {
    Ready,
    Scheduled,
    Inflight,
}

pub struct AdminJob {
    pub key: Vec<u8>,
    pub state: AdminJobState,
    pub job: Job,
}

pub struct AdminDeadLetter {
    pub key: Vec<u8>,
    pub record: DeadLetterRecord,
}

pub struct AuditEntry {
    pub seq: u64,
    pub ts_ms: i64,
    pub record: AuditRecord,
}

#[derive(Default)]
pub struct AuditFilter {
    pub actor: Option<String>,
    pub action_prefix: Option<String>,
}

impl AuditFilter {
    pub(crate) fn matches(&self, record: &AuditRecord) -> bool {
        self.actor.as_deref().is_none_or(|a| record.actor == a)
            && self
                .action_prefix
                .as_deref()
                .is_none_or(|p| record.action.starts_with(p))
    }
}

pub struct AuditPage {
    pub entries: Vec<AuditEntry>,
    // Resume cursor: pass as `before` to continue the walk. None means the
    // scan reached the oldest entry.
    pub next_before: Option<u64>,
}

// Rows examined per list_audit call. Bounds the work of a filtered walk: a
// page can come back short (even empty) with next_before set instead of
// scanning arbitrarily far for the next match.
pub(crate) const AUDIT_SCAN_CAP: usize = 1_000;

// Snapshot-read view of the database for admin reads off the committer
// thread: point gets, plus the bounded audit range. Methods are sync (callers
// wrap them in spawn_blocking) and peeked keys can vanish between peek and
// resolve, so misses are silently skipped.
#[derive(Clone)]
pub struct ReadHandle {
    pub(crate) db: TxDatabase,
    pub(crate) jobs: TxKeyspace,
    pub(crate) payloads: TxKeyspace,
    pub(crate) inflight: TxKeyspace,
    pub(crate) ready: TxKeyspace,
    pub(crate) scheduled: TxKeyspace,
    pub(crate) dead_letter: TxKeyspace,
    pub(crate) audit: TxKeyspace,
}

impl ReadHandle {
    pub(crate) fn load_job(&self, snap: &impl Readable, job_id: &str) -> Option<Job> {
        let stored = snap.get(&self.jobs, job_id.as_bytes()).ok().flatten()?;
        let (queue, mut job) = JobValue::decode(&stored).ok()?;
        job.queue = queue;
        if let Some(bytes) = snap.get(&self.payloads, job_id.as_bytes()).ok().flatten() {
            job.payload = Payload::decode(&*bytes).ok();
        }

        Some(job)
    }

    pub fn resolve_ready(&self, keys: &[Vec<u8>]) -> Vec<AdminJob> {
        let snap = self.db.read_tx();
        keys.iter()
            .filter_map(|key| {
                let job_id = ReadyKey::decode(key)?.job_id.to_owned();
                let attempt = snap
                    .get(&self.ready, key)
                    .ok()
                    .flatten()
                    .and_then(|v| v.first_chunk::<4>().map(|b| u32::from_be_bytes(*b)))?;
                let mut job = self.load_job(&snap, &job_id)?;
                job.attempt = attempt;

                Some(AdminJob {
                    key: key.clone(),
                    state: AdminJobState::Ready,
                    job,
                })
            })
            .collect()
    }

    pub fn resolve_scheduled(&self, keys: &[Vec<u8>]) -> Vec<AdminJob> {
        let snap = self.db.read_tx();
        keys.iter()
            .filter_map(|key| {
                let job_id = std::str::from_utf8(TimerKey::job_id(key)?).ok()?;
                let attempt = snap
                    .get(&self.scheduled, key)
                    .ok()
                    .flatten()
                    .and_then(|v| v.first_chunk::<4>().map(|b| u32::from_be_bytes(*b)))?;
                let mut job = self.load_job(&snap, job_id)?;
                job.attempt = attempt;

                Some(AdminJob {
                    key: key.clone(),
                    state: AdminJobState::Scheduled,
                    job,
                })
            })
            .collect()
    }

    pub fn resolve_inflight(&self, keys: &[Vec<u8>]) -> Vec<AdminJob> {
        let snap = self.db.read_tx();
        keys.iter()
            .filter_map(|key| {
                let job_id = std::str::from_utf8(TimerKey::job_id(key)?).ok()?;
                let stored = snap.get(&self.inflight, job_id.as_bytes()).ok().flatten()?;
                let inflight = Inflight::decode(&stored).ok()?;
                // A peeked lease key goes stale when the job is extended or
                // re-reserved; only the key matching the live lease counts.
                if inflight.lease_expires_at != deadline_of(key) {
                    return None;
                }
                let mut job = self.load_job(&snap, job_id)?;
                job.attempt = inflight.attempt;
                job.lease_expires_at = Some(millis_to_timestamp(inflight.lease_expires_at));

                Some(AdminJob {
                    key: key.clone(),
                    state: AdminJobState::Inflight,
                    job,
                })
            })
            .collect()
    }

    pub fn resolve_dead_letters(&self, keys: &[Vec<u8>]) -> Vec<AdminDeadLetter> {
        let snap = self.db.read_tx();
        keys.iter()
            .filter_map(|key| {
                let stored = snap.get(&self.dead_letter, key).ok().flatten()?;
                // The record embeds the job and its payload (see
                // maybe_store_dead_letter); no further lookups needed.
                let record = DeadLetterRecord::decode(&*stored).ok()?;

                Some(AdminDeadLetter {
                    key: key.clone(),
                    record,
                })
            })
            .collect()
    }

    // A nack only carries the job ID, so we need a way to look up the queue
    // to get the effective limits applied for that queue. It's a point read
    // on a tx snapshot, which is cheap and doesn't contend with the committer
    // thread. A stale answer is harmless (attempt fencing rejects the op at
    // apply); a read error is not, so it fails the nack instead of resolving
    // a wrong delay.
    pub(crate) fn queue_of_inflight_job(&self, job_id: &str) -> Result<Option<String>, Status> {
        let snap = self.db.read_tx();
        let Some(stored) = snap
            .get(&self.inflight, job_id.as_bytes())
            .map_err(stg_err)?
        else {
            return Ok(None);
        };
        Ok(Some(Inflight::decode(&stored)?.queue))
    }

    pub fn get_job(&self, job_id: &str) -> Option<AdminJob> {
        let snap = self.db.read_tx();
        let inflight = snap
            .get(&self.inflight, job_id.as_bytes())
            .ok()
            .flatten()
            .and_then(|stored| Inflight::decode(&stored).ok());

        let mut job = self.load_job(&snap, job_id)?;
        if let Some(inflight) = inflight {
            job.attempt = inflight.attempt;
            job.lease_expires_at = Some(millis_to_timestamp(inflight.lease_expires_at));
            let key = TimerKey {
                deadline: inflight.lease_expires_at,
                job_id,
            }
            .encode();

            return Some(AdminJob {
                key,
                state: AdminJobState::Inflight,
                job,
            });
        }

        // Keys are reconstructed best-effort: a nack-retry's timer deadline
        // is not recoverable from the job record, so such jobs report Ready.
        let scheduled_at = job.scheduled_at.as_ref().map(timestamp_to_millis);
        let (state, key) = match scheduled_at {
            Some(at) if at > now_ms() => (
                AdminJobState::Scheduled,
                TimerKey {
                    deadline: at,
                    job_id,
                }
                .encode(),
            ),
            _ => (
                AdminJobState::Ready,
                ReadyKey {
                    queue: &job.queue,
                    priority: job.priority,
                    enqueued_at: job
                        .enqueued_at
                        .as_ref()
                        .map(timestamp_to_millis)
                        .unwrap_or(0),
                    job_id,
                }
                .encode(),
            ),
        };

        Some(AdminJob { key, state, job })
    }

    // The keyspace is cold (admin actions only) and every call scans at
    // most AUDIT_SCAN_CAP rows, so it does not contend with the committer.
    pub fn list_audit(&self, before: Option<u64>, limit: usize, filter: &AuditFilter) -> AuditPage {
        let snap = self.db.read_tx();
        let iter = match before {
            Some(seq) => snap.range(&self.audit, ..seq.to_be_bytes().to_vec()),
            None => snap.iter(&self.audit),
        };

        let mut entries = Vec::new();
        let mut last_seq = 0;
        for (scanned, guard) in iter.rev().enumerate() {
            // Checked before consuming the pulled row, so next_before is only
            // set when at least one unexamined row remains.
            if entries.len() >= limit || scanned >= AUDIT_SCAN_CAP {
                return AuditPage {
                    entries,
                    next_before: Some(last_seq),
                };
            }
            let Ok((key, value)) = guard.into_inner() else {
                continue;
            };
            let Ok(bytes) = <[u8; 8]>::try_from(key.as_ref()) else {
                continue;
            };
            last_seq = u64::from_be_bytes(bytes);
            if let Some((ts_ms, record)) = AuditValue::decode(&value)
                && filter.matches(&record)
            {
                entries.push(AuditEntry {
                    seq: last_seq,
                    ts_ms,
                    record,
                });
            }
        }

        AuditPage {
            entries,
            next_before: None,
        }
    }
}
