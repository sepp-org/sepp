use std::collections::{BTreeMap, HashMap};

use fjall::Readable;
use tracing::error;

use crate::keys::{
    CLOSING_PREFIX, DeadLetterKey, DedupTimerKey, Inflight, TimerKey, closing_queue, deadline_of,
    read_queue,
};
use crate::metrics::QueueDepthSnapshot;

use super::*;

pub(crate) fn bump_queue(map: &mut HashMap<String, u64>, queue: &str) {
    *map.entry(queue.to_string()).or_default() += 1;
}

pub(crate) fn drop_queue(map: &mut HashMap<String, u64>, queue: &str) {
    if let Some(n) = map.get_mut(queue) {
        *n -= 1;
        if *n == 0 {
            map.remove(queue);
        }
    }
}

#[derive(Default)]
pub(crate) struct ReadyIndex {
    pub(crate) keys: BTreeMap<Vec<u8>, u32>,
    pub(crate) by_queue: HashMap<String, u64>,
}

impl ReadyIndex {
    pub(crate) fn insert(&mut self, ready_key: Vec<u8>, attempt: u32) {
        if !self.keys.contains_key(&ready_key)
            && let Some(queue) = read_queue(&ready_key)
        {
            bump_queue(&mut self.by_queue, queue);
        }

        self.keys.insert(ready_key, attempt);
    }

    pub(crate) fn pop_front(&mut self, queue_prefix: &[u8]) -> Option<(Vec<u8>, u32)> {
        let key = self
            .keys
            .range(queue_prefix.to_vec()..)
            .next()
            .filter(|(k, _)| k.starts_with(queue_prefix))
            .map(|(k, _)| k.clone())?;

        let attempt = self.keys.remove(&key)?;
        if let Some(queue) = read_queue(&key) {
            drop_queue(&mut self.by_queue, queue);
        }

        Some((key, attempt))
    }

    pub(crate) fn attempt(&self, ready_key: &[u8]) -> Option<u32> {
        self.keys.get(ready_key).copied()
    }

    pub(crate) fn remove(&mut self, ready_key: &[u8]) -> Option<u32> {
        let attempt = self.keys.remove(ready_key)?;
        if let Some(queue) = read_queue(ready_key) {
            drop_queue(&mut self.by_queue, queue);
        }

        Some(attempt)
    }
}

// Timer keys are `deadline | job_id` and carry no queue, so each key stores
// its owning queue as the map value. Caller passes it on insert; pop_due /
// remove return it so we can keep the by_queue counter in sync without an
// extra DB lookup.
#[derive(Default)]
pub(crate) struct TimerIndex {
    pub(crate) keys: BTreeMap<Vec<u8>, String>,
    pub(crate) by_queue: HashMap<String, u64>,
}

impl TimerIndex {
    pub(crate) fn insert(&mut self, key: Vec<u8>, queue: &str) {
        if !self.keys.contains_key(&key) {
            bump_queue(&mut self.by_queue, queue);
        }

        self.keys.insert(key, queue.to_string());
    }

    pub(crate) fn remove(&mut self, key: &[u8]) -> Option<String> {
        let queue = self.keys.remove(key)?;
        drop_queue(&mut self.by_queue, &queue);

        Some(queue)
    }

    pub(crate) fn pop_due(&mut self, now: i64) -> Option<(Vec<u8>, String)> {
        let (key, _) = self.keys.iter().next()?;
        if deadline_of(key) > now {
            return None;
        }
        let key = key.clone();
        let queue = self.keys.remove(&key)?;
        drop_queue(&mut self.by_queue, &queue);

        Some((key, queue))
    }

    pub(crate) fn earliest(&self) -> Option<i64> {
        self.keys.keys().next().map(|k| deadline_of(k))
    }

    pub(crate) fn iter_oldest(&self) -> impl Iterator<Item = (&[u8], &str)> {
        self.keys.iter().map(|(k, v)| (k.as_slice(), v.as_str()))
    }
}

#[derive(Default)]
pub(crate) struct Indexes {
    pub(crate) ready: ReadyIndex,
    pub(crate) scheduled: TimerIndex,
    pub(crate) leases: TimerIndex,
    pub(crate) dedup_timers: TimerIndex,
    pub(crate) dead_letter: TimerIndex,
    // Queues an admin delete is draining, mapped to a grace deadline (ms). While
    // a queue is closing, enqueues to it are rejected (QueueClosing) so the
    // delete's purge loop is guaranteed to drain rather than livelock against a
    // concurrent producer. The deadline auto-clears the tombstone if the delete
    // handler dies; the handler refreshes it each chunk and clears it on finish.
    // Mirrors the `closing/<queue>` rows in `meta`.
    pub(crate) closing: HashMap<String, i64>,
}

impl Indexes {
    pub(crate) fn live_depth(&self, queue: &str) -> u64 {
        self.ready.by_queue.get(queue).copied().unwrap_or(0)
            + self.scheduled.by_queue.get(queue).copied().unwrap_or(0)
            + self.leases.by_queue.get(queue).copied().unwrap_or(0)
    }

    pub(crate) fn depth_counts(&self, queue: &str) -> QueueDepthCounts {
        QueueDepthCounts {
            ready: self.ready.by_queue.get(queue).copied().unwrap_or(0),
            scheduled: self.scheduled.by_queue.get(queue).copied().unwrap_or(0),
            inflight: self.leases.by_queue.get(queue).copied().unwrap_or(0),
            dead_letter: self.dead_letter.by_queue.get(queue).copied().unwrap_or(0),
        }
    }

    pub(crate) fn snapshot(&self) -> QueueDepthSnapshot {
        QueueDepthSnapshot {
            ready: self.ready.by_queue.clone(),
            scheduled: self.scheduled.by_queue.clone(),
            inflight: self.leases.by_queue.clone(),
            dead_letter: self.dead_letter.by_queue.clone(),
        }
    }
}
pub(crate) fn rebuild_indexes(store: &Store) -> Result<Indexes, fjall::Error> {
    let mut indexes = Indexes::default();
    let snap = store.db.read_tx();

    for guard in snap.iter(&store.ready) {
        let (key, value) = guard.into_inner()?;
        let attempt = value
            .first_chunk::<4>()
            .map(|b| u32::from_be_bytes(*b))
            .unwrap_or(1);
        indexes.ready.insert(key.to_vec(), attempt);
    }

    for guard in snap.iter(&store.scheduled) {
        let (key, _) = guard.into_inner()?;
        let queue = TimerKey::job_id(&key)
            .and_then(|job_id| snap.get(&store.jobs, job_id).ok().flatten())
            .and_then(|stored| read_queue(&stored).map(str::to_owned))
            .unwrap_or_default();
        indexes.scheduled.insert(key.to_vec(), &queue);
    }

    for guard in snap.iter(&store.leases) {
        let (key, _) = guard.into_inner()?;
        let queue = TimerKey::job_id(&key)
            .and_then(|job_id| snap.get(&store.inflight, job_id).ok().flatten())
            .and_then(|stored| Inflight::decode(&stored).ok().map(|i| i.queue))
            .unwrap_or_default();
        indexes.leases.insert(key.to_vec(), &queue);
    }

    for guard in snap.iter(&store.dedup_timers) {
        let (key, _) = guard.into_inner()?;
        let queue = DedupTimerKey::queue(&key).unwrap_or("").to_string();
        indexes.dedup_timers.insert(key.to_vec(), &queue);
    }

    for guard in snap.iter(&store.dead_letter) {
        let key = guard.key()?;
        let queue = DeadLetterKey::queue(&key).unwrap_or("").to_string();
        indexes.dead_letter.insert(key.to_vec(), &queue);
    }

    // Because sweep works based on the in-memory indexes, we must load them all at boot.
    // Otherwise the expired tombstones would be orphaned.
    for guard in snap.prefix(&store.meta, CLOSING_PREFIX) {
        let (key, value) = guard.into_inner()?;
        let Some(queue) = closing_queue(&key) else {
            continue;
        };
        let deadline = value
            .first_chunk::<8>()
            .map(|b| i64::from_be_bytes(*b))
            .unwrap_or(0);
        indexes.closing.insert(queue.to_string(), deadline);
    }

    Ok(indexes)
}

pub(crate) fn resync(store: &Store, indexes: &mut Indexes) {
    match rebuild_indexes(store) {
        Ok(fresh) => *indexes = fresh,
        Err(e) => error!(error = %e, "could not re-sync the in-memory indexes"),
    }
}
