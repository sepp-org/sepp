// The snapshot cadence task. openraft's own entry-count policy would dump
// the full database every few seconds at sepp's entry rates, so the engine
// runs with snapshotting disabled and this task calls trigger().snapshot()
// when BOTH hold: enough entries since the last snapshot AND enough wall
// time since the last build. Constants in v1, config keys only if demand
// appears.

use std::future::Future;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;
use tracing::debug;

// Retained log ≈ entry rate × floor × entry bytes; both defaults are sized
// in the design doc's disk-headroom formula.
pub const SNAPSHOT_ENTRY_THRESHOLD: u64 = 2_000_000;
pub const SNAPSHOT_TIME_FLOOR: Duration = Duration::from_secs(15 * 60);

// Applied and snapshot-covered log indexes, as the trigger task sees them
// (folded down from RaftMetrics by the caller).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogProgress {
    pub applied: Option<u64>,
    pub snapshot: Option<u64>,
}

pub struct SnapshotCadence {
    entry_threshold: u64,
    time_floor: Duration,
    last_built: Instant,
}

impl SnapshotCadence {
    pub fn new(entry_threshold: u64, time_floor: Duration) -> Self {
        Self {
            entry_threshold,
            time_floor,
            // Boot counts as a build: a floor's worth of quiet before the
            // first snapshot, so restarts never stampede.
            last_built: Instant::now(),
        }
    }

    // Time left until the floor stops gating; zero means only the entry
    // threshold gates.
    pub fn floor_remaining(&self, now: Instant) -> Duration {
        self.time_floor
            .saturating_sub(now.duration_since(self.last_built))
    }

    pub fn should_build(&self, now: Instant, progress: LogProgress) -> bool {
        let entries_since = match (progress.applied, progress.snapshot) {
            (Some(applied), Some(snapshot)) => applied.saturating_sub(snapshot),
            (Some(applied), None) => applied.saturating_add(1),
            (None, _) => 0,
        };
        entries_since >= self.entry_threshold && self.floor_remaining(now).is_zero()
    }

    pub fn built(&mut self, now: Instant) {
        self.last_built = now;
    }
}

// Drives the cadence against live progress. `trigger` requests one snapshot
// build and returns false when the engine is gone; PR 8 passes a closure
// over Raft::trigger().snapshot(). Exits when either side shuts down.
pub async fn run_snapshot_trigger<F, Fut>(
    mut progress: watch::Receiver<LogProgress>,
    mut cadence: SnapshotCadence,
    mut trigger: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    loop {
        // Wait out the time floor first: no metrics churn can force a build
        // inside it.
        let remaining = cadence.floor_remaining(Instant::now());
        if !remaining.is_zero() {
            tokio::time::sleep(remaining).await;
        }

        // Then wait for the entry threshold. A closed progress channel means
        // shutdown, checked even while the threshold holds.
        loop {
            if progress.has_changed().is_err() {
                return;
            }
            let current = *progress.borrow_and_update();
            if cadence.should_build(Instant::now(), current) {
                break;
            }
            if progress.changed().await.is_err() {
                return;
            }
        }

        debug!("snapshot cadence met; requesting a snapshot build");
        if !trigger().await {
            return;
        }
        cadence.built(Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress(applied: u64, snapshot: Option<u64>) -> LogProgress {
        LogProgress {
            applied: Some(applied),
            snapshot,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn both_gates_must_hold() {
        let mut cadence = SnapshotCadence::new(1000, Duration::from_secs(60));
        let start = Instant::now();

        // Entries alone: floored.
        assert!(!cadence.should_build(start, progress(5000, None)));

        // Time alone: below the entry threshold (indexes are 0-based, so
        // applied index 900 with no snapshot is 901 entries).
        tokio::time::advance(Duration::from_secs(61)).await;
        let now = Instant::now();
        assert!(!cadence.should_build(now, progress(900, None)));
        assert!(!cadence.should_build(now, progress(1500, Some(1000))));

        // Both: builds, and building re-arms the floor.
        assert!(cadence.should_build(now, progress(5000, Some(1000))));
        cadence.built(now);
        assert!(!cadence.should_build(now, progress(9000, Some(1000))));
    }

    #[tokio::test(start_paused = true)]
    async fn no_applied_entries_never_builds() {
        let cadence = SnapshotCadence::new(1, Duration::from_secs(0));
        assert!(!cadence.should_build(
            Instant::now(),
            LogProgress {
                applied: None,
                snapshot: None
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn driver_fires_once_both_gates_open() {
        let (tx, rx) = watch::channel(LogProgress::default());
        let (fired_tx, mut fired_rx) = tokio::sync::mpsc::unbounded_channel();

        let cadence = SnapshotCadence::new(100, Duration::from_secs(60));
        let driver = tokio::spawn(run_snapshot_trigger(rx, cadence, move || {
            let fired_tx = fired_tx.clone();
            async move {
                let _ = fired_tx.send(());
                true
            }
        }));

        // Threshold met immediately, but the floor holds it back.
        tx.send(progress(500, None)).unwrap();
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert!(fired_rx.try_recv().is_err(), "fired inside the time floor");

        // Passing the floor releases the pending build.
        tokio::time::advance(Duration::from_secs(31)).await;
        tokio::time::timeout(Duration::from_secs(5), fired_rx.recv())
            .await
            .expect("build triggered once the floor passed");

        // The next build waits out a fresh floor even with entries banked.
        tx.send(progress(5000, Some(500))).unwrap();
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert!(fired_rx.try_recv().is_err(), "floor did not re-arm");

        // Dropping the progress sender ends the driver.
        drop(tx);
        tokio::time::advance(Duration::from_secs(60)).await;
        driver.await.expect("driver exits when progress closes");
    }
}
