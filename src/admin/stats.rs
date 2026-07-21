use std::collections::{BTreeSet, HashMap};
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderName, header};
use axum::response::IntoResponse;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::config::Role;
use crate::storage::{AdminSnapshot, QueueTotals};

use super::{AdminState, Event};

#[derive(Clone, Copy, Default, Serialize)]
pub struct RateSample {
    pub ts_ms: i64,
    pub enqueued: f64,
    pub acked: f64,
    pub nacked: f64,
    pub dead_lettered: f64,
}

fn queue_names(state: &AdminState, snapshot: &AdminSnapshot) -> BTreeSet<String> {
    let mut names: BTreeSet<String> = snapshot
        .depths
        .ready
        .keys()
        .chain(snapshot.depths.scheduled.keys())
        .chain(snapshot.depths.inflight.keys())
        .chain(snapshot.depths.dead_letter.keys())
        .chain(snapshot.totals.keys())
        .cloned()
        .collect();
    names.extend(state.registry.load().declared_names().map(str::to_string));
    names
}

fn build_frame(
    state: &AdminState,
    snapshot: &AdminSnapshot,
    rates: &HashMap<String, RateSample>,
    seq: u64,
) -> Value {
    let depth = |m: &HashMap<String, u64>, name: &str| m.get(name).copied().unwrap_or(0);
    let mut queues = serde_json::Map::new();
    for name in queue_names(state, snapshot) {
        let totals = snapshot.totals.get(&name).cloned().unwrap_or_default();
        let r = rates.get(&name).copied().unwrap_or_default();
        queues.insert(
            name.clone(),
            json!({
                "ready": depth(&snapshot.depths.ready, &name),
                "scheduled": depth(&snapshot.depths.scheduled, &name),
                "inflight": depth(&snapshot.depths.inflight, &name),
                "dead_lettered": depth(&snapshot.depths.dead_letter, &name),
                "totals": {
                    "enqueued": totals.enqueued,
                    "reserved": totals.reserved,
                    "acked": totals.acked,
                    "nacked": totals.nacked,
                    "dead_lettered": totals.dead_lettered,
                },
                "rates": {
                    "enqueued": r.enqueued,
                    "acked": r.acked,
                    "nacked": r.nacked,
                    "dead_lettered": r.dead_lettered,
                },
            }),
        );
    }

    json!({
        "seq": seq,
        "ts_ms": snapshot.ts_ms,
        "server": { "command_queue_len": state.storage.command_queue_depth() },
        "queues": queues,
    })
}

// Publishes an initial frame so /overview and the SSE hello have one before
// the first hub tick.
pub(super) fn prime(state: &AdminState) {
    let snapshot = state.stats.load_full();
    let frame = build_frame(
        state,
        &snapshot,
        &HashMap::new(),
        state.frame_seq.load(Ordering::Relaxed),
    );
    state.latest_frame.store(Arc::new(frame));
}

pub(super) async fn run_hub(state: Arc<AdminState>, mut shutdown: watch::Receiver<bool>) {
    let mut prev: HashMap<String, QueueTotals> = HashMap::new();
    let mut last_tick = std::time::Instant::now();
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = shutdown.changed() => return,
        }

        let dt = last_tick.elapsed().as_secs_f64().max(0.001);
        last_tick = std::time::Instant::now();

        let snapshot = state.stats.load_full();
        let names = queue_names(&state, &snapshot);
        let rate = |cur: u64, prev: Option<u64>| match prev {
            Some(p) => (cur.saturating_sub(p) as f64 / dt * 100.0).round() / 100.0,
            None => 0.0,
        };

        let mut rates: HashMap<String, RateSample> = HashMap::new();
        for name in &names {
            let cur = snapshot.totals.get(name).cloned().unwrap_or_default();
            let was = prev.get(name);
            rates.insert(
                name.clone(),
                RateSample {
                    ts_ms: snapshot.ts_ms,
                    enqueued: rate(cur.enqueued, was.map(|t| t.enqueued)),
                    acked: rate(cur.acked, was.map(|t| t.acked)),
                    nacked: rate(cur.nacked, was.map(|t| t.nacked)),
                    dead_lettered: rate(cur.dead_lettered, was.map(|t| t.dead_lettered)),
                },
            );
        }
        prev = snapshot.totals.clone();

        {
            // One sample per second, so the cap is the retention in seconds.
            // Read per tick: shrinking on a hot reload trims rings right away.
            let cap = (state.config.load().admin.stats_history_ms / 1000).max(1) as usize;
            let mut history = state.history.write().expect("history lock");
            history.retain(|name, _| names.contains(name));
            for (name, sample) in &rates {
                let ring = history.entry(name.clone()).or_default();
                ring.push_back(*sample);
                while ring.len() > cap {
                    ring.pop_front();
                }
            }
        }

        let seq = state.frame_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let frame = build_frame(&state, &snapshot, &rates, seq);
        let serialized = frame.to_string();
        state.latest_frame.store(Arc::new(frame));
        let _ = state.hub.send(Event::Stats(Arc::new(serialized)));
    }
}

pub(super) async fn watch_reloads(state: Arc<AdminState>, mut shutdown: watch::Receiver<bool>) {
    let mut seq_rx = state.reload_seq.clone();
    loop {
        tokio::select! {
            changed = seq_rx.changed() => {
                if changed.is_err() {
                    return;
                }
                let seq = *seq_rx.borrow_and_update();
                let _ = state.hub.send(Event::Config(seq));
            }
            _ = shutdown.changed() => return,
        }
    }
}

pub(super) async fn events(
    viewer: super::authz::RequireViewer,
    State(state): State<Arc<AdminState>>,
) -> impl IntoResponse {
    let is_admin = viewer.0.role >= Role::Admin;
    let rx = state.hub.subscribe();

    let hello = {
        let history = state.history.read().expect("history lock");
        let frame = state.latest_frame.load_full();
        json!({ "history": &*history, "frame": &*frame }).to_string()
    };

    let first = tokio_stream::once(Ok::<_, Infallible>(
        SseEvent::default().event("hello").data(hello),
    ));
    let rest = BroadcastStream::new(rx).filter_map(move |event| match event {
        Ok(Event::Stats(frame)) => Some(Ok(SseEvent::default().event("stats").data(&*frame))),
        Ok(Event::Config(seq)) => Some(Ok(SseEvent::default()
            .event("config")
            .data(seq.to_string()))),
        Ok(Event::Audit(entry)) if is_admin => {
            Some(Ok(SseEvent::default().event("audit").data(&*entry)))
        }
        // The audit log is admin-only.
        Ok(Event::Audit(_)) => None,
        Err(BroadcastStreamRecvError::Lagged(n)) => {
            Some(Ok(SseEvent::default().event("lagged").data(n.to_string())))
        }
    });

    let sse =
        Sse::new(first.chain(rest)).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)));

    (
        [
            (header::CACHE_CONTROL, "no-cache"),
            (HeaderName::from_static("x-accel-buffering"), "no"),
        ],
        sse,
    )
}
