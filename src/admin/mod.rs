pub mod assets;
pub mod config_edit;
pub mod routes;
pub mod stats;

use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwap;
use axum::Router;
use axum::routing::{get, post};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::config::{Config, SharedConfig};
use crate::queues::SharedRegistry;
use crate::storage::{AdminSnapshot, ReadHandle, Storage, now_ms};

use stats::RateSample;

// Pre-serialized broadcast events fanned out to every SSE subscriber.
#[derive(Clone)]
pub enum Event {
    Stats(Arc<String>),
    Config(u64),
}

pub type History = Arc<RwLock<HashMap<String, VecDeque<RateSample>>>>;

#[derive(Clone)]
pub struct AdminState {
    pub storage: Storage,
    pub read: ReadHandle,
    pub stats: Arc<ArcSwap<AdminSnapshot>>,
    pub config: SharedConfig,
    // The config the server booted with. Hot reloads store the whole on-disk
    // config into `config`, including restart-only fields whose running
    // values never change; for those, `boot` is the truth.
    pub boot: Arc<Config>,
    pub registry: SharedRegistry,
    pub hub: broadcast::Sender<Event>,
    pub history: History,
    // The latest StatsHub frame, reused by /overview and the SSE hello event.
    pub latest_frame: Arc<ArcSwap<serde_json::Value>>,
    pub frame_seq: Arc<AtomicU64>,
    pub config_path: PathBuf,
    pub config_write_lock: Arc<tokio::sync::Mutex<()>>,
    pub reload_seq: watch::Receiver<u64>,
    pub started_at_ms: i64,
}

impl AdminState {
    pub fn new(
        storage: Storage,
        config: SharedConfig,
        boot: Config,
        registry: SharedRegistry,
        config_path: PathBuf,
        reload_seq: watch::Receiver<u64>,
    ) -> Self {
        let (hub, _) = broadcast::channel(32);
        Self {
            read: storage.read_handle(),
            stats: storage.admin_stats(),
            storage,
            config,
            boot: Arc::new(boot),
            registry,
            hub,
            history: Arc::new(RwLock::new(HashMap::new())),
            latest_frame: Arc::new(ArcSwap::from_pointee(serde_json::Value::Null)),
            frame_seq: Arc::new(AtomicU64::new(0)),
            config_path,
            config_write_lock: Arc::new(tokio::sync::Mutex::new(())),
            reload_seq,
            started_at_ms: now_ms(),
        }
    }
}

pub async fn spawn(
    state: AdminState,
    addr: SocketAddr,
    shutdown: watch::Receiver<bool>,
) -> Result<(SocketAddr, JoinHandle<()>), Box<dyn Error>> {
    let state = Arc::new(state);
    stats::prime(&state);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("binding admin UI {addr}: {e}"))?;
    let local_addr = listener.local_addr()?;
    info!(addr = %local_addr, "admin UI listening");

    tokio::spawn(stats::run_hub(state.clone(), shutdown.clone()));
    tokio::spawn(stats::watch_reloads(state.clone(), shutdown.clone()));

    let app = router(state);
    let mut shutdown = shutdown;
    let handle = tokio::spawn(async move {
        let serve = axum::serve(listener, app).with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        });
        if let Err(e) = serve.await {
            error!(error = %e, "admin UI server stopped with error");
        }
    });

    Ok((local_addr, handle))
}

fn router(state: Arc<AdminState>) -> Router {
    Router::new()
        .route("/admin/api/v1/overview", get(routes::overview))
        .route("/admin/api/v1/queues", get(routes::list_queues))
        .route(
            "/admin/api/v1/queues/{name}",
            get(routes::get_queue)
                .put(routes::put_queue)
                .delete(routes::delete_queue),
        )
        .route(
            "/admin/api/v1/queues/{name}/jobs",
            get(routes::list_jobs).post(routes::enqueue_job),
        )
        .route(
            "/admin/api/v1/queues/{name}/dead-letters/{key_b64}",
            get(routes::get_dead_letter),
        )
        .route(
            "/admin/api/v1/queues/{name}/jobs:dead-letter",
            post(routes::dead_letter_jobs),
        )
        .route(
            "/admin/api/v1/queues/{name}/dead-letters:requeue",
            post(routes::requeue_dead_letters),
        )
        .route(
            "/admin/api/v1/queues/{name}/dead-letters:delete",
            post(routes::delete_dead_letters),
        )
        .route("/admin/api/v1/jobs/{id}", get(routes::get_job))
        .route(
            "/admin/api/v1/config",
            get(routes::get_config).put(routes::put_config),
        )
        .route("/admin/api/v1/server-info", get(routes::server_info))
        .route("/admin/api/v1/events", get(stats::events))
        .fallback(assets::serve)
        .with_state(state)
}
