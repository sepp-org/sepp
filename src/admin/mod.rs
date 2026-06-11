pub mod assets;
pub mod auth;
pub mod authz;
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
use axum_server::tls_rustls::RustlsConfig;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::config::{Config, SharedConfig};
use crate::queues::SharedRegistry;
use crate::storage::{AdminSnapshot, ReadHandle, Storage, now_ms};

use auth::SessionStore;
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
    pub sessions: SessionStore,
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
            sessions: SessionStore::default(),
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
    let tls = state
        .boot
        .admin
        .tls_enabled()
        .then(|| tls_config(&state.boot.admin))
        .transpose()?;

    let state = Arc::new(state);
    stats::prime(&state);

    let listener =
        std::net::TcpListener::bind(addr).map_err(|e| format!("binding admin UI {addr}: {e}"))?;
    listener.set_nonblocking(true)?;
    let local_addr = listener.local_addr()?;
    let scheme = if tls.is_some() { "https" } else { "http" };
    let tls_label = if tls.is_some() { "enabled" } else { "disabled" };
    if local_addr.ip().is_unspecified() {
        // An unspecified bind serves every interface but is not a browsable
        // host itself; loopback is always among them.
        info!(
            addr = %local_addr,
            tls = tls_label,
            "admin UI listening at {scheme}://localhost:{}",
            local_addr.port(),
        );
    } else {
        info!(
            tls = tls_label,
            "admin UI listening at {scheme}://{local_addr}",
        );
    }
    if state.boot.admin.keys.is_some() && tls.is_none() && !local_addr.ip().is_loopback() {
        warn!("admin keys are enabled without TLS; keys and session cookies are sent in plaintext");
    }

    tokio::spawn(stats::run_hub(state.clone(), shutdown.clone()));
    tokio::spawn(stats::watch_reloads(state.clone(), shutdown.clone()));

    let app = router(state);
    let handle = axum_server::Handle::new();
    {
        let handle = handle.clone();
        let mut shutdown = shutdown;
        tokio::spawn(async move {
            let _ = shutdown.changed().await;
            // SSE streams end once the stats hub observes the same shutdown
            // signal; the timeout only backstops stuck connections.
            handle.graceful_shutdown(Some(std::time::Duration::from_secs(10)));
        });
    }

    let serve: std::pin::Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> = match tls {
        Some(tls) => Box::pin(
            axum_server::from_tcp_rustls(listener, tls)
                .map_err(|e| format!("admin UI listener: {e}"))?
                .handle(handle)
                .serve(app.into_make_service()),
        ),
        None => Box::pin(
            axum_server::from_tcp(listener)
                .map_err(|e| format!("admin UI listener: {e}"))?
                .handle(handle)
                .serve(app.into_make_service()),
        ),
    };
    let task = tokio::spawn(async move {
        if let Err(e) = serve.await {
            error!(error = %e, "admin UI server stopped with error");
        }
    });

    Ok((local_addr, task))
}

fn tls_config(admin: &crate::config::AdminConfig) -> Result<RustlsConfig, Box<dyn Error>> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let cert_path = admin.tls_cert_path.as_deref().expect("tls_cert_path set");
    let key_path = admin.tls_key_path.as_deref().expect("tls_key_path set");

    let certs: Vec<CertificateDer> = CertificateDer::pem_file_iter(cert_path)
        .map_err(|e| format!("reading admin.tls_cert_path ({cert_path}): {e}"))?
        .collect::<Result<_, _>>()
        .map_err(|e| format!("parsing admin.tls_cert_path ({cert_path}): {e}"))?;
    let key = PrivateKeyDer::from_pem_file(key_path)
        .map_err(|e| format!("reading admin.tls_key_path ({key_path}): {e}"))?;

    // An explicit provider rather than rustls's process default: tonic's
    // tls-ring already puts ring in the graph and the default would panic at
    // runtime if any future dependency also enabled aws-lc-rs.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("admin TLS protocol versions: {e}"))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("admin TLS identity: {e}"))?;

    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(RustlsConfig::from_config(Arc::new(config)))
}

fn router(state: Arc<AdminState>) -> Router {
    Router::new()
        .route(
            "/admin/api/v1/session",
            post(auth::login).get(auth::session).delete(auth::logout),
        )
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
        // Registered routes only; the asset fallback below stays public so the
        // SPA shell and the login page itself load unauthenticated.
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require,
        ))
        .fallback(assets::serve)
        .with_state(state)
}
