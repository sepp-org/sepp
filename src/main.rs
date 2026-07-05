use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Identity, Server, ServerTlsConfig};

use sepp::admin::{self, AdminState};
use sepp::auth::ApiKeyInterceptor;
use sepp::config::{Config, DEFAULT_CONFIG_PATH};
use sepp::config_watch::{self, ReloadState};
use sepp::metrics;
use sepp::pb::sepp::v1::queue_service_server::QueueServiceServer;
use sepp::queue_server::QueueServer;
use sepp::queues::QueueRegistry;
use sepp::telemetry;
use tracing::{debug, info, warn};

const EXAMPLE_CONFIG: &str = include_str!("../sepp.example.toml");

#[derive(Parser)]
#[command(
    name = "sepp",
    version,
    about = "A language-agnostic durable job queue"
)]
struct Cli {
    #[arg(long, env = "SEPP_CONFIG", value_name = "PATH")]
    config: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    Example,
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if let Some(Commands::Config {
        action: ConfigAction::Example,
    }) = &cli.command
    {
        print!("{EXAMPLE_CONFIG}");
        return Ok(ExitCode::SUCCESS);
    }

    let config_path = cli.config;
    let config = Config::load(config_path.as_deref())?;
    let _telemetry = telemetry::init(&config.logging, &config.tracing)?;

    install_panic_hook();
    debug!(
        persist_mode = ?config.storage.persist_mode,
        sweep_interval_ms = config.storage.sweep_interval_ms,
        sweep_limit = config.storage.sweep_limit,
        command_queue_capacity = config.storage.command_queue_capacity,
        tracing_enabled = config.tracing.enabled,
        sample_ratio = config.tracing.sample_ratio,
        metrics_enabled = config.metrics.enabled,
        metrics_export_interval_ms = config.metrics.export_interval_ms,
        prometheus_enabled = config.metrics.prometheus_enabled,
        "configuration loaded",
    );

    let _metrics = metrics::init(&config.metrics, &config.tracing.service_name).await?;
    let addr = config.server.listen_addr;

    let registry = QueueRegistry::from_config(&config);
    let declared_queues = registry.declared_count();
    if declared_queues > 0 {
        let names: Vec<&str> = registry.declared_names().collect();
        info!(queues = ?names, "declared queues from config");
    }

    let registry = registry.into_shared();
    let shared_config = config.clone().into_shared();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (reload_seq_tx, reload_seq_rx) = tokio::sync::watch::channel(0u64);
    let svc = QueueServer::new(shared_config.clone(), registry.clone(), shutdown_rx)?;
    // QueueServiceServer::new consumes svc, so the admin UI's storage handle
    // must be cloned out first.
    let admin_storage = config.admin.enabled.then(|| svc.storage());
    let queue_service = QueueServiceServer::new(svc)
        .max_decoding_message_size(config.limits.max_message_bytes as usize);
    let interceptor = ApiKeyInterceptor::new(&config.auth.api_keys);
    let service = InterceptedService::new(queue_service, interceptor.clone());

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<QueueServiceServer<QueueServer>>()
        .await;

    let tls_enabled = config.server.tls_enabled();
    let mut builder = Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)));
    if tls_enabled {
        let identity = load_tls_identity(&config)?;
        builder = builder.tls_config(ServerTlsConfig::new().identity(identity))?;
    }

    if interceptor.is_enforcing() && !tls_enabled {
        warn!("API-key auth is enabled without TLS; keys are sent in plaintext");
    }

    let incoming = TcpIncoming::bind(addr)
        .map_err(|e| format!("binding {addr}: {e}"))?
        .with_nodelay(Some(true)); // Disable Nagle
    let local_addr = incoming
        .local_addr()
        .map_err(|e| format!("resolving bound address: {e}"))?;

    let on_off = |on: bool| if on { "enabled" } else { "disabled" };
    info!(
        addr = %local_addr,
        db_path = %config.server.db_path,
        tls = on_off(tls_enabled),
        auth = on_off(interceptor.is_enforcing()),
        strict_queues = on_off(config.server.strict_queues),
        declared_queues,
        "queue server listening",
    );

    let watch_path = config_path.as_deref().unwrap_or(DEFAULT_CONFIG_PATH);
    if std::path::Path::new(watch_path).exists() {
        let state = ReloadState {
            config: shared_config.clone(),
            registry: registry.clone(),
            interceptor: interceptor.clone(),
            reload_seq: std::sync::Arc::new(reload_seq_tx),
        };
        match config_watch::spawn(watch_path.into(), state) {
            Ok(()) => info!(path = %watch_path, "watching config file for hot reload"),
            Err(e) => warn!(error = %e, "could not start config watcher; hot reload disabled"),
        }
    } else {
        debug!(path = %watch_path, "no config file on disk; hot reload disabled");
    }

    if config.admin.enabled {
        if let Some(keys) = &config.admin.keys
            && keys.iter().any(|k| k.key == "admin")
        {
            warn!(
                "an [admin] key is the literal string \"admin\" (the default) \
                 Change it before \
                 exposing the port beyond this machine"
            );
        }
        let storage = admin_storage.expect("storage captured when admin is enabled");
        let state = AdminState::new(
            storage,
            shared_config.clone(),
            config.clone(),
            registry.clone(),
            watch_path.into(),
            reload_seq_rx,
        );
        admin::spawn(state, config.admin.listen_addr, shutdown_tx.subscribe()).await?;
    }

    builder
        .add_service(health_service)
        .add_service(service)
        .serve_with_incoming_shutdown(incoming, async move {
            shutdown_signal().await;
            // Unblocks long-poll reserves so the drain doesn't wait out
            // max_wait_timeout.
            let _ = shutdown_tx.send(true);
        })
        .await?;

    info!("queue server stopped");
    Ok(ExitCode::SUCCESS)
}

fn load_tls_identity(config: &Config) -> Result<Identity, Box<dyn std::error::Error>> {
    let cert_path = config
        .server
        .tls_cert_path
        .as_deref()
        .expect("tls_cert_path set");

    let key_path = config
        .server
        .tls_key_path
        .as_deref()
        .expect("tls_key_path set");

    let cert = std::fs::read(cert_path)
        .map_err(|e| format!("reading server.tls_cert_path ({cert_path}): {e}"))?;
    let key = std::fs::read(key_path)
        .map_err(|e| format!("reading server.tls_key_path ({key_path}): {e}"))?;

    Ok(Identity::from_pem(cert, key))
}

// Catch panics for logging
fn install_panic_hook() {
    let default = std::panic::take_hook();

    std::panic::set_hook(Box::new(move |info| {
        tracing::error!("panic: {info}");
        default(info);
    }));
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }

    info!("shutdown signal received");
}
