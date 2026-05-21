use std::process::ExitCode;
use std::time::Duration;

use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Identity, Server, ServerTlsConfig};

use sepp::auth::ApiKeyInterceptor;
use sepp::config::Config;
use sepp::metrics;
use sepp::pb::sepp::v1::queue_service_server::QueueServiceServer;
use sepp::queue_server::QueueServer;
use sepp::telemetry;
use tracing::{info, warn};

const EXAMPLE_CONFIG: &str = include_str!("../sepp.example.toml");

fn config_path_arg() -> Option<String> {
    let mut args = std::env::args();
    while let Some(arg) = args.next() {
        if arg == "--config" {
            return args.next();
        }
        if let Some(path) = arg.strip_prefix("--config=") {
            return Some(path.to_string());
        }
    }
    std::env::var("SEPP_CONFIG").ok()
}

fn handle_subcommand() -> Option<ExitCode> {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("config") {
        return None;
    }
    match args.next().as_deref() {
        Some("example") => {
            print!("{EXAMPLE_CONFIG}");
            Some(ExitCode::SUCCESS)
        }
        Some(other) => {
            eprintln!("sepp config: unknown subcommand '{other}'\n\nusage: sepp config example");
            Some(ExitCode::FAILURE)
        }
        None => {
            eprintln!("sepp config: missing subcommand\n\nusage: sepp config example");
            Some(ExitCode::FAILURE)
        }
    }
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn std::error::Error>> {
    if let Some(code) = handle_subcommand() {
        return Ok(code);
    }
    let config = Config::load(config_path_arg().as_deref())?;
    let _telemetry = telemetry::init(&config.logging, &config.tracing)?;
    install_panic_hook();
    let _metrics = metrics::init(&config.metrics, &config.tracing.service_name)?;
    let addr = config.server.listen_addr.parse()?;
    let svc = QueueServer::new(&config)?;

    let queue_service = QueueServiceServer::new(svc)
        .max_decoding_message_size(config.limits.max_message_bytes as usize);
    let interceptor = ApiKeyInterceptor::new(&config.auth.api_keys);
    let service = InterceptedService::new(queue_service, interceptor.clone());

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
    let on_off = |on: bool| if on { "enabled" } else { "disabled" };
    info!(
        %addr,
        db_path = %config.server.db_path,
        tls = on_off(tls_enabled),
        auth = on_off(interceptor.is_enforcing()),
        "queue server listening",
    );

    builder
        .add_service(service)
        .serve_with_shutdown(addr, shutdown_signal())
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
