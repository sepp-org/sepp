use std::time::Duration;

use tonic::transport::Server;

use crate::config::Config;
use crate::pb::sepp::v1::queue_service_server::QueueServiceServer;
use crate::queue_server::QueueServer;
use tracing::info;

mod config;
mod pb;
mod queue_server;
mod storage;
mod telemetry;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load(config_path_arg().as_deref())?;
    let _telemetry = telemetry::init(&config.logging, &config.tracing)?;
    let addr = config.server.listen_addr.parse()?;
    let svc = QueueServer::new(&config)?;
    info!(%addr, db_path = %config.server.db_path, "queue server listening");
    Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
        .add_service(QueueServiceServer::new(svc))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;
    info!("queue server stopped");
    Ok(())
}

/// Resolves when the process receives a Ctrl+C, or a SIGTERM on Unix. tonic's
/// graceful shutdown then stops accepting connections and waits for in-flight
/// requests to finish before `serve_with_shutdown` returns.
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
