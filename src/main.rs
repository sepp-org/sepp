use std::time::Duration;

use tonic::transport::Server;

use crate::config::{Config, LogFormat, LoggingConfig};
use crate::pb::sepp::v1::queue_service_server::QueueServiceServer;
use crate::queue_server::QueueServer;
use tracing::info;

mod config;
mod pb;
mod queue_server;
mod storage;

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

fn init_tracing(cfg: &LoggingConfig) -> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new(&cfg.level))?;
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match cfg.format {
        LogFormat::Text => builder.init(),
        LogFormat::Json => builder.json().init(),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load(config_path_arg().as_deref())?;
    init_tracing(&config.logging)?;
    let addr = config.server.listen_addr.parse()?;
    let svc = QueueServer::new(&config)?;
    info!(%addr, db_path = %config.server.db_path, "queue server listening");
    Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
        .add_service(QueueServiceServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}
