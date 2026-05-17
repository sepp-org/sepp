use std::{error::Error, path::Path};

use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};

const DEFAULT_CONFIG_PATH: &str = "./sepp.toml";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistMode {
    SyncAll,
    SyncData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub db_path: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:50051".to_string(),
            db_path: "./sepp-data".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LimitsConfig {
    pub max_lease_duration_ms: u64,
    pub default_max_attempts: u32,
    pub max_reserve_batch: u32,
    pub max_wait_timeout_ms: u64,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_lease_duration_ms: 5 * 60 * 1000,
            default_max_attempts: 3,
            max_reserve_batch: 256,
            max_wait_timeout_ms: 5 * 60 * 1000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub persist_mode: PersistMode,
    pub sweep_interval_ms: u64,
    pub sweep_limit: usize,
    pub dedup_window_ms: i64,
    pub command_queue_capacity: usize,
    pub cache_size_bytes: Option<u64>,
    pub max_journaling_size_bytes: Option<u64>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            persist_mode: PersistMode::SyncAll,
            sweep_interval_ms: 1000,
            sweep_limit: 10_000,
            dedup_window_ms: 24 * 60 * 60 * 1000,
            command_queue_capacity: 1024,
            cache_size_bytes: None,
            max_journaling_size_bytes: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
    pub format: LogFormat,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: LogFormat::Text,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TracingConfig {
    pub enabled: bool,
    pub otlp_endpoint: String,
    pub service_name: String,
    pub sample_ratio: f64,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: "http://localhost:4317".to_string(),
            service_name: "sepp".to_string(),
            sample_ratio: 1.0,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub limits: LimitsConfig,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
    pub tracing: TracingConfig,
}

impl Config {
    pub fn load(explicit_path: Option<&str>) -> Result<Self, Box<dyn Error>> {
        let path = explicit_path.unwrap_or(DEFAULT_CONFIG_PATH);
        if explicit_path.is_some() && !Path::new(path).exists() {
            return Err(format!("config file not found: {path}").into());
        }
        let config: Config = Figment::new()
            .merge(Serialized::defaults(Config::default()))
            .merge(Toml::file(path))
            .merge(Env::prefixed("SEPP_").split("__"))
            .extract()?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        self.server
            .listen_addr
            .parse::<std::net::SocketAddr>()
            .map_err(|e| format!("server.listen_addr is not a valid address: {e}"))?;
        if self.server.db_path.is_empty() {
            return Err("server.db_path must not be empty".into());
        }
        if self.limits.max_lease_duration_ms == 0 {
            return Err("limits.max_lease_duration_ms must be > 0".into());
        }
        if self.limits.default_max_attempts == 0 {
            return Err("limits.default_max_attempts must be > 0".into());
        }
        if self.limits.max_reserve_batch == 0 {
            return Err("limits.max_reserve_batch must be > 0".into());
        }
        if self.limits.max_wait_timeout_ms == 0 {
            return Err("limits.max_wait_timeout_ms must be > 0".into());
        }
        if self.storage.sweep_interval_ms == 0 {
            return Err("storage.sweep_interval_ms must be > 0".into());
        }
        if self.storage.sweep_limit == 0 {
            return Err("storage.sweep_limit must be > 0".into());
        }
        if self.storage.command_queue_capacity == 0 {
            return Err("storage.command_queue_capacity must be > 0".into());
        }
        if self.storage.dedup_window_ms <= 0 {
            return Err("storage.dedup_window_ms must be > 0".into());
        }
        if matches!(self.storage.cache_size_bytes, Some(0)) {
            return Err("storage.cache_size_bytes must be > 0 when set".into());
        }
        if let Some(bytes) = self.storage.max_journaling_size_bytes
            && bytes < 64 * 1024 * 1024
        {
            return Err("storage.max_journaling_size_bytes must be >= 64 MiB when set".into());
        }
        if !(0.0..=1.0).contains(&self.tracing.sample_ratio) {
            return Err("tracing.sample_ratio must be in [0.0, 1.0]".into());
        }
        if self.tracing.enabled && self.tracing.service_name.is_empty() {
            return Err("tracing.service_name must not be empty".into());
        }
        Ok(())
    }
}
