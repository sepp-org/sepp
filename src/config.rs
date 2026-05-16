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
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_lease_duration_ms: 5 * 60 * 1000,
            default_max_attempts: 3,
            max_reserve_batch: 256,
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
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            persist_mode: PersistMode::SyncAll,
            sweep_interval_ms: 1000,
            sweep_limit: 10_000,
            dedup_window_ms: 24 * 60 * 60 * 1000,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub limits: LimitsConfig,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
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
        if self.storage.sweep_interval_ms == 0 {
            return Err("storage.sweep_interval_ms must be > 0".into());
        }
        if self.storage.sweep_limit == 0 {
            return Err("storage.sweep_limit must be > 0".into());
        }
        if self.storage.dedup_window_ms <= 0 {
            return Err("storage.dedup_window_ms must be > 0".into());
        }
        Ok(())
    }
}
