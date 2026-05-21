use std::{collections::HashSet, error::Error, path::Path};

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
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
    pub strict_queues: bool,
}

impl ServerConfig {
    pub fn tls_enabled(&self) -> bool {
        self.tls_cert_path.is_some() && self.tls_key_path.is_some()
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:50051".to_string(),
            db_path: "./sepp-data".to_string(),
            tls_cert_path: None,
            tls_key_path: None,
            strict_queues: false,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct QueueConfig {
    pub name: String,
    pub max_lease_duration_ms: Option<u64>,
    pub default_max_attempts: Option<u32>,
    pub max_attempts_ceiling: Option<u32>,
    pub default_priority: Option<u32>,
    pub max_payload_bytes: Option<u64>,
    pub allowed_encodings: Option<Vec<String>>,
    pub max_schedule_horizon_ms: Option<u64>,
    pub max_custom_entries: Option<u32>,
    pub max_custom_total_bytes: Option<u64>,
    pub max_custom_key_bytes: Option<u32>,
    pub dedup_window_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct EffectiveLimits {
    pub max_lease_duration_ms: u64,
    pub default_max_attempts: u32,
    pub max_attempts_ceiling: u32,
    pub default_priority: u32,
    pub max_payload_bytes: u64,
    pub allowed_encodings: Option<Vec<String>>,
    pub max_schedule_horizon_ms: u64,
    pub max_custom_entries: u32,
    pub max_custom_total_bytes: u64,
    pub max_custom_key_bytes: u32,
    pub dedup_window_ms: i64,
}

impl EffectiveLimits {
    pub fn from_globals(limits: &LimitsConfig, storage: &StorageConfig) -> Self {
        Self {
            max_lease_duration_ms: limits.max_lease_duration_ms,
            default_max_attempts: limits.default_max_attempts,
            max_attempts_ceiling: limits.max_attempts_ceiling,
            default_priority: limits.default_priority,
            max_payload_bytes: limits.max_payload_bytes,
            allowed_encodings: limits.allowed_encodings.clone(),
            max_schedule_horizon_ms: limits.max_schedule_horizon_ms,
            max_custom_entries: limits.max_custom_entries,
            max_custom_total_bytes: limits.max_custom_total_bytes,
            max_custom_key_bytes: limits.max_custom_key_bytes,
            dedup_window_ms: storage.dedup_window_ms,
        }
    }

    pub fn merged_with(&self, q: &QueueConfig) -> Self {
        Self {
            max_lease_duration_ms: q
                .max_lease_duration_ms
                .unwrap_or(self.max_lease_duration_ms),
            default_max_attempts: q.default_max_attempts.unwrap_or(self.default_max_attempts),
            max_attempts_ceiling: q.max_attempts_ceiling.unwrap_or(self.max_attempts_ceiling),
            default_priority: q.default_priority.unwrap_or(self.default_priority),
            max_payload_bytes: q.max_payload_bytes.unwrap_or(self.max_payload_bytes),
            allowed_encodings: q
                .allowed_encodings
                .clone()
                .or_else(|| self.allowed_encodings.clone()),
            max_schedule_horizon_ms: q
                .max_schedule_horizon_ms
                .unwrap_or(self.max_schedule_horizon_ms),
            max_custom_entries: q.max_custom_entries.unwrap_or(self.max_custom_entries),
            max_custom_total_bytes: q
                .max_custom_total_bytes
                .unwrap_or(self.max_custom_total_bytes),
            max_custom_key_bytes: q.max_custom_key_bytes.unwrap_or(self.max_custom_key_bytes),
            dedup_window_ms: q.dedup_window_ms.unwrap_or(self.dedup_window_ms),
        }
    }

    pub fn validate(&self, max_message_bytes: u64, scope: &str) -> Result<(), Box<dyn Error>> {
        let bad =
            |field: &str, msg: &str| -> Box<dyn Error> { format!("{scope}.{field}: {msg}").into() };
        if self.max_lease_duration_ms == 0 {
            return Err(bad("max_lease_duration_ms", "must be > 0"));
        }
        if self.default_max_attempts == 0 {
            return Err(bad("default_max_attempts", "must be > 0"));
        }
        if self.max_attempts_ceiling == 0 {
            return Err(bad("max_attempts_ceiling", "must be > 0"));
        }
        if self.default_max_attempts > self.max_attempts_ceiling {
            return Err(bad(
                "default_max_attempts",
                "must not exceed max_attempts_ceiling",
            ));
        }
        if self.default_priority > 9 {
            return Err(bad("default_priority", "must be in [0, 9]"));
        }
        if self.max_payload_bytes == 0 {
            return Err(bad("max_payload_bytes", "must be > 0"));
        }
        if self.max_payload_bytes > max_message_bytes {
            return Err(bad(
                "max_payload_bytes",
                "must not exceed limits.max_message_bytes",
            ));
        }
        if let Some(encodings) = &self.allowed_encodings
            && encodings.iter().any(|e| e.is_empty())
        {
            return Err(bad("allowed_encodings", "entries must not be empty"));
        }
        if self.max_schedule_horizon_ms == 0 {
            return Err(bad("max_schedule_horizon_ms", "must be > 0"));
        }
        if self.max_custom_entries == 0 {
            return Err(bad("max_custom_entries", "must be > 0"));
        }
        if self.max_custom_total_bytes == 0 {
            return Err(bad("max_custom_total_bytes", "must be > 0"));
        }
        if self.max_custom_key_bytes == 0 {
            return Err(bad("max_custom_key_bytes", "must be > 0"));
        }
        if self.dedup_window_ms <= 0 {
            return Err(bad("dedup_window_ms", "must be > 0"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    pub api_keys: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LimitsConfig {
    pub max_lease_duration_ms: u64,
    pub default_max_attempts: u32,
    pub max_attempts_ceiling: u32,
    pub default_priority: u32,
    pub max_reserve_batch: u32,
    pub max_reserve_queues: u32,
    pub max_wait_timeout_ms: u64,
    pub max_enqueue_batch: u32,
    pub max_payload_bytes: u64,
    pub max_message_bytes: u64,
    pub max_custom_entries: u32,
    pub max_custom_total_bytes: u64,
    pub max_custom_key_bytes: u32,
    pub max_queue_name_bytes: u32,
    pub max_job_type_bytes: u32,
    pub max_idempotency_key_bytes: u32,
    pub max_schedule_horizon_ms: u64,
    // None = unrestricted, Some(vec![]) = reject all, Some(vec!["a", "b"]) = accept only a and b
    pub allowed_encodings: Option<Vec<String>>,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_lease_duration_ms: 5 * 60 * 1000,
            default_max_attempts: 3,
            max_attempts_ceiling: 100,
            default_priority: 0,
            max_reserve_batch: 256,
            max_reserve_queues: 32,
            max_wait_timeout_ms: 5 * 60 * 1000,
            max_enqueue_batch: 256,
            max_payload_bytes: 1 << 20,
            max_message_bytes: 16 << 20,
            max_custom_entries: 64,
            max_custom_total_bytes: 16 << 10,
            max_custom_key_bytes: 256,
            max_queue_name_bytes: 512,
            max_job_type_bytes: 256,
            max_idempotency_key_bytes: 256,
            max_schedule_horizon_ms: 30 * 24 * 60 * 60 * 1000,
            allowed_encodings: None,
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
    pub max_cached_files: Option<usize>,
    pub worker_threads: Option<usize>,
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
            max_cached_files: None,
            worker_threads: None,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub otlp_endpoint: String,
    pub export_interval_ms: u64,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: "http://localhost:4317".to_string(),
            export_interval_ms: 60_000,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub limits: LimitsConfig,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
    pub tracing: TracingConfig,
    pub metrics: MetricsConfig,
    pub queues: Vec<QueueConfig>,
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
        match (&self.server.tls_cert_path, &self.server.tls_key_path) {
            (Some(_), None) => {
                return Err(
                    "server.tls_cert_path is set but server.tls_key_path is not; set both or neither".into(),
                );
            }
            (None, Some(_)) => {
                return Err(
                    "server.tls_key_path is set but server.tls_cert_path is not; set both or neither".into(),
                );
            }
            (Some(cert), Some(key)) => {
                if cert.is_empty() || key.is_empty() {
                    return Err(
                        "server.tls_cert_path and server.tls_key_path must not be empty".into(),
                    );
                }
            }
            (None, None) => {}
        }
        if let Some(keys) = &self.auth.api_keys
            && keys.iter().any(|k| k.is_empty())
        {
            return Err("auth.api_keys entries must not be empty".into());
        }
        // Request-shape limits — global only (not overridable per queue).
        if self.limits.max_reserve_batch == 0 {
            return Err("limits.max_reserve_batch must be > 0".into());
        }
        if self.limits.max_reserve_queues == 0 {
            return Err("limits.max_reserve_queues must be > 0".into());
        }
        if self.limits.max_wait_timeout_ms == 0 {
            return Err("limits.max_wait_timeout_ms must be > 0".into());
        }
        if self.limits.max_enqueue_batch == 0 {
            return Err("limits.max_enqueue_batch must be > 0".into());
        }
        if self.limits.max_message_bytes == 0 {
            return Err("limits.max_message_bytes must be > 0".into());
        }
        if self.limits.max_queue_name_bytes == 0
            || self.limits.max_queue_name_bytes > u16::MAX as u32
        {
            return Err("limits.max_queue_name_bytes must be in [1, 65535]".into());
        }
        if self.limits.max_job_type_bytes == 0 {
            return Err("limits.max_job_type_bytes must be > 0".into());
        }
        if self.limits.max_idempotency_key_bytes == 0 {
            return Err("limits.max_idempotency_key_bytes must be > 0".into());
        }
        // Overridable per-job/per-queue limits — the same field rules apply
        // to the global defaults and to each queue's merged effective view.
        let defaults = EffectiveLimits::from_globals(&self.limits, &self.storage);
        defaults.validate(self.limits.max_message_bytes, "limits")?;
        if self.storage.sweep_interval_ms == 0 {
            return Err("storage.sweep_interval_ms must be > 0".into());
        }
        if self.storage.sweep_limit == 0 {
            return Err("storage.sweep_limit must be > 0".into());
        }
        if self.storage.command_queue_capacity == 0 {
            return Err("storage.command_queue_capacity must be > 0".into());
        }
        if matches!(self.storage.cache_size_bytes, Some(0)) {
            return Err("storage.cache_size_bytes must be > 0 when set".into());
        }
        if let Some(bytes) = self.storage.max_journaling_size_bytes
            && bytes < 64 * 1024 * 1024
        {
            return Err("storage.max_journaling_size_bytes must be >= 64 MiB when set".into());
        }
        if let Some(n) = self.storage.max_cached_files
            && n < 10
        {
            return Err("storage.max_cached_files must be >= 10 when set".into());
        }
        if matches!(self.storage.worker_threads, Some(0)) {
            return Err("storage.worker_threads must be > 0 when set".into());
        }
        if !(0.0..=1.0).contains(&self.tracing.sample_ratio) {
            return Err("tracing.sample_ratio must be in [0.0, 1.0]".into());
        }
        if self.tracing.enabled && self.tracing.service_name.is_empty() {
            return Err("tracing.service_name must not be empty".into());
        }
        if self.metrics.enabled {
            if self.metrics.otlp_endpoint.is_empty() {
                return Err("metrics.otlp_endpoint must not be empty when enabled".into());
            }
            if self.metrics.export_interval_ms == 0 {
                return Err("metrics.export_interval_ms must be > 0".into());
            }
        }
        self.validate_queues(&defaults)?;
        Ok(())
    }

    fn validate_queues(&self, defaults: &EffectiveLimits) -> Result<(), Box<dyn Error>> {
        let mut seen: HashSet<&str> = HashSet::new();
        for q in &self.queues {
            if q.name.is_empty() {
                return Err("queues[].name must not be empty".into());
            }
            if q.name.len() > self.limits.max_queue_name_bytes as usize {
                return Err(format!(
                    "queues[].name {:?} exceeds limits.max_queue_name_bytes ({})",
                    q.name, self.limits.max_queue_name_bytes
                )
                .into());
            }
            if !seen.insert(q.name.as_str()) {
                return Err(format!("queues[] contains duplicate name {:?}", q.name).into());
            }
            defaults.merged_with(q).validate(
                self.limits.max_message_bytes,
                &format!("queues[{:?}]", q.name),
            )?;
        }
        Ok(())
    }
}
