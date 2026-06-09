use std::sync::Arc;
use std::{collections::HashSet, error::Error, net::SocketAddr, path::Path};

use arc_swap::ArcSwap;
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use garde::Validate;
use serde::{Deserialize, Serialize};

pub const DEFAULT_CONFIG_PATH: &str = "./sepp.toml";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistMode {
    SyncAll,
    SyncData,
    Buffer,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate)]
#[serde(default)]
#[garde(allow_unvalidated)]
pub struct ServerConfig {
    pub listen_addr: SocketAddr,
    #[garde(length(min = 1))]
    pub db_path: String,
    #[garde(inner(length(min = 1)))]
    pub tls_cert_path: Option<String>,
    #[garde(inner(length(min = 1)))]
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
            listen_addr: SocketAddr::from(([0, 0, 0, 0], 50051)),
            db_path: "./sepp-data".to_string(),
            tls_cert_path: None,
            tls_key_path: None,
            strict_queues: false,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct QueueConfig {
    pub name: String,
    pub max_lease_duration_ms: Option<u64>,
    pub default_max_attempts: Option<u32>,
    pub max_attempts_ceiling: Option<u32>,
    pub default_priority: Option<u32>,
    pub max_payload_bytes: Option<u64>,
    pub allowed_encodings: Option<Vec<String>>,
    pub allowed_job_types: Option<Vec<String>>,
    pub max_schedule_horizon_ms: Option<u64>,
    pub max_custom_entries: Option<u32>,
    pub max_custom_total_bytes: Option<u64>,
    pub max_custom_key_bytes: Option<u32>,
    pub dedup_window_ms: Option<i64>,
    pub max_queue_depth: Option<u64>,
}

#[derive(Debug, Clone, Validate)]
#[garde(context(u64 as max_message_bytes))]
pub struct EffectiveLimits {
    #[garde(range(min = 1))]
    pub max_lease_duration_ms: u64,
    #[garde(range(min = 1))]
    pub default_max_attempts: u32,
    #[garde(range(min = 1))]
    pub max_attempts_ceiling: u32,
    #[garde(range(max = 9))]
    pub default_priority: u32,
    #[garde(range(min = 1), custom(payload_within_message_limit))]
    pub max_payload_bytes: u64,
    #[garde(inner(inner(length(min = 1))))]
    pub allowed_encodings: Option<Vec<String>>,
    #[garde(inner(length(min = 1), inner(length(min = 1))))]
    pub allowed_job_types: Option<Vec<String>>,
    #[garde(range(min = 1))]
    pub max_schedule_horizon_ms: u64,
    #[garde(range(min = 1))]
    pub max_custom_entries: u32,
    #[garde(range(min = 1))]
    pub max_custom_total_bytes: u64,
    #[garde(range(min = 1))]
    pub max_custom_key_bytes: u32,
    #[garde(range(min = 1))]
    pub dedup_window_ms: i64,
    #[garde(skip)]
    pub max_queue_depth: Option<u64>,
}

fn payload_within_message_limit(value: &u64, max: &u64) -> garde::Result {
    if *value > *max {
        return Err(garde::Error::new(
            "must not exceed limits.max_message_bytes",
        ));
    }

    Ok(())
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
            allowed_job_types: None,
            max_schedule_horizon_ms: limits.max_schedule_horizon_ms,
            max_custom_entries: limits.max_custom_entries,
            max_custom_total_bytes: limits.max_custom_total_bytes,
            max_custom_key_bytes: limits.max_custom_key_bytes,
            dedup_window_ms: storage.dedup_window_ms,
            max_queue_depth: limits.max_queue_depth,
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
            allowed_job_types: q.allowed_job_types.clone(),
            max_schedule_horizon_ms: q
                .max_schedule_horizon_ms
                .unwrap_or(self.max_schedule_horizon_ms),
            max_custom_entries: q.max_custom_entries.unwrap_or(self.max_custom_entries),
            max_custom_total_bytes: q
                .max_custom_total_bytes
                .unwrap_or(self.max_custom_total_bytes),
            max_custom_key_bytes: q.max_custom_key_bytes.unwrap_or(self.max_custom_key_bytes),
            dedup_window_ms: q.dedup_window_ms.unwrap_or(self.dedup_window_ms),
            max_queue_depth: q.max_queue_depth.or(self.max_queue_depth),
        }
    }

    pub fn validate(&self, max_message_bytes: u64, scope: &str) -> Result<(), Box<dyn Error>> {
        self.validate_with(&max_message_bytes)
            .map_err(|e| format!("{scope}: {e}"))?;

        if self.default_max_attempts > self.max_attempts_ceiling {
            return Err(format!(
                "{scope}.default_max_attempts: must not exceed max_attempts_ceiling"
            )
            .into());
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Validate)]
#[serde(default)]
pub struct AuthConfig {
    #[garde(inner(inner(length(min = 1))))]
    pub api_keys: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate)]
#[serde(default)]
#[garde(allow_unvalidated)]
pub struct LimitsConfig {
    pub max_lease_duration_ms: u64,
    pub default_max_attempts: u32,
    pub max_attempts_ceiling: u32,
    pub default_priority: u32,
    #[garde(range(min = 1))]
    pub max_reserve_batch: u32,
    #[garde(range(min = 1))]
    pub max_reserve_queues: u32,
    #[garde(range(min = 1))]
    pub max_wait_timeout_ms: u64,
    #[garde(range(min = 1))]
    pub max_enqueue_batch: u32,
    pub max_queue_depth: Option<u64>,
    pub max_payload_bytes: u64,
    #[garde(range(min = 1))]
    pub max_message_bytes: u64,
    pub max_custom_entries: u32,
    pub max_custom_total_bytes: u64,
    pub max_custom_key_bytes: u32,
    #[garde(range(min = 1, max = 65535))]
    pub max_queue_name_bytes: u32,
    #[garde(range(min = 1))]
    pub max_job_type_bytes: u32,
    #[garde(range(min = 1))]
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
            max_queue_depth: None,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate)]
#[serde(default)]
#[garde(allow_unvalidated)]
pub struct StorageConfig {
    pub persist_mode: PersistMode,
    #[garde(range(min = 1))]
    pub sweep_interval_ms: u64,
    #[garde(range(min = 1))]
    pub sweep_limit: usize,
    pub dedup_window_ms: i64,
    pub dead_letter_retention_ms: u64,
    #[garde(range(min = 1))]
    pub command_queue_capacity: usize,
    #[garde(inner(range(min = 1)))]
    pub cache_size_bytes: Option<u64>,
    #[garde(inner(range(min = 64 * 1024 * 1024)))]
    pub max_journaling_size_bytes: Option<u64>,
    #[garde(inner(range(min = 10)))]
    pub max_cached_files: Option<usize>,
    #[garde(inner(range(min = 1)))]
    pub worker_threads: Option<usize>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            persist_mode: PersistMode::SyncAll,
            sweep_interval_ms: 1000,
            sweep_limit: 10_000,
            dedup_window_ms: 24 * 60 * 60 * 1000,
            dead_letter_retention_ms: 0,
            command_queue_capacity: 4096,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
    pub format: LogFormat,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info,lsm_tree=warn,fjall=warn".to_string(),
            format: LogFormat::Text,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate)]
#[serde(default)]
#[garde(allow_unvalidated)]
pub struct TracingConfig {
    pub enabled: bool,
    pub otlp_endpoint: String,
    pub service_name: String,
    #[garde(range(min = 0.0, max = 1.0))]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate)]
#[serde(default)]
#[garde(allow_unvalidated)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub otlp_endpoint: String,
    pub export_interval_ms: u64,
    pub prometheus_enabled: bool,
    pub prometheus_listen_addr: SocketAddr,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: "http://localhost:4317".to_string(),
            export_interval_ms: 5_000,
            prometheus_enabled: false,
            prometheus_listen_addr: SocketAddr::from(([0, 0, 0, 0], 9464)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AdminConfig {
    pub enabled: bool,
    pub listen_addr: SocketAddr,
    // None = auth disabled; only allowed on a loopback listen_addr.
    pub keys: Option<Vec<AdminKey>>,
    pub session_ttl_ms: u64,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 9465)),
            keys: None,
            session_ttl_ms: 12 * 60 * 60 * 1000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminKey {
    pub name: String,
    pub key: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub limits: LimitsConfig,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
    pub tracing: TracingConfig,
    pub metrics: MetricsConfig,
    pub admin: AdminConfig,
    pub queues: Vec<QueueConfig>,
}

pub type SharedConfig = Arc<ArcSwap<Config>>;

impl Config {
    // Wraps this configuration in a shared, atomically-swappable handle.
    pub fn into_shared(self) -> SharedConfig {
        Arc::new(ArcSwap::from_pointee(self))
    }

    pub fn load(explicit_path: Option<&str>) -> Result<Self, Box<dyn Error>> {
        let path = explicit_path.unwrap_or(DEFAULT_CONFIG_PATH);
        if explicit_path.is_some() && !Path::new(path).exists() {
            return Err(format!("config file not found: {path}").into());
        }

        Self::extract(Toml::file(path))
    }

    pub fn from_toml_str(toml: &str) -> Result<Self, Box<dyn Error>> {
        Self::extract(Toml::string(toml))
    }

    fn extract(toml: impl figment::Provider) -> Result<Self, Box<dyn Error>> {
        let config: Config = Figment::new()
            .merge(Serialized::defaults(Config::default()))
            .merge(toml)
            .merge(Env::prefixed("SEPP_").split("__"))
            .extract()?;
        config.validate()?;

        Ok(config)
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        self.server.validate().map_err(|e| format!("server: {e}"))?;
        self.auth.validate().map_err(|e| format!("auth: {e}"))?;
        self.limits.validate().map_err(|e| format!("limits: {e}"))?;
        self.storage
            .validate()
            .map_err(|e| format!("storage: {e}"))?;
        self.tracing
            .validate()
            .map_err(|e| format!("tracing: {e}"))?;
        self.metrics
            .validate()
            .map_err(|e| format!("metrics: {e}"))?;

        // Cross-field rules that garde can't express declaratively.
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
            _ => {}
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

        if self.admin.enabled
            && !self.admin.listen_addr.ip().is_loopback()
            && self.admin.keys.is_none()
        {
            return Err("admin UI on a non-loopback address requires [admin] keys".into());
        }

        let defaults = EffectiveLimits::from_globals(&self.limits, &self.storage);
        defaults.validate(self.limits.max_message_bytes, "limits")?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        assert!(Config::default().validate().is_ok());
    }

    #[test]
    fn example_config_matches_defaults() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/sepp.example.toml");
        let from_example: Config = Figment::new()
            .merge(Serialized::defaults(Config::default()))
            .merge(Toml::file(path))
            .extract()
            .expect("sepp.example.toml should parse");
        assert_eq!(
            from_example,
            Config::default(),
            "sepp.example.toml drifted from Config::default(); \
             update src/config.rs or sepp.example.toml so they agree"
        );
    }

    #[test]
    fn tls_requires_both_cert_and_key() {
        let mut cert_only = Config::default();
        cert_only.server.tls_cert_path = Some("cert.pem".into());
        assert!(
            cert_only.validate().is_err(),
            "a cert without a key must be rejected"
        );

        let mut key_only = Config::default();
        key_only.server.tls_key_path = Some("key.pem".into());
        assert!(
            key_only.validate().is_err(),
            "a key without a cert must be rejected"
        );

        let mut both = Config::default();
        both.server.tls_cert_path = Some("cert.pem".into());
        both.server.tls_key_path = Some("key.pem".into());
        assert!(both.validate().is_ok(), "both set is the valid case");
    }

    #[test]
    fn default_max_attempts_must_not_exceed_ceiling() {
        let mut cfg = Config::default();
        cfg.limits.default_max_attempts = 5;
        cfg.limits.max_attempts_ceiling = 3;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn per_queue_max_attempts_must_not_exceed_ceiling() {
        let cfg = Config {
            queues: vec![QueueConfig {
                name: "q".into(),
                default_max_attempts: Some(50),
                max_attempts_ceiling: Some(10),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn payload_limit_must_not_exceed_message_limit() {
        let mut cfg = Config::default();
        cfg.limits.max_payload_bytes = cfg.limits.max_message_bytes + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn per_queue_payload_limit_must_not_exceed_message_limit() {
        let over = LimitsConfig::default().max_message_bytes + 1;
        let cfg = Config {
            queues: vec![QueueConfig {
                name: "big".into(),
                max_payload_bytes: Some(over),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn duplicate_queue_names_are_rejected() {
        let cfg = Config {
            queues: vec![
                QueueConfig {
                    name: "dup".into(),
                    ..Default::default()
                },
                QueueConfig {
                    name: "dup".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn empty_queue_name_is_rejected() {
        let cfg = Config {
            queues: vec![QueueConfig {
                name: String::new(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn queue_name_over_the_byte_limit_is_rejected() {
        let cfg = Config {
            limits: LimitsConfig {
                max_queue_name_bytes: 4,
                ..Default::default()
            },
            queues: vec![QueueConfig {
                name: "toolong".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn metrics_when_enabled_need_endpoint_and_interval() {
        let mut no_endpoint = Config::default();
        no_endpoint.metrics.enabled = true;
        no_endpoint.metrics.otlp_endpoint = String::new();
        assert!(no_endpoint.validate().is_err());

        let mut zero_interval = Config::default();
        zero_interval.metrics.enabled = true;
        zero_interval.metrics.export_interval_ms = 0;
        assert!(zero_interval.validate().is_err());
    }

    #[test]
    fn tracing_when_enabled_needs_a_service_name() {
        let mut cfg = Config::default();
        cfg.tracing.enabled = true;
        cfg.tracing.service_name = String::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn a_zero_valued_field_limit_fails_validation() {
        let mut cfg = Config::default();
        cfg.limits.max_reserve_batch = 0;
        assert!(
            cfg.validate().is_err(),
            "garde range(min = 1) rejects a zero batch cap"
        );
    }

    #[test]
    fn admin_on_non_loopback_requires_keys() {
        let mut cfg = Config::default();
        cfg.admin.enabled = true;
        cfg.admin.listen_addr = SocketAddr::from(([0, 0, 0, 0], 9465));
        assert!(
            cfg.validate().is_err(),
            "a non-loopback admin bind without keys must be rejected"
        );

        cfg.admin.keys = Some(vec![AdminKey {
            name: "ops".into(),
            key: "secret".into(),
        }]);
        assert!(cfg.validate().is_ok());

        let mut loopback = Config::default();
        loopback.admin.enabled = true;
        assert!(loopback.validate().is_ok(), "loopback admin needs no keys");
    }

    #[test]
    fn from_toml_str_validates_like_load() {
        assert!(Config::from_toml_str("").is_ok());
        assert!(
            Config::from_toml_str("[admin]\nenabled = true\nlisten_addr = \"0.0.0.0:9465\"\n")
                .is_err()
        );
    }

    #[test]
    fn a_well_formed_queue_override_is_accepted() {
        let cfg = Config {
            queues: vec![QueueConfig {
                name: "emails".into(),
                max_lease_duration_ms: Some(60_000),
                default_max_attempts: Some(5),
                default_priority: Some(5),
                allowed_job_types: Some(vec!["send_welcome".into()]),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }
}
