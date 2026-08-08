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

// Server-side delay applied to a nack that carries no explicit retry
// directive (NackRetry.default or no retry field at all).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryBackoff {
    #[default]
    None,
    Exponential,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate)]
#[serde(default)]
#[garde(allow_unvalidated)]
pub struct ClusterConfig {
    pub enabled: bool,
    #[garde(range(min = 1))]
    pub node_id: u64,
    pub bootstrap_single: bool,
    pub peer_listen_addr: SocketAddr,
    #[garde(inner(custom(host_port)))]
    pub peer_advertise_addr: Option<String>,
    #[garde(inner(custom(host_port)))]
    pub client_advertise_addr: Option<String>,
    #[garde(inner(length(min = 1)))]
    pub peer_tls_cert_path: Option<String>,
    #[garde(inner(length(min = 1)))]
    pub peer_tls_key_path: Option<String>,
    #[garde(inner(length(min = 1)))]
    pub peer_tls_ca_path: Option<String>,
    #[garde(inner(length(min = 1), inner(length(min = 1))))]
    pub peer_auth_keys: Option<Vec<String>>, // First element is sender key, for rolling rotation.
    #[garde(range(min = 1))]
    pub heartbeat_interval_ms: u64,
    #[garde(range(min = 1))]
    pub election_timeout_min_ms: u64,
    #[garde(range(min = 1))]
    pub election_timeout_max_ms: u64,
}

fn host_port(value: &str, _: &()) -> garde::Result {
    // IP literals, incl. bracketed IPv6 like [::1]:50052.
    if let Ok(addr) = value.parse::<SocketAddr>() {
        return if addr.port() != 0 {
            Ok(())
        } else {
            Err(garde::Error::new("port must be 1-65535"))
        };
    }

    let Some((host, port)) = value.rsplit_once(':') else {
        return Err(garde::Error::new("must be host:port"));
    };

    if host.is_empty() || host.contains(':') {
        return Err(garde::Error::new(
            "must be host:port; bracket IPv6 addresses like [::1]:50052",
        ));
    }

    match port.parse::<u16>() {
        Ok(p) if p != 0 => Ok(()),
        _ => Err(garde::Error::new("port must be 1-65535")),
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: 1,
            bootstrap_single: false,
            peer_listen_addr: SocketAddr::from(([0, 0, 0, 0], 50052)),
            peer_advertise_addr: None,
            client_advertise_addr: None,
            peer_tls_cert_path: None,
            peer_tls_key_path: None,
            peer_tls_ca_path: None,
            peer_auth_keys: None,
            heartbeat_interval_ms: 250,
            election_timeout_min_ms: 1500,
            election_timeout_max_ms: 3000,
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
    pub retry_delay_ms: Option<u64>,
    pub retry_backoff: Option<RetryBackoff>,
    pub retry_delay_max_ms: Option<u64>,
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
    #[garde(skip)]
    pub retry_delay_ms: u64,
    #[garde(skip)]
    pub retry_backoff: RetryBackoff,
    #[garde(range(min = 1))]
    pub retry_delay_max_ms: u64,
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
            retry_delay_ms: limits.retry_delay_ms,
            retry_backoff: limits.retry_backoff,
            retry_delay_max_ms: limits.retry_delay_max_ms,
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
            retry_delay_ms: q.retry_delay_ms.unwrap_or(self.retry_delay_ms),
            retry_backoff: q.retry_backoff.unwrap_or(self.retry_backoff),
            retry_delay_max_ms: q.retry_delay_max_ms.unwrap_or(self.retry_delay_max_ms),
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

        if self.retry_delay_ms > self.retry_delay_max_ms {
            return Err(
                format!("{scope}.retry_delay_ms: must not exceed retry_delay_max_ms").into(),
            );
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Validate)]
#[serde(default)]
pub struct AuthConfig {
    // None = auth disabled; Some(vec![]) = deny all. Validated in
    // Config::validate alongside admin.keys.
    #[garde(skip)]
    pub api_keys: Option<Vec<ApiKeyEntry>>,
}

// A named gRPC API key. The name labels the client (worker pool, producer)
// in the admin UI and audit log; only the key is the secret.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiKeyEntry {
    pub name: String,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate)]
#[serde(default)]
#[garde(allow_unvalidated)]
pub struct LimitsConfig {
    pub max_lease_duration_ms: u64,
    pub default_max_attempts: u32,
    pub max_attempts_ceiling: u32,
    pub default_priority: u32,
    pub retry_delay_ms: u64,
    pub retry_backoff: RetryBackoff,
    pub retry_delay_max_ms: u64,
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
            retry_delay_ms: 0,
            retry_backoff: RetryBackoff::None,
            retry_delay_max_ms: 60 * 60 * 1000,
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
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
    // None = auth disabled; only allowed on a loopback listen_addr.
    pub keys: Option<Vec<AdminKey>>,
    pub session_ttl_ms: u64,
    pub stats_history_ms: u64,
}

impl AdminConfig {
    pub fn tls_enabled(&self) -> bool {
        self.tls_cert_path.is_some() && self.tls_key_path.is_some()
    }
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 9465)),
            tls_cert_path: None,
            tls_key_path: None,
            keys: None,
            session_ttl_ms: 12 * 60 * 60 * 1000,
            stats_history_ms: 60 * 60 * 1000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminKey {
    pub name: String,
    pub key: String,
    #[serde(default)]
    pub role: Role,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Viewer,
    Operator,
    #[default]
    Admin,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Role::Viewer => "viewer",
            Role::Operator => "operator",
            Role::Admin => "admin",
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub cluster: ClusterConfig,
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
        self.cluster
            .validate()
            .map_err(|e| format!("cluster: {e}"))?;
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
        validate_tls_config(
            &self.server.tls_cert_path,
            &self.server.tls_key_path,
            "server.tls",
        )?;
        validate_tls_config(
            &self.cluster.peer_tls_cert_path,
            &self.cluster.peer_tls_key_path,
            "cluster.peer_tls",
        )?;
        validate_tls_config(
            &self.admin.tls_cert_path,
            &self.admin.tls_key_path,
            "admin.tls",
        )?;

        if self.cluster.peer_tls_ca_path.is_some() && self.cluster.peer_tls_cert_path.is_none() {
            return Err(
                "cluster.peer_tls_ca_path is set but peer TLS is not enabled; set \
                 cluster.peer_tls_cert_path and peer_tls_key_path"
                    .into(),
            );
        }

        if self.cluster.enabled {
            if self.storage.persist_mode == PersistMode::Buffer {
                return Err(
                    "storage.persist_mode = \"buffer\" is invalid in cluster mode; use sync_data or sync_all".into(),
                );
            }

            if self.cluster.client_advertise_addr.is_none() {
                return Err(
                    "cluster.enabled requires cluster.client_advertise_addr to be set".into(),
                );
            }

            if self.cluster.peer_listen_addr.ip().is_unspecified()
                && self.cluster.peer_advertise_addr.is_none()
            {
                return Err(
                    "cluster.peer_listen_addr is a wildcard address; set cluster.peer_advertise_addr".into(),
                );
            }

            if self.cluster.election_timeout_min_ms >= self.cluster.election_timeout_max_ms {
                return Err(
                    "cluster.election_timeout_min_ms must be less than election_timeout_max_ms"
                        .into(),
                );
            }

            if self.cluster.heartbeat_interval_ms >= self.cluster.election_timeout_min_ms {
                return Err(
                    "cluster.heartbeat_interval_ms must be less than election_timeout_min_ms"
                        .into(),
                );
            }

            if let Some(keys) = &self.cluster.peer_auth_keys {
                let mut seen_keys: HashSet<&str> = HashSet::new();
                for (i, k) in keys.iter().enumerate() {
                    if !k.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
                        return Err(
                            format!("cluster.peer_auth_keys[{i}] must be visible ASCII").into()
                        );
                    }

                    if !seen_keys.insert(k.as_str()) {
                        return Err(format!(
                            "cluster.peer_auth_keys[{i}] duplicates an earlier key"
                        )
                        .into());
                    }
                }
            }
        } else {
            if self.cluster.bootstrap_single {
                return Err("cluster.bootstrap_single requires cluster.enabled to be true".into());
            }
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
            return Err(
                "admin UI on a non-loopback address requires [admin] keys (set \
                 admin.tls_cert_path/tls_key_path for HTTPS, or terminate TLS in front of it)"
                    .into(),
            );
        }

        if let Some(keys) = &self.admin.keys {
            let mut names: HashSet<&str> = HashSet::new();
            let mut secrets: HashSet<&str> = HashSet::new();
            for k in keys {
                if k.name.is_empty() {
                    return Err("admin.keys[].name must not be empty".into());
                }
                if k.key.is_empty() {
                    return Err(format!("admin key {:?} has an empty key", k.name).into());
                }
                // Sessions are matched back to keys by name; duplicates would
                // make rotation and audit attribution ambiguous.
                if !names.insert(&k.name) {
                    return Err(format!("admin.keys has a duplicate name {:?}", k.name).into());
                }
                // A shared secret would resolve to whichever entry lists it
                // last, making the granted role depend on file order.
                if !secrets.insert(&k.key) {
                    return Err(
                        format!("admin keys {:?} and another share the same key", k.name).into(),
                    );
                }
            }
        }

        if let Some(keys) = &self.auth.api_keys {
            let mut names: HashSet<&str> = HashSet::new();
            let mut secrets: HashSet<&str> = HashSet::new();
            for k in keys {
                if k.name.is_empty() {
                    return Err("auth.api_keys[].name must not be empty".into());
                }
                if k.key.is_empty() {
                    return Err(format!("API key {:?} has an empty key", k.name).into());
                }
                if !k.key.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
                    return Err(format!("API key {:?} must be visible ASCII", k.name).into());
                }
                if !names.insert(&k.name) {
                    return Err(format!("auth.api_keys has a duplicate name {:?}", k.name).into());
                }
                if !secrets.insert(&k.key) {
                    return Err(
                        format!("API keys {:?} and another share the same key", k.name).into(),
                    );
                }
            }
        }

        // One year keeps expiry arithmetic far from overflow; one minute keeps
        // a typo'd TTL from instantly expiring every session.
        if !(60_000..=31_536_000_000).contains(&self.admin.session_ttl_ms) {
            return Err(
                "admin.session_ttl_ms must be between 60000 (1 minute) and 31536000000 (365 days)"
                    .into(),
            );
        }

        // The history ring holds one sample per second per queue and ships
        // whole to every dashboard load; the ceiling keeps both memory and the
        // SSE hello payload sane.
        if !(60_000..=21_600_000).contains(&self.admin.stats_history_ms) {
            return Err(
                "admin.stats_history_ms must be between 60000 (1 minute) and 21600000 (6 hours)"
                    .into(),
            );
        }

        let defaults = EffectiveLimits::from_globals(&self.limits, &self.storage);
        defaults.validate(self.limits.max_message_bytes, "limits")?;
        self.validate_queues(&defaults)?;
        Ok(())
    }

    fn validate_queues(&self, defaults: &EffectiveLimits) -> Result<(), Box<dyn Error>> {
        let mut seen: HashSet<&str> = HashSet::new();
        for q in &self.queues {
            // Same validity rule the gRPC request path enforces (empty, "."/"..",
            // '/', control chars), so declared and auto-created names agree and
            // both stay addressable through the admin REST API.
            if let Some(why) = crate::validate::queue_name_error(&q.name) {
                return Err(format!("queues[].name {:?} {why}", q.name).into());
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

fn validate_tls_config(
    cert_path: &Option<String>,
    key_path: &Option<String>,
    prefix: &str,
) -> Result<(), Box<dyn Error>> {
    match (cert_path, key_path) {
        (Some(_), None) => {
            return Err(format!(
                "{prefix}_cert_path is set but {prefix}_key_path is not; set both or neither"
            )
            .into());
        }
        (None, Some(_)) => {
            return Err(format!(
                "{prefix}_key_path is set but {prefix}_cert_path is not; set both or neither"
            )
            .into());
        }
        _ => {}
    }

    Ok(())
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
    fn admin_tls_requires_both_cert_and_key() {
        let mut cert_only = Config::default();
        cert_only.admin.tls_cert_path = Some("cert.pem".into());
        assert!(
            cert_only.validate().is_err(),
            "a cert without a key must be rejected"
        );

        let mut key_only = Config::default();
        key_only.admin.tls_key_path = Some("key.pem".into());
        assert!(
            key_only.validate().is_err(),
            "a key without a cert must be rejected"
        );

        let mut both = Config::default();
        both.admin.tls_cert_path = Some("cert.pem".into());
        both.admin.tls_key_path = Some("key.pem".into());
        assert!(both.validate().is_ok(), "both set is the valid case");
        assert!(both.admin.tls_enabled());
        assert!(!Config::default().admin.tls_enabled());
    }

    #[test]
    fn default_max_attempts_must_not_exceed_ceiling() {
        let mut cfg = Config::default();
        cfg.limits.default_max_attempts = 5;
        cfg.limits.max_attempts_ceiling = 3;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn retry_delay_must_not_exceed_its_max() {
        let mut cfg = Config::default();
        cfg.limits.retry_delay_ms = 10_000;
        cfg.limits.retry_delay_max_ms = 5_000;
        assert!(cfg.validate().is_err());

        let cfg = Config {
            queues: vec![QueueConfig {
                name: "q".into(),
                retry_delay_ms: Some(10_000),
                retry_delay_max_ms: Some(5_000),
                ..Default::default()
            }],
            ..Default::default()
        };
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
    fn dot_queue_names_are_rejected() {
        for name in [".", ".."] {
            let cfg = Config {
                queues: vec![QueueConfig {
                    name: name.into(),
                    ..Default::default()
                }],
                ..Default::default()
            };
            assert!(cfg.validate().is_err(), "{name:?} must be rejected");
        }
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
            role: Role::Admin,
        }]);
        assert!(cfg.validate().is_ok());

        let mut loopback = Config::default();
        loopback.admin.enabled = true;
        assert!(loopback.validate().is_ok(), "loopback admin needs no keys");

        let mut disabled = Config::default();
        disabled.admin.enabled = false;
        disabled.admin.listen_addr = SocketAddr::from(([0, 0, 0, 0], 9465));
        assert!(
            disabled.validate().is_ok(),
            "the bind address is irrelevant while the admin UI is disabled"
        );
    }

    #[test]
    fn admin_keys_must_be_well_formed() {
        let key = |name: &str, key: &str| AdminKey {
            name: name.into(),
            key: key.into(),
            role: Role::Viewer,
        };

        let mut cfg = Config::default();
        cfg.admin.keys = Some(vec![key("ops", "a"), key("ops", "b")]);
        assert!(cfg.validate().is_err(), "duplicate names are rejected");

        cfg.admin.keys = Some(vec![key("", "a")]);
        assert!(cfg.validate().is_err(), "empty names are rejected");

        cfg.admin.keys = Some(vec![key("ops", "")]);
        assert!(cfg.validate().is_err(), "empty keys are rejected");

        cfg.admin.keys = Some(vec![key("ops", "same"), key("dev", "same")]);
        assert!(
            cfg.validate().is_err(),
            "a shared secret across entries is rejected"
        );

        cfg.admin.keys = Some(vec![key("ops", "a"), key("dev", "b")]);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn api_keys_must_be_well_formed() {
        let key = |name: &str, key: &str| ApiKeyEntry {
            name: name.into(),
            key: key.into(),
        };

        let mut cfg = Config::default();
        cfg.auth.api_keys = Some(vec![key("pool", "a"), key("pool", "b")]);
        assert!(cfg.validate().is_err(), "duplicate names are rejected");

        cfg.auth.api_keys = Some(vec![key("", "a")]);
        assert!(cfg.validate().is_err(), "empty names are rejected");

        cfg.auth.api_keys = Some(vec![key("pool", "")]);
        assert!(cfg.validate().is_err(), "empty keys are rejected");

        cfg.auth.api_keys = Some(vec![key("pool", "same"), key("dev", "same")]);
        assert!(
            cfg.validate().is_err(),
            "a shared secret across entries is rejected"
        );

        cfg.auth.api_keys = Some(vec![key("pool", "sécret")]);
        assert!(
            cfg.validate().is_err(),
            "non-ASCII cannot cross an authorization header"
        );

        cfg.auth.api_keys = Some(vec![key("pool", "a"), key("dev", "b")]);
        assert!(cfg.validate().is_ok());

        cfg.auth.api_keys = Some(vec![]);
        assert!(cfg.validate().is_ok(), "deny-all is valid");
    }

    #[test]
    fn session_ttl_is_bounded() {
        let mut cfg = Config::default();
        cfg.admin.session_ttl_ms = 0;
        assert!(cfg.validate().is_err(), "zero TTL is rejected");

        cfg.admin.session_ttl_ms = u64::MAX;
        assert!(cfg.validate().is_err(), "absurd TTL is rejected");

        cfg.admin.session_ttl_ms = 60_000;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn stats_history_is_bounded() {
        let mut cfg = Config::default();
        cfg.admin.stats_history_ms = 0;
        assert!(cfg.validate().is_err(), "zero history is rejected");

        cfg.admin.stats_history_ms = 86_400_000;
        assert!(cfg.validate().is_err(), "a day of samples is rejected");

        cfg.admin.stats_history_ms = 21_600_000;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn admin_key_roles_parse_and_default_to_admin() {
        let cfg = Config::from_toml_str(
            "[admin]\nkeys = [\
             { name = \"a\", key = \"ka\" }, \
             { name = \"v\", key = \"kv\", role = \"viewer\" }, \
             { name = \"o\", key = \"ko\", role = \"operator\" }]\n",
        )
        .expect("roles parse");
        let keys = cfg.admin.keys.as_deref().unwrap();
        assert_eq!(keys[0].role, Role::Admin, "role defaults to admin");
        assert_eq!(keys[1].role, Role::Viewer);
        assert_eq!(keys[2].role, Role::Operator);
        assert!(Role::Viewer < Role::Operator && Role::Operator < Role::Admin);

        assert!(
            Config::from_toml_str(
                "[admin]\nkeys = [{ name = \"a\", key = \"k\", role = \"root\" }]\n"
            )
            .is_err(),
            "unknown roles are rejected"
        );
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

    // A minimal valid enabled [cluster]: the advertise addrs are the only
    // keys without workable defaults.
    fn enabled_cluster() -> Config {
        let mut cfg = Config::default();
        cfg.cluster.enabled = true;
        cfg.cluster.client_advertise_addr = Some("sepp-1.example.com:50051".into());
        cfg.cluster.peer_advertise_addr = Some("sepp-1.internal:50052".into());
        cfg
    }

    #[test]
    fn cluster_section_round_trips_from_toml() {
        let cfg = Config::from_toml_str(
            r#"
            [cluster]
            enabled = true
            node_id = 2
            peer_listen_addr = "0.0.0.0:50052"
            peer_advertise_addr = "sepp-2.internal:50052"
            client_advertise_addr = "sepp-2.example.com:50051"
            peer_tls_cert_path = "/tls/peer.crt"
            peer_tls_key_path = "/tls/peer.key"
            peer_tls_ca_path = "/tls/ca.crt"
            peer_auth_keys = ["key-one", "key-two"]
            heartbeat_interval_ms = 100
            election_timeout_min_ms = 1000
            election_timeout_max_ms = 2000
            "#,
        )
        .expect("a full cluster section parses and validates");
        assert!(cfg.cluster.enabled);
        assert_eq!(cfg.cluster.node_id, 2);
        assert_eq!(
            cfg.cluster.peer_advertise_addr.as_deref(),
            Some("sepp-2.internal:50052")
        );
        assert_eq!(
            cfg.cluster.peer_auth_keys.as_deref(),
            Some(["key-one".to_string(), "key-two".to_string()].as_slice())
        );

        let cfg = Config::from_toml_str("").expect("empty config");
        assert!(!cfg.cluster.enabled, "cluster defaults to disabled");
    }

    #[test]
    fn cluster_mode_bans_buffer_persist_mode() {
        let mut cfg = enabled_cluster();
        cfg.storage.persist_mode = PersistMode::Buffer;
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("buffer"),
            "unexpected error: {err}"
        );

        // Buffer stays valid outside cluster mode.
        let mut cfg = Config::default();
        cfg.storage.persist_mode = PersistMode::Buffer;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn enabled_cluster_needs_client_advertise_addr() {
        assert!(enabled_cluster().validate().is_ok());

        let mut cfg = enabled_cluster();
        cfg.cluster.client_advertise_addr = None;
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("client_advertise_addr"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn wildcard_peer_bind_needs_an_advertise_addr() {
        let mut cfg = enabled_cluster();
        cfg.cluster.peer_advertise_addr = None;
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("peer_advertise_addr"),
            "unexpected error: {err}"
        );

        // A concrete bind is dialable as-is.
        let mut cfg = enabled_cluster();
        cfg.cluster.peer_advertise_addr = None;
        cfg.cluster.peer_listen_addr = "10.0.0.5:50052".parse().unwrap();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn cluster_timer_ordering_is_enforced() {
        let mut cfg = enabled_cluster();
        cfg.cluster.election_timeout_min_ms = cfg.cluster.election_timeout_max_ms;
        assert!(cfg.validate().is_err(), "min must stay below max");

        let mut cfg = enabled_cluster();
        cfg.cluster.heartbeat_interval_ms = cfg.cluster.election_timeout_min_ms;
        assert!(cfg.validate().is_err(), "heartbeat must stay below min");
    }

    #[test]
    fn peer_tls_pairing_and_orphan_ca_are_rejected() {
        let mut cfg = Config::default();
        cfg.cluster.peer_tls_cert_path = Some("/tls/peer.crt".into());
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("cluster.peer_tls"),
            "unexpected error: {err}"
        );

        let mut cfg = Config::default();
        cfg.cluster.peer_tls_ca_path = Some("/tls/ca.crt".into());
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("peer_tls_ca_path"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn peer_auth_keys_are_shape_checked() {
        let mut cfg = Config::default();
        cfg.cluster.peer_auth_keys = Some(vec![]);
        assert!(
            cfg.validate().is_err(),
            "an empty list authenticates nothing"
        );

        let mut cfg = Config::default();
        cfg.cluster.peer_auth_keys = Some(vec!["".into()]);
        assert!(cfg.validate().is_err(), "empty entries are rejected");

        let mut cfg = enabled_cluster();
        cfg.cluster.peer_auth_keys = Some(vec!["kü".into()]);
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("ASCII"), "unexpected error: {err}");

        let mut cfg = enabled_cluster();
        cfg.cluster.peer_auth_keys = Some(vec!["dup".into(), "dup".into()]);
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("duplicates"),
            "unexpected error: {err}"
        );

        let mut cfg = enabled_cluster();
        cfg.cluster.peer_auth_keys = Some(vec!["key-one".into(), "key-two".into()]);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn bootstrap_single_needs_cluster_enabled() {
        let mut cfg = Config::default();
        cfg.cluster.bootstrap_single = true;
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("bootstrap_single"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn cluster_node_id_zero_is_rejected() {
        let mut cfg = Config::default();
        cfg.cluster.node_id = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn host_port_accepts_dialable_shapes_only() {
        for ok in [
            "sepp-1.internal:50052",
            "10.0.0.5:50051",
            "[::1]:50052",
            "localhost:1",
            "host:65535",
        ] {
            assert!(host_port(ok, &()).is_ok(), "{ok:?} should be accepted");
        }
        for bad in [
            "",
            "sepp-1.internal",
            ":50052",
            "::1:50052",
            "[::1]",
            "10.0.0.5:0",
            "host:0",
            "host:70000",
            "host:52/path",
        ] {
            assert!(host_port(bad, &()).is_err(), "{bad:?} should be rejected");
        }
    }
}
