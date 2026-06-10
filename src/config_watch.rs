use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use notify::{RecursiveMode, Watcher};
use tracing::{debug, error, info, warn};

use crate::auth::ApiKeyInterceptor;
use crate::config::{Config, SharedConfig};
use crate::queues::{QueueRegistry, SharedRegistry};

// Each save can trigger multiple events
const DEBOUNCE: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub struct ReloadState {
    pub config: SharedConfig,
    pub registry: SharedRegistry,
    pub interceptor: ApiKeyInterceptor,
    pub reload_seq: Arc<tokio::sync::watch::Sender<u64>>,
}

pub fn spawn(path: PathBuf, state: ReloadState) -> Result<(), Box<dyn Error>> {
    let file_name = path
        .file_name()
        .ok_or("config path has no file name to watch")?
        .to_os_string();

    // A bare "sepp.toml" has an empty parent; watch the current directory.
    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };

    let (tx, rx) = mpsc::channel();
    let mut watcher = notify::recommended_watcher(move |res| {
        let _ = tx.send(res);
    })?;
    watcher.watch(&dir, RecursiveMode::NonRecursive)?;

    std::thread::Builder::new()
        .name("config-watch".into())
        .spawn(move || {
            let _watcher = watcher;

            while let Ok(res) = rx.recv() {
                let event = match res {
                    Ok(event) => event,
                    Err(e) => {
                        debug!(error = %e, "config watch event error");
                        continue;
                    }
                };

                if !event
                    .paths
                    .iter()
                    .any(|p| p.file_name() == Some(file_name.as_os_str()))
                {
                    continue;
                }

                loop {
                    std::thread::sleep(DEBOUNCE);
                    while rx.try_recv().is_ok() {}
                    apply_reload(&state, &path);
                    // Bumped even when the reload no-ops or fails, so a writer
                    // waiting on the sequence learns its change was seen.
                    state.reload_seq.send_modify(|n| *n += 1);

                    match rx.try_recv() {
                        Ok(_) => continue,
                        Err(_) => break,
                    }
                }
            }
        })?;

    Ok(())
}

fn apply_reload(state: &ReloadState, path: &Path) {
    let Some(path_str) = path.to_str() else {
        error!("config path is not valid UTF-8; skipping reload");
        return;
    };

    let new = match Config::load(Some(path_str)) {
        Ok(new) => new,
        Err(e) => {
            error!(error = %e, "config reload failed; keeping the running configuration");
            return;
        }
    };

    let current = state.config.load();
    let current: &Config = &current;
    if *current == new {
        // A touched mtime or a no-op save; nothing to do.
        return;
    }

    let restart_only = restart_only_changes(current, &new);
    if !restart_only.is_empty() {
        warn!(
            fields = ?restart_only,
            "config reload: these fields changed but require a restart to take effect; \
             the running values are unchanged",
        );
    }

    // Theoretically there is a small window in which a request could observe the new registry but the old keys
    state
        .registry
        .store(Arc::new(QueueRegistry::from_config(&new)));
    state.interceptor.set_keys(new.auth.api_keys.clone());

    state.config.store(Arc::new(new));
    info!(path = %path_str, "configuration reloaded");
}

pub fn restart_only_changes(old: &Config, new: &Config) -> Vec<&'static str> {
    let mut changed = Vec::new();

    // Bind address, db path, and TLS are consumed once during startup wiring.
    // `strict_queues` is read live per request, so it is intentionally absent.
    let (a, b) = (&old.server, &new.server);
    if a.listen_addr != b.listen_addr {
        changed.push("server.listen_addr");
    }
    if a.db_path != b.db_path {
        changed.push("server.db_path");
    }
    if a.tls_cert_path != b.tls_cert_path {
        changed.push("server.tls_cert_path");
    }
    if a.tls_key_path != b.tls_key_path {
        changed.push("server.tls_key_path");
    }

    // This applies to the actual gRPC server
    if old.limits.max_message_bytes != new.limits.max_message_bytes {
        changed.push("limits.max_message_bytes");
    }

    // Storage tuning is baked into the database and committer at startup.
    let (a, b) = (&old.storage, &new.storage);
    if a.persist_mode != b.persist_mode {
        changed.push("storage.persist_mode");
    }
    if a.sweep_interval_ms != b.sweep_interval_ms {
        changed.push("storage.sweep_interval_ms");
    }
    if a.sweep_limit != b.sweep_limit {
        changed.push("storage.sweep_limit");
    }
    if a.dead_letter_retention_ms != b.dead_letter_retention_ms {
        changed.push("storage.dead_letter_retention_ms");
    }
    if a.command_queue_capacity != b.command_queue_capacity {
        changed.push("storage.command_queue_capacity");
    }
    if a.cache_size_bytes != b.cache_size_bytes {
        changed.push("storage.cache_size_bytes");
    }
    if a.max_journaling_size_bytes != b.max_journaling_size_bytes {
        changed.push("storage.max_journaling_size_bytes");
    }
    if a.max_cached_files != b.max_cached_files {
        changed.push("storage.max_cached_files");
    }
    if a.worker_threads != b.worker_threads {
        changed.push("storage.worker_threads");
    }

    // The observability stacks are initialised exactly once.
    if old.logging != new.logging {
        changed.push("logging");
    }
    if old.tracing != new.tracing {
        changed.push("tracing");
    }
    if old.metrics != new.metrics {
        changed.push("metrics");
    }

    // The admin listener is bound once at startup.
    let (a, b) = (&old.admin, &new.admin);
    if a.enabled != b.enabled {
        changed.push("admin.enabled");
    }
    if a.listen_addr != b.listen_addr {
        changed.push("admin.listen_addr");
    }

    changed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_configs_report_no_restart_changes() {
        let cfg = Config::default();
        assert!(restart_only_changes(&cfg, &cfg).is_empty());
    }

    #[test]
    fn reloadable_limits_are_not_flagged_for_restart() {
        let mut new = Config::default();
        // These all flow through EffectiveLimits → the registry.
        new.limits.max_payload_bytes -= 1;
        new.limits.default_max_attempts += 1;
        new.limits.max_schedule_horizon_ms += 1;
        new.storage.dedup_window_ms += 1;
        // strict_queues and the per-call server limits are read live per request.
        new.server.strict_queues = !new.server.strict_queues;
        new.limits.max_reserve_batch += 1;
        new.limits.max_reserve_queues += 1;
        new.limits.max_wait_timeout_ms += 1;
        new.limits.max_enqueue_batch += 1;
        new.limits.max_queue_name_bytes += 1;
        new.limits.max_job_type_bytes += 1;
        new.limits.max_idempotency_key_bytes += 1;
        new.queues.push(crate::config::QueueConfig {
            name: "added".into(),
            ..Default::default()
        });
        assert!(restart_only_changes(&Config::default(), &new).is_empty());
    }

    #[test]
    fn strict_queues_is_hot_reloadable() {
        let mut new = Config::default();
        new.server.strict_queues = !new.server.strict_queues;
        assert!(restart_only_changes(&Config::default(), &new).is_empty());
    }

    #[test]
    fn captured_fields_are_flagged_for_restart() {
        let mut new = Config::default();
        new.server.db_path = "/somewhere/else".into();
        new.limits.max_message_bytes += 1;
        new.storage.sweep_limit += 1;
        new.metrics.enabled = !new.metrics.enabled;
        new.admin.enabled = !new.admin.enabled;
        new.admin.listen_addr = "127.0.0.1:9999".parse().unwrap();

        let changed = restart_only_changes(&Config::default(), &new);
        assert!(changed.contains(&"server.db_path"));
        assert!(changed.contains(&"limits.max_message_bytes"));
        assert!(changed.contains(&"storage.sweep_limit"));
        assert!(changed.contains(&"metrics"));
        assert!(changed.contains(&"admin.enabled"));
        assert!(changed.contains(&"admin.listen_addr"));
    }

    #[test]
    fn auth_changes_are_never_restart_only() {
        let mut new = Config::default();
        new.auth.api_keys = Some(vec!["k".into()]);
        assert!(restart_only_changes(&Config::default(), &new).is_empty());
    }

    #[test]
    fn apply_reload_publishes_new_config_registry_and_keys() {
        // Write a config that differs from defaults in all three hot-reloadable
        // surfaces: a declared queue (registry), an API key (interceptor), and
        // the config snapshot itself.
        let dir = std::env::temp_dir().join(format!("sepp_reload_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("sepp.toml");
        std::fs::write(
            &path,
            "[server]\nstrict_queues = true\n\n\
             [auth]\napi_keys = [\"secret\"]\n\n\
             [[queues]]\nname = \"emails\"\nmax_payload_bytes = 4321\n",
        )
        .unwrap();

        // Start from defaults, so the file genuinely differs.
        let state = ReloadState {
            config: Config::default().into_shared(),
            registry: QueueRegistry::from_config(&Config::default()).into_shared(),
            interceptor: ApiKeyInterceptor::new(&None),
            reload_seq: Arc::new(tokio::sync::watch::channel(0).0),
        };
        assert!(!state.interceptor.is_enforcing());
        assert!(!state.registry.load().is_declared("emails"));
        assert!(!state.config.load().server.strict_queues);

        apply_reload(&state, &path);

        // Registry picked up the declared queue and its per-queue limit.
        let reg = state.registry.load();
        assert!(reg.is_declared("emails"));
        assert_eq!(reg.effective("emails").max_payload_bytes, 4321);
        // The API-key policy is now enforced.
        assert!(state.interceptor.is_enforcing());
        // The republished snapshot is the live config handlers read: the
        // hot-reloadable strict_queues flag and the auth keys both updated.
        assert!(state.config.load().server.strict_queues);
        assert_eq!(
            state.config.load().auth.api_keys.as_deref(),
            Some(&["secret".to_string()][..])
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
