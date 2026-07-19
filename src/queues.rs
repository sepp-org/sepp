use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::config::{Config, EffectiveLimits, QueueConfig, RetryBackoff};

pub struct QueueRegistry {
    defaults: EffectiveLimits,
    declared: HashMap<String, QueueConfig>,
    // True when some queue can resolve a nonzero policy delay; when false the
    // nack path skips its inflight lookup entirely.
    any_retry_policy: bool,
    // The floor of max_lease_duration_ms across the defaults and every queue
    // override: a lease at or under it is unclamped for every possible queue,
    // so the extend path can skip its inflight lookup.
    min_max_lease_ms: u64,
    // Bumped on every publish() so a registry snapshot can be referenced
    // compactly (e.g. by an op-stream recording). Boot is generation 0.
    generation: u64,
}

// Cheaper than `effective()`: no allow-list vec clones.
pub struct RetryPolicy {
    pub retry_delay_ms: u64,
    pub retry_backoff: RetryBackoff,
    pub retry_delay_max_ms: u64,
    pub max_schedule_horizon_ms: u64,
}

pub type SharedRegistry = Arc<ArcSwap<QueueRegistry>>;

// All swaps go through here so generations stay monotonic.
pub fn publish(shared: &SharedRegistry, mut next: QueueRegistry) {
    next.generation = shared.load().generation + 1;
    shared.store(Arc::new(next));
}

impl QueueRegistry {
    pub fn from_config(cfg: &Config) -> Self {
        let defaults = EffectiveLimits::from_globals(&cfg.limits, &cfg.storage);
        let declared = cfg
            .queues
            .iter()
            .map(|q| (q.name.clone(), q.clone()))
            .collect();

        Self {
            any_retry_policy: defaults.retry_delay_ms > 0
                || cfg
                    .queues
                    .iter()
                    .any(|q| q.retry_delay_ms.is_some_and(|v| v > 0)),
            min_max_lease_ms: cfg
                .queues
                .iter()
                .filter_map(|q| q.max_lease_duration_ms)
                .fold(defaults.max_lease_duration_ms, u64::min),
            defaults,
            declared,
            generation: 0,
        }
    }

    pub fn into_shared(self) -> SharedRegistry {
        Arc::new(ArcSwap::from_pointee(self))
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn effective(&self, queue: &str) -> EffectiveLimits {
        match self.declared.get(queue) {
            Some(q) => self.defaults.merged_with(q),
            None => self.defaults.clone(),
        }
    }

    pub fn any_retry_policy(&self) -> bool {
        self.any_retry_policy
    }

    pub fn min_max_lease_ms(&self) -> u64 {
        self.min_max_lease_ms
    }

    pub fn retry_policy(&self, queue: &str) -> RetryPolicy {
        let q = self.declared.get(queue);
        RetryPolicy {
            retry_delay_ms: q
                .and_then(|q| q.retry_delay_ms)
                .unwrap_or(self.defaults.retry_delay_ms),
            retry_backoff: q
                .and_then(|q| q.retry_backoff)
                .unwrap_or(self.defaults.retry_backoff),
            retry_delay_max_ms: q
                .and_then(|q| q.retry_delay_max_ms)
                .unwrap_or(self.defaults.retry_delay_max_ms),
            max_schedule_horizon_ms: q
                .and_then(|q| q.max_schedule_horizon_ms)
                .unwrap_or(self.defaults.max_schedule_horizon_ms),
        }
    }

    // Cheaper than `effective()` for the enqueue hot path: reads one field
    // without cloning the allow-list vecs.
    pub fn dedup_window_ms(&self, queue: &str) -> i64 {
        self.declared
            .get(queue)
            .and_then(|q| q.dedup_window_ms)
            .unwrap_or(self.defaults.dedup_window_ms)
    }

    pub fn is_declared(&self, queue: &str) -> bool {
        self.declared.contains_key(queue)
    }

    pub fn declared_names(&self) -> impl Iterator<Item = &str> {
        self.declared.keys().map(String::as_str)
    }

    pub fn declared_count(&self) -> usize {
        self.declared.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, QueueConfig};

    fn cfg_with(queues: Vec<QueueConfig>) -> Config {
        Config {
            queues,
            ..Default::default()
        }
    }

    #[test]
    fn publish_bumps_the_generation() {
        let shared = QueueRegistry::from_config(&cfg_with(vec![])).into_shared();
        assert_eq!(shared.load().generation(), 0);

        publish(&shared, QueueRegistry::from_config(&cfg_with(vec![])));
        publish(&shared, QueueRegistry::from_config(&cfg_with(vec![])));
        assert_eq!(shared.load().generation(), 2);
    }

    #[test]
    fn undeclared_queue_resolves_to_defaults() {
        let cfg = cfg_with(vec![]);
        let reg = QueueRegistry::from_config(&cfg);
        let eff = reg.effective("unknown");
        assert_eq!(eff.max_lease_duration_ms, cfg.limits.max_lease_duration_ms);
        assert_eq!(eff.default_max_attempts, cfg.limits.default_max_attempts);
        assert!(!reg.is_declared("unknown"));
    }

    #[test]
    fn declared_queue_overrides_only_set_fields() {
        let mut cfg = cfg_with(vec![QueueConfig {
            name: "emails".into(),
            max_lease_duration_ms: Some(60_000),
            default_priority: Some(7),
            ..Default::default()
        }]);
        // Pick a non-default global so we can tell merge from clone.
        cfg.limits.default_max_attempts = 4;
        let reg = QueueRegistry::from_config(&cfg);
        let eff = reg.effective("emails");
        assert_eq!(eff.max_lease_duration_ms, 60_000);
        assert_eq!(eff.default_priority, 7);
        assert_eq!(
            eff.default_max_attempts, 4,
            "fields without an override fall back to the global"
        );
        assert!(reg.is_declared("emails"));
    }

    #[test]
    fn retry_policy_matches_effective() {
        let mut cfg = cfg_with(vec![QueueConfig {
            name: "emails".into(),
            retry_delay_ms: Some(5_000),
            ..Default::default()
        }]);
        cfg.limits.retry_delay_ms = 1_000;
        cfg.limits.retry_backoff = crate::config::RetryBackoff::Exponential;
        let reg = QueueRegistry::from_config(&cfg);

        let policy = reg.retry_policy("emails");
        let eff = reg.effective("emails");
        assert_eq!(policy.retry_delay_ms, 5_000);
        assert_eq!(policy.retry_backoff, eff.retry_backoff);
        assert_eq!(policy.retry_delay_max_ms, eff.retry_delay_max_ms);
        assert_eq!(policy.max_schedule_horizon_ms, eff.max_schedule_horizon_ms);
        assert_eq!(reg.retry_policy("other").retry_delay_ms, 1_000);

        assert!(reg.any_retry_policy());
        assert!(!QueueRegistry::from_config(&cfg_with(vec![])).any_retry_policy());
    }

    #[test]
    fn min_max_lease_is_the_floor_across_defaults_and_overrides() {
        let mut cfg = cfg_with(vec![
            QueueConfig {
                name: "short".into(),
                max_lease_duration_ms: Some(5_000),
                ..Default::default()
            },
            QueueConfig {
                name: "long".into(),
                max_lease_duration_ms: Some(600_000),
                ..Default::default()
            },
        ]);
        cfg.limits.max_lease_duration_ms = 30_000;
        let reg = QueueRegistry::from_config(&cfg);
        assert_eq!(reg.min_max_lease_ms(), 5_000);

        let no_overrides = QueueRegistry::from_config(&cfg_with(vec![]));
        assert_eq!(
            no_overrides.min_max_lease_ms(),
            no_overrides.effective("any").max_lease_duration_ms
        );
    }

    #[test]
    fn dedup_window_ms_matches_effective() {
        let mut cfg = cfg_with(vec![QueueConfig {
            name: "emails".into(),
            dedup_window_ms: Some(5_000),
            ..Default::default()
        }]);
        cfg.storage.dedup_window_ms = 1_000;
        let reg = QueueRegistry::from_config(&cfg);
        assert_eq!(reg.dedup_window_ms("emails"), 5_000);
        assert_eq!(
            reg.dedup_window_ms("emails"),
            reg.effective("emails").dedup_window_ms
        );
        assert_eq!(reg.dedup_window_ms("other"), 1_000);
        assert_eq!(
            reg.dedup_window_ms("other"),
            reg.effective("other").dedup_window_ms
        );
    }

    #[test]
    fn allowed_job_types_is_per_queue_only() {
        let cfg = cfg_with(vec![QueueConfig {
            name: "strict".into(),
            allowed_job_types: Some(vec!["send_email".into()]),
            ..Default::default()
        }]);
        let reg = QueueRegistry::from_config(&cfg);
        assert_eq!(
            reg.effective("strict").allowed_job_types.as_deref(),
            Some(&["send_email".into()][..])
        );
        assert!(
            reg.effective("other").allowed_job_types.is_none(),
            "queues without an override accept any job_type"
        );
    }

    #[test]
    fn allowed_encodings_override_replaces_globally_allowed_list() {
        let mut cfg = cfg_with(vec![QueueConfig {
            name: "strict".into(),
            allowed_encodings: Some(vec!["protobuf".into()]),
            ..Default::default()
        }]);
        cfg.limits.allowed_encodings = Some(vec!["json".into(), "msgpack".into()]);
        let reg = QueueRegistry::from_config(&cfg);
        let eff = reg.effective("strict");
        assert_eq!(
            eff.allowed_encodings.as_deref(),
            Some(&["protobuf".into()][..])
        );
        let other = reg.effective("other");
        assert_eq!(
            other.allowed_encodings.as_deref(),
            Some(&["json".into(), "msgpack".into()][..])
        );
    }
}
