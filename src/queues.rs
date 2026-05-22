use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::config::{Config, EffectiveLimits, QueueConfig};

pub struct QueueRegistry {
    defaults: EffectiveLimits,
    declared: HashMap<String, QueueConfig>,
}

pub type SharedRegistry = Arc<ArcSwap<QueueRegistry>>;

impl QueueRegistry {
    pub fn from_config(cfg: &Config) -> Self {
        let defaults = EffectiveLimits::from_globals(&cfg.limits, &cfg.storage);
        let declared = cfg
            .queues
            .iter()
            .map(|q| (q.name.clone(), q.clone()))
            .collect();
        Self { defaults, declared }
    }

    pub fn into_shared(self) -> SharedRegistry {
        Arc::new(ArcSwap::from_pointee(self))
    }

    pub fn effective(&self, queue: &str) -> EffectiveLimits {
        match self.declared.get(queue) {
            Some(q) => self.defaults.merged_with(q),
            None => self.defaults.clone(),
        }
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
