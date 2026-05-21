use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::config::{Config, EffectiveLimits, QueueConfig};

/// Owns the global defaults and the per-queue declarations and resolves
/// effective settings for a given queue name. Independent of strict mode —
/// the strict gate is enforced separately by callers via [`Self::is_declared`].
///
/// The strict flag is intentionally *not* on the registry: flipping it
/// requires a restart, while declarations are designed to be mutated at
/// runtime (future RPCs) behind a [`SharedRegistry`].
pub struct QueueRegistry {
    defaults: EffectiveLimits,
    declared: HashMap<String, QueueConfig>,
}

/// Read-mostly handle to a [`QueueRegistry`]. Loaders grab a cheap [`arc_swap::Guard`]
/// to a snapshot; future "create/modify queue" RPCs rebuild the registry and
/// `store()` a new one.
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

    /// Returns the global defaults merged with this queue's overrides if it's
    /// declared, or the global defaults unchanged otherwise. Independent of
    /// the strict flag — declarations always take effect when the name matches.
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
    fn allowed_encodings_override_replaces_globally_allowed_list() {
        let mut cfg = cfg_with(vec![QueueConfig {
            name: "strict".into(),
            allowed_encodings: Some(vec!["protobuf".into()]),
            ..Default::default()
        }]);
        cfg.limits.allowed_encodings = Some(vec!["json".into(), "msgpack".into()]);
        let reg = QueueRegistry::from_config(&cfg);
        let eff = reg.effective("strict");
        assert_eq!(eff.allowed_encodings.as_deref(), Some(&["protobuf".into()][..]));
        let other = reg.effective("other");
        assert_eq!(
            other.allowed_encodings.as_deref(),
            Some(&["json".into(), "msgpack".into()][..])
        );
    }
}
