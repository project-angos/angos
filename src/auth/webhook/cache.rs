use tracing::warn;

use crate::{cache::Cache, metrics_provider::metrics_provider};

pub async fn lookup_cached_decision(cache: &Cache, name: &str, cache_key: &str) -> Option<bool> {
    match cache.retrieve::<bool>(cache_key).await {
        Ok(Some(value)) => {
            let label = if value { "cached_allow" } else { "cached_deny" };
            metrics_provider()
                .webhook_auth_requests
                .with_label_values(&[name, label])
                .inc();
            Some(value)
        }
        Ok(None) => None,
        Err(err) => {
            warn!("Webhook '{name}' cache retrieve failed for key {cache_key}: {err}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::lookup_cached_decision;
    use crate::cache::{self, Cache, stub};

    #[tokio::test]
    async fn lookup_cached_decision_unwraps_hit_to_some_value() {
        crate::metrics_provider::init_for_tests();
        let cache = cache::Config::Memory.to_backend().unwrap();
        cache.store("allow-key", &true, 60).await.unwrap();
        cache.store("deny-key", &false, 60).await.unwrap();

        assert_eq!(
            lookup_cached_decision(cache.as_ref(), "wh", "allow-key").await,
            Some(true)
        );
        assert_eq!(
            lookup_cached_decision(cache.as_ref(), "wh", "deny-key").await,
            Some(false)
        );
    }

    #[tokio::test]
    async fn lookup_cached_decision_returns_none_on_miss() {
        let cache = cache::Config::Memory.to_backend().unwrap();
        assert_eq!(
            lookup_cached_decision(cache.as_ref(), "wh", "no-such-key").await,
            None,
            "missing key must return None"
        );
    }

    #[tokio::test]
    async fn lookup_cached_decision_returns_none_on_error() {
        let backend = stub::Backend::new();
        backend.set_retrieve_error(Some("injected retrieve failure".to_string()));
        let cache = Cache::Stub(backend);
        assert_eq!(
            lookup_cached_decision(&cache, "wh", "any-key").await,
            None,
            "backend error must return None"
        );
    }
}
