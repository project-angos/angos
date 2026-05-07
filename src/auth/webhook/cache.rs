use std::sync::Arc;

use tracing::warn;

use crate::cache::{Cache, CacheExt, CacheOutcome};

pub async fn lookup_cached_decision(
    cache: &Arc<dyn Cache>,
    name: &str,
    cache_key: &str,
) -> Option<bool> {
    match cache.retrieve::<bool>(cache_key).await {
        CacheOutcome::Hit(value) => {
            let label = if value { "cached_allow" } else { "cached_deny" };
            super::metrics::WEBHOOK_REQUESTS
                .with_label_values(&[name, label])
                .inc();
            Some(value)
        }
        CacheOutcome::Miss => None,
        CacheOutcome::Error(err) => {
            warn!("Webhook '{name}' cache retrieve failed for key {cache_key}: {err}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::lookup_cached_decision;
    use crate::cache::{self, Cache, CacheExt};

    #[derive(Debug)]
    struct ErrorCache;

    #[async_trait]
    impl Cache for ErrorCache {
        async fn store_value(
            &self,
            _key: &str,
            _value: &str,
            _expires_in: u64,
        ) -> Result<(), cache::Error> {
            Ok(())
        }

        async fn retrieve_value(&self, _key: &str) -> Result<Option<String>, cache::Error> {
            Err(cache::Error::Execution(
                "injected retrieve failure".to_string(),
            ))
        }

        async fn delete_value(&self, _key: &str) -> Result<(), cache::Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn lookup_cached_decision_unwraps_hit_to_some_value() {
        let cache = cache::Config::Memory.to_backend().unwrap();
        cache.store("allow-key", &true, 60).await.unwrap();
        cache.store("deny-key", &false, 60).await.unwrap();

        assert_eq!(
            lookup_cached_decision(&cache, "wh", "allow-key").await,
            Some(true)
        );
        assert_eq!(
            lookup_cached_decision(&cache, "wh", "deny-key").await,
            Some(false)
        );
    }

    #[tokio::test]
    async fn lookup_cached_decision_returns_none_on_miss_or_error() {
        let cache = cache::Config::Memory.to_backend().unwrap();
        assert_eq!(
            lookup_cached_decision(&cache, "wh", "no-such-key").await,
            None,
            "missing key must return None"
        );

        let error_cache: Arc<dyn Cache> = Arc::new(ErrorCache);
        assert_eq!(
            lookup_cached_decision(&error_cache, "wh", "any-key").await,
            None,
            "backend error must return None"
        );
    }
}
