use std::sync::Arc;

use tracing::warn;

use crate::cache::{self, Cache, CacheExt};

enum CacheOutcome {
    Hit(bool),
    Miss,
    Error(cache::Error),
}

async fn cache_retrieve(cache: &Arc<dyn Cache>, key: &str) -> CacheOutcome {
    match cache.retrieve::<bool>(key).await {
        Ok(Some(value)) => CacheOutcome::Hit(value),
        Ok(None) => CacheOutcome::Miss,
        Err(err) => CacheOutcome::Error(err),
    }
}

pub async fn lookup_cached_decision(
    cache: &Arc<dyn Cache>,
    name: &str,
    cache_key: &str,
) -> Option<bool> {
    match cache_retrieve(cache, cache_key).await {
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

    use super::{CacheOutcome, cache_retrieve, lookup_cached_decision};
    use crate::cache::{self, Cache, CacheExt};

    #[tokio::test]
    async fn cache_retrieve_returns_miss_on_missing_key() {
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = cache_retrieve(&cache, "missing-key").await;

        assert!(matches!(result, CacheOutcome::Miss));
    }

    #[tokio::test]
    async fn cache_retrieve_returns_hit_true_on_allow_value() {
        let cache = cache::Config::Memory.to_backend().unwrap();
        cache.store("the-key", &true, 60).await.unwrap();

        let result = cache_retrieve(&cache, "the-key").await;

        assert!(matches!(result, CacheOutcome::Hit(true)));
    }

    #[tokio::test]
    async fn cache_retrieve_returns_hit_false_on_deny_value() {
        let cache = cache::Config::Memory.to_backend().unwrap();
        cache.store("deny-key", &false, 60).await.unwrap();

        let result = cache_retrieve(&cache, "deny-key").await;

        assert!(matches!(result, CacheOutcome::Hit(false)));
    }

    #[tokio::test]
    async fn cache_retrieve_returns_miss_after_ttl_expiry() {
        let cache = cache::Config::Memory.to_backend().unwrap();
        cache.store("exp-key", &true, 1).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        let result = cache_retrieve(&cache, "exp-key").await;

        assert!(
            matches!(result, CacheOutcome::Miss),
            "expired entry must be treated as a miss"
        );
    }

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
    async fn cache_retrieve_returns_error_on_backend_failure() {
        let cache: Arc<dyn Cache> = Arc::new(ErrorCache);

        let result = cache_retrieve(&cache, "any-key").await;

        assert!(
            matches!(result, CacheOutcome::Error(_)),
            "backend error must surface as CacheOutcome::Error"
        );
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
