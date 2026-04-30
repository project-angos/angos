use std::sync::Arc;

use crate::{
    cache::{Cache, CacheExt},
    command::server::Error,
};

pub async fn cache_retrieve(
    cache: &Arc<dyn Cache>,
    name: &str,
    cache_key: &str,
) -> Result<Option<bool>, Error> {
    let Ok(Some(cached)) = cache.retrieve::<bool>(cache_key).await else {
        return Ok(None);
    };

    let label = if cached {
        "cached_allow"
    } else {
        "cached_deny"
    };

    super::authorizer::WEBHOOK_REQUESTS
        .with_label_values(&[name, label])
        .inc();

    Ok(Some(cached))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::cache_retrieve;
    use crate::cache::{self, Cache, CacheExt};

    #[tokio::test]
    async fn cache_retrieve_returns_none_on_miss() {
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = cache_retrieve(&cache, "wh", "missing-key").await;

        assert_eq!(result, Ok(None));
    }

    #[tokio::test]
    async fn cache_retrieve_returns_true_on_allow_hit() {
        let cache = cache::Config::Memory.to_backend().unwrap();
        cache.store("the-key", &true, 60).await.unwrap();

        let result = cache_retrieve(&cache, "wh", "the-key").await;

        assert_eq!(result, Ok(Some(true)));
    }

    #[tokio::test]
    async fn cache_retrieve_returns_false_on_deny_hit() {
        let cache = cache::Config::Memory.to_backend().unwrap();
        cache.store("deny-key", &false, 60).await.unwrap();

        let result = cache_retrieve(&cache, "wh", "deny-key").await;

        assert_eq!(result, Ok(Some(false)));
    }

    #[tokio::test]
    async fn cache_retrieve_returns_none_after_ttl_expiry() {
        let cache = cache::Config::Memory.to_backend().unwrap();
        // Store with TTL of 1 second then wait for expiry.
        cache.store("exp-key", &true, 1).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        let result = cache_retrieve(&cache, "wh", "exp-key").await;

        assert_eq!(result, Ok(None), "expired entry must be treated as a miss");
    }

    // A cache whose retrieve_value always returns a backend error.
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
            Err(cache::Error::Backend(
                "injected retrieve failure".to_string(),
            ))
        }

        async fn delete_value(&self, _key: &str) -> Result<(), cache::Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn cache_retrieve_returns_none_on_backend_error() {
        let cache: Arc<dyn Cache> = Arc::new(ErrorCache);

        // A backend error must be treated as a miss (returns Ok(None)) so
        // that authorization always falls through to the real webhook call.
        let result = cache_retrieve(&cache, "wh", "any-key").await;

        assert_eq!(
            result,
            Ok(None),
            "backend error during retrieve must not propagate — treat as miss"
        );
    }
}
