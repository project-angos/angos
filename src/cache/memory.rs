use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use tokio::{sync::RwLock, time::Instant};
use tracing::info;

use crate::cache::Error;

#[derive(Debug)]
pub struct Backend {
    store: RwLock<HashMap<String, (String, Instant)>>,
    counter: AtomicUsize,
}

impl Backend {
    pub fn new() -> Self {
        info!("Using in-memory cache store");
        Backend {
            store: RwLock::new(HashMap::new()),
            counter: AtomicUsize::new(0),
        }
    }

    async fn cleanup_expired(&self) {
        let mut store = self.store.write().await;
        let now = Instant::now();
        store.retain(|_, &mut (_, expiry)| expiry > now);
    }

    async fn maybe_cleanup(&self) {
        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        if count.is_multiple_of(1000) {
            self.cleanup_expired().await;
        }
    }

    pub async fn store_value(&self, key: &str, value: &str, ttl: u64) -> Result<(), Error> {
        self.maybe_cleanup().await;
        let mut store = self.store.write().await;
        store.insert(
            key.to_string(),
            (value.to_string(), Instant::now() + Duration::from_secs(ttl)),
        );
        Ok(())
    }

    pub async fn retrieve_value(&self, key: &str) -> Result<Option<String>, Error> {
        self.maybe_cleanup().await;
        let store = self.store.read().await;
        if let Some((value, expiry)) = store.get(key)
            && *expiry > Instant::now()
        {
            return Ok(Some(value.clone()));
        }

        Ok(None)
    }

    pub async fn delete_value(&self, key: &str) -> Result<(), Error> {
        let mut store = self.store.write().await;
        store.remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::{task::JoinSet, time};

    use super::*;

    #[tokio::test]
    async fn test_store_and_retrieve() {
        let cache = Backend::new();

        let res = cache.store_value("key", "value", 1).await;
        assert!(res.is_ok());
        assert_eq!(
            cache.retrieve_value("key").await,
            Ok(Some("value".to_string()))
        );

        tokio::time::sleep(Duration::from_millis(1050)).await;
        assert_eq!(cache.retrieve_value("key").await, Ok(None));
    }

    #[tokio::test]
    async fn test_cleanup_on_counter() {
        let cache = Backend::new();

        for i in 0..500 {
            cache
                .store_value(&format!("short_{i}"), "value", 1)
                .await
                .unwrap();
        }

        for i in 0..5 {
            cache
                .store_value(&format!("long_{i}"), "value", 100)
                .await
                .unwrap();
        }

        time::sleep(Duration::from_millis(1100)).await;

        for i in 0..495 {
            let _ = cache.retrieve_value(&format!("nonexistent_{i}")).await;
        }

        assert_eq!(
            cache.retrieve_value("long_0").await,
            Ok(Some("value".to_string()))
        );
        assert_eq!(cache.retrieve_value("short_0").await, Ok(None));
    }

    /// Deleting a key that was never stored must return `Ok(())` without panicking.
    #[tokio::test]
    async fn test_delete_missing_key() {
        let cache = Backend::new();
        let result = cache.delete_value("never_stored").await;
        assert!(result.is_ok());
    }

    /// Ten tasks each store a distinct key concurrently; every key must be
    /// retrievable afterwards.
    #[tokio::test]
    async fn test_concurrent_store_distinct_keys() {
        let cache = Arc::new(Backend::new());
        let mut set = JoinSet::new();

        for i in 0..10_u32 {
            let c = Arc::clone(&cache);
            set.spawn(async move {
                c.store_value(&format!("concurrent_key_{i}"), &format!("v{i}"), 60)
                    .await
                    .unwrap();
            });
        }

        while let Some(res) = set.join_next().await {
            res.unwrap();
        }

        for i in 0..10_u32 {
            assert_eq!(
                cache.retrieve_value(&format!("concurrent_key_{i}")).await,
                Ok(Some(format!("v{i}"))),
                "key concurrent_key_{i} missing after concurrent stores"
            );
        }
    }

    /// Ten tasks delete the same key concurrently.  The first deletion
    /// removes it; the rest are silent no-ops.  None must panic or return an
    /// error.
    #[tokio::test]
    async fn test_concurrent_delete_same_key() {
        let cache = Arc::new(Backend::new());
        cache.store_value("shared", "value", 60).await.unwrap();

        let mut set = JoinSet::new();
        for _ in 0..10_u32 {
            let c = Arc::clone(&cache);
            set.spawn(async move { c.delete_value("shared").await.unwrap() });
        }

        while let Some(res) = set.join_next().await {
            res.unwrap();
        }

        // Key must be gone; no panic occurred.
        assert_eq!(cache.retrieve_value("shared").await, Ok(None));
    }

    /// An entry stored with TTL 0 is stored with expiry equal to the instant of
    /// insertion (`Instant::now() + 0`).  The retrieval check is strict `expiry > now`,
    /// so any non-negative wall-clock progress between store and retrieve makes
    /// the entry invisible.  In practice the entry is never observable: the
    /// `maybe_cleanup` call inside `retrieve_value` runs first, which also
    /// acquires the write lock before the read, giving the clock time to advance
    /// at least slightly.  This test documents that TTL 0 behaves as
    /// "already expired" from the caller's perspective.
    #[tokio::test]
    async fn test_ttl_zero_expires_immediately() {
        let cache = Backend::new();
        cache.store_value("zero_ttl", "value", 0).await.unwrap();
        // The expiry instant equals the store instant; any elapsed time since
        // then makes `expiry > now` false.
        assert_eq!(cache.retrieve_value("zero_ttl").await, Ok(None));
    }

    /// An entry stored with a TTL larger than one year (400 days) must still
    /// be present when retrieved immediately without any sleep.
    #[tokio::test]
    async fn test_huge_ttl_not_expired() {
        const FOUR_HUNDRED_DAYS_SECS: u64 = 400 * 24 * 3600;
        let cache = Backend::new();
        cache
            .store_value("huge_ttl", "persisted", FOUR_HUNDRED_DAYS_SECS)
            .await
            .unwrap();
        assert_eq!(
            cache.retrieve_value("huge_ttl").await,
            Ok(Some("persisted".to_string()))
        );
    }

    /// Verifies the TTL boundary: the entry is present before expiry and absent
    /// after.  The implementation stores `tokio::time::Instant` values; because
    /// the `test-util` tokio feature is not enabled in this workspace, the test
    /// uses real wall-clock sleeps with a short TTL (2 s) and generous margins
    /// (1 s before, 3 s after) to avoid timer precision issues on CI.
    #[tokio::test]
    async fn test_ttl_boundary() {
        const TTL_SECS: u64 = 2;
        let cache = Backend::new();
        cache
            .store_value("ttl_key", "alive", TTL_SECS)
            .await
            .unwrap();

        // One second before expiry: entry must still be present.
        time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            cache.retrieve_value("ttl_key").await,
            Ok(Some("alive".to_string())),
            "entry should be present before TTL expires"
        );

        // Sleep past the TTL boundary with a generous margin.
        time::sleep(Duration::from_secs(3)).await;
        assert_eq!(
            cache.retrieve_value("ttl_key").await,
            Ok(None),
            "entry should be absent after TTL expires"
        );
    }
}
