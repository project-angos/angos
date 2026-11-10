use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::info;

/// `Instant + Duration` panics past what the clock can represent, and a TTL
/// arrives straight from an upstream token endpoint's `expires_in`; a month is
/// as good as forever.
const MAX_TTL_SECS: u64 = 30 * 24 * 60 * 60;

/// Operations between sweeps of expired entries.
const CLEANUP_INTERVAL: usize = 1000;

#[derive(Debug)]
struct Entry {
    value: String,
    expiry: Instant,
}

#[derive(Debug)]
pub struct Backend {
    store: RwLock<HashMap<String, Entry>>,
    counter: AtomicUsize,
}

impl Default for Backend {
    fn default() -> Self {
        Self::new()
    }
}

impl Backend {
    #[must_use]
    pub fn new() -> Self {
        info!("Using in-memory cache store");
        Backend {
            store: RwLock::new(HashMap::new()),
            counter: AtomicUsize::new(0),
        }
    }

    async fn maybe_cleanup(&self) {
        if self
            .counter
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(CLEANUP_INTERVAL)
        {
            let now = Instant::now();
            self.store
                .write()
                .await
                .retain(|_, entry| entry.expiry > now);
        }
    }

    pub async fn store_value(&self, key: &str, value: &str, ttl: u64) {
        self.maybe_cleanup().await;
        self.store.write().await.insert(
            key.to_string(),
            Entry {
                value: value.to_string(),
                expiry: Instant::now() + Duration::from_secs(ttl.min(MAX_TTL_SECS)),
            },
        );
    }

    pub async fn retrieve_value(&self, key: &str) -> Option<String> {
        self.maybe_cleanup().await;
        self.store
            .read()
            .await
            .get(key)
            .filter(|entry| entry.expiry > Instant::now())
            .map(|entry| entry.value.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time;

    use super::*;

    /// An unclamped TTL panics the storing task, which `angos worker` never respawns.
    #[tokio::test]
    async fn a_huge_ttl_is_clamped_rather_than_overflowing_the_expiry() {
        let cache = Backend::new();

        cache.store_value("key", "value", u64::MAX).await;

        assert_eq!(cache.retrieve_value("key").await, Some("value".to_string()));
    }

    /// Retrieval already filters expired entries, so only the map's length
    /// shows the sweep evicted them.
    #[tokio::test(start_paused = true)]
    async fn test_cleanup_on_counter() {
        const SHORT: usize = 500;
        const LONG: usize = 5;

        let cache = Backend::new();
        for i in 0..SHORT {
            cache.store_value(&format!("short_{i}"), "value", 1).await;
        }
        for i in 0..LONG {
            cache.store_value(&format!("long_{i}"), "value", 100).await;
        }

        time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(
            cache.store.read().await.len(),
            SHORT + LONG,
            "expired entries stay in the map until a sweep runs"
        );

        // Operations so far, one per store; drive the counter to the sweep.
        for i in 0..(CLEANUP_INTERVAL - SHORT - LONG) {
            cache.retrieve_value(&format!("nonexistent_{i}")).await;
        }
        assert_eq!(
            cache.store.read().await.len(),
            SHORT + LONG,
            "no sweep before the interval elapses"
        );

        // The next operation is the interval-th, so it sweeps before reading.
        assert_eq!(
            cache.retrieve_value("long_0").await,
            Some("value".to_string())
        );
        assert_eq!(
            cache.store.read().await.len(),
            LONG,
            "the sweep must evict every expired entry, not just hide it on read"
        );
    }

    #[tokio::test]
    async fn test_ttl_zero_expires_immediately() {
        let cache = Backend::new();
        cache.store_value("zero_ttl", "value", 0).await;
        assert_eq!(cache.retrieve_value("zero_ttl").await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn test_ttl_boundary() {
        const TTL_SECS: u64 = 2;
        let cache = Backend::new();
        cache.store_value("ttl_key", "alive", TTL_SECS).await;

        time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            cache.retrieve_value("ttl_key").await,
            Some("alive".to_string()),
            "entry should be present before TTL expires"
        );

        time::sleep(Duration::from_secs(3)).await;
        assert_eq!(
            cache.retrieve_value("ttl_key").await,
            None,
            "entry should be absent after TTL expires"
        );
    }
}
