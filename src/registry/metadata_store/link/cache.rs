//! The link-metadata cache helpers: namespaced key derivation and
//! get/put/invalidate over the optional cache, gated on `link_cache_ttl`.
//!
//! These back the cache-aware `read_link` in [`super::storage`] and the
//! post-commit cache maintenance in [`super::ops`].

use tracing::warn;

use crate::registry::metadata_store::{LinkKind, LinkMetadata, MetadataStore};

impl MetadataStore {
    fn cache_key(namespace: &str, link: &LinkKind) -> String {
        format!("link:{namespace}:{link}")
    }

    pub async fn cache_get(&self, namespace: &str, link: &LinkKind) -> Option<LinkMetadata> {
        if self.link_cache_ttl == 0 {
            return None;
        }
        let cache = self.cache.as_ref()?;
        cache
            .retrieve::<LinkMetadata>(&Self::cache_key(namespace, link))
            .await
            .ok()
            .flatten()
    }

    pub async fn cache_put(&self, namespace: &str, link: &LinkKind, metadata: &LinkMetadata) {
        if self.link_cache_ttl == 0 {
            return;
        }
        if let Some(cache) = &self.cache {
            let key = Self::cache_key(namespace, link);
            if let Err(err) = cache.store(&key, metadata, self.link_cache_ttl).await {
                warn!("Failed to store link metadata in cache for {namespace}/{link}: {err}");
            }
        }
    }

    pub async fn cache_invalidate(&self, namespace: &str, link: &LinkKind) {
        if let Some(cache) = &self.cache {
            let _ = cache.delete_value(&Self::cache_key(namespace, link)).await;
        }
    }
}
