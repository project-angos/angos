use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;

use crate::{
    cache::Cache,
    cache::memory::Backend as CacheMemoryBackend,
    metrics_provider,
    oci::Digest,
    registry::{
        metadata_store::{
            Backend, BlobIndex, ConditionalCapabilities, LockStrategy, link_kind::LinkKind,
            s3::BackendConfig,
        },
        path_builder,
    },
    secret::Secret,
};

mod access_time;
mod blob_index;
mod cache;
mod coordinator;
mod legacy_fallback;
mod namespace_registry;

pub fn test_config() -> BackendConfig {
    metrics_provider::init_for_tests();
    BackendConfig {
        access_key_id: Secret::new("root".to_string()),
        secret_key: Secret::new("roottoor".to_string()),
        endpoint: "http://127.0.0.1:9000".to_string(),
        region: "region".to_string(),
        bucket: "registry".to_string(),
        key_prefix: format!("test-backend-{}", uuid::Uuid::new_v4()),
        lock_strategy: LockStrategy::Memory,
        link_cache_ttl: 30,
        access_time_debounce_secs: 0,
        capabilities: None,
    }
}

pub fn test_backend_with_cache(config: &BackendConfig) -> (Backend, Arc<Cache>) {
    let cache = Arc::new(Cache::Memory(CacheMemoryBackend::new()));
    let backend = config
        .to_backend(
            Some(ConditionalCapabilities {
                put_if_none_match: true,
                put_if_match: true,
                delete_if_match: false,
            }),
            Some(cache.clone()),
        )
        .unwrap();
    (backend, cache)
}

pub fn test_backend_with_debounce(config: &BackendConfig, debounce_secs: u64) -> Backend {
    let mut cfg = config.clone();
    cfg.access_time_debounce_secs = debounce_secs;
    cfg.to_backend(
        Some(ConditionalCapabilities {
            put_if_none_match: true,
            put_if_match: true,
            delete_if_match: false,
        }),
        None,
    )
    .unwrap()
}

pub fn legacy_blob_index_with(entries: Vec<(&str, Vec<LinkKind>)>) -> BlobIndex {
    let mut namespace: HashMap<String, HashSet<LinkKind>> = HashMap::new();
    for (ns, links) in entries {
        namespace.insert(ns.to_string(), links.into_iter().collect());
    }
    BlobIndex { namespace }
}

pub async fn put_legacy_index(backend: &Backend, digest: &Digest, index: &BlobIndex) {
    let body = Bytes::from(serde_json::to_vec(index).unwrap());
    backend
        .store()
        .put(&path_builder::blob_index_path(digest), body)
        .await
        .unwrap();
}
