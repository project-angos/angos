use super::*;
use crate::{
    configuration::GlobalConfig,
    oci::Digest,
    policy::{AccessPolicyConfig, RetentionPolicyConfig},
    registry::metadata_store::{MetadataStoreExt, link_kind::LinkKind},
};

pub fn create_test_repositories() -> Arc<HashMap<String, Repository>> {
    let token_cache = cache::Config::default().to_backend().unwrap();

    let config = repository::Config {
        access_policy: AccessPolicyConfig {
            default_allow: true,
            ..AccessPolicyConfig::default()
        },
        retention_policy: RetentionPolicyConfig::default(),
        ..repository::Config::default()
    };

    let mut repositories = HashMap::new();
    repositories.insert(
        "test-repo".to_string(),
        Repository::new("test-repo", &config, &token_cache).unwrap(),
    );

    Arc::new(repositories)
}

pub fn create_test_registry(
    blob_store: Arc<dyn BlobStore + Send + Sync>,
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
) -> Registry {
    let repositories_config = create_test_repositories();
    let global = GlobalConfig::default();

    let config = RegistryConfig::new()
        .update_pull_time(global.update_pull_time)
        .enable_blob_redirect(global.resolved_enable_blob_redirect())
        .enable_manifest_redirect(global.resolved_enable_manifest_redirect())
        .concurrent_cache_jobs(global.max_concurrent_cache_jobs)
        .global_immutable_tags(global.immutable_tags)
        .global_immutable_tags_exclusions(global.immutable_tags_exclusions.clone());

    Registry::new(blob_store, metadata_store, repositories_config, config).unwrap()
}

pub async fn create_test_blob(
    registry: &Registry,
    namespace: &str,
    content: &[u8],
) -> (Digest, Repository) {
    // Create a test blob
    let digest = registry.blob_store.create_blob(content).await.unwrap();

    let tag_link = LinkKind::Tag("latest".to_string());
    let layer_link = LinkKind::Layer(digest.clone());
    let mut tx = registry.metadata_store.begin_transaction(namespace);
    tx.create_link(&tag_link, &digest).add();
    tx.create_link(&layer_link, &digest).add();
    tx.commit().await.unwrap();

    let blob_index = registry
        .metadata_store
        .read_blob_index(&digest)
        .await
        .unwrap();
    assert!(blob_index.namespace.contains_key(namespace));
    let namespace_links = blob_index.namespace.get(namespace).unwrap();
    assert!(namespace_links.contains(&layer_link));

    // Create a non-pull-through repository
    let cache = cache::Config::Memory.to_backend().unwrap();
    let repository = Repository::new(
        "test-repo",
        &repository::Config {
            upstream: Vec::new(),
            access_policy: AccessPolicyConfig::default(),
            retention_policy: RetentionPolicyConfig { rules: Vec::new() },
            immutable_tags: false,
            immutable_tags_exclusions: Vec::new(),
            ..repository::Config::default()
        },
        &cache,
    )
    .unwrap();

    (digest, repository)
}
