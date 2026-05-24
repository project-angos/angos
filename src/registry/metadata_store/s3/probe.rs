use bytes::Bytes;
use tracing::{info, warn};

use crate::registry::metadata_store::{ConditionalCapabilities, Error};
use angos_storage::{ConditionalStore, Error as StorageError, Etag};

/// RAII guard that warns if dropped while still armed.
///
/// Create after a probe object is successfully written; call `disarm()` after
/// explicit cleanup succeeds. If the function exits early while the guard is
/// still armed, `Drop` emits a `warn!` so the orphaned key surfaces in logs.
struct ProbeGuard {
    key: Option<String>,
}

impl ProbeGuard {
    fn new(key: String) -> Self {
        Self { key: Some(key) }
    }

    fn disarm(&mut self) {
        self.key.take();
    }
}

impl Drop for ProbeGuard {
    fn drop(&mut self) {
        if let Some(key) = &self.key {
            warn!(
                "S3 probe object '{key}' may be orphaned: probe terminated before cleanup; manual deletion may be required"
            );
        }
    }
}

/// Probe each conditional S3 operation independently.
///
/// Tests `PutObject If-None-Match: *`, `PutObject If-Match: <etag>`, and
/// `DeleteObject If-Match: <etag>` in sequence. Each probe is self-validating:
/// bogus-ETag attempts verify that the provider actually enforces the condition.
///
/// Returns the probed capabilities. The caller is responsible for failing
/// startup if any required capability is absent.
pub async fn probe_conditional_capabilities(
    store: &impl ConditionalStore,
) -> Result<ConditionalCapabilities, Error> {
    let probe_key = format!("_angos_probe_{}", uuid::Uuid::new_v4());
    probe_conditional_capabilities_with_key(store, &probe_key).await
}

/// Inner implementation that accepts an explicit probe key.
///
/// Exposed as `pub` so tests can pass a known key and verify cleanup
/// without having to discover the UUID-suffixed key after the fact.
pub async fn probe_conditional_capabilities_with_key(
    store: &impl ConditionalStore,
    probe_key: &str,
) -> Result<ConditionalCapabilities, Error> {
    store
        .put(probe_key, Bytes::from_static(b"probe"))
        .await
        .map_err(|e| {
            Error::Lock(format!(
                "conditional capability probe: failed to create probe object: {e}"
            ))
        })?;

    let mut guard = ProbeGuard::new(probe_key.to_string());

    // Test If-None-Match: * — expect PreconditionFailed because the object already exists.
    let put_if_none_match = match store
        .put_if_absent(probe_key, Bytes::from_static(b"probe"))
        .await
    {
        Err(StorageError::PreconditionFailed) => true,
        Ok(_) => {
            warn!(
                "conditional probe: If-None-Match: * was accepted on existing key; provider does not enforce it"
            );
            false
        }
        Err(e) => {
            warn!("conditional probe: If-None-Match error: {e}");
            false
        }
    };

    // Test If-Match: <etag> — correct ETag must succeed; bogus ETag must fail.
    let put_if_match = match store.get_with_etag(probe_key).await {
        Ok((_, Some(etag))) => {
            let correct = store
                .put_if_match(probe_key, &etag, Bytes::from_static(b"updated"))
                .await
                .is_ok();
            let bogus_rejected = matches!(
                store
                    .put_if_match(
                        probe_key,
                        &Etag::new("\"bogus\"".to_string()),
                        Bytes::from_static(b"fail")
                    )
                    .await,
                Err(StorageError::PreconditionFailed)
            );
            correct && bogus_rejected
        }
        Ok((_, None)) => {
            warn!("conditional probe: ETag not returned; If-Match support cannot be verified");
            false
        }
        Err(e) => {
            warn!("conditional probe: failed to read probe object for If-Match test: {e}");
            false
        }
    };

    // Test DeleteObject If-Match: <etag>.
    let delete_if_match = match store.get_with_etag(probe_key).await {
        Ok((_, Some(etag))) => {
            let bogus_rejected = matches!(
                store
                    .delete_if_match(probe_key, &Etag::new("\"bogus\"".to_string()))
                    .await,
                Err(StorageError::PreconditionFailed)
            );
            let correct = store.delete_if_match(probe_key, &etag).await.is_ok();
            bogus_rejected && correct
        }
        Ok((_, None)) => {
            warn!(
                "conditional probe: ETag not returned; DeleteObject If-Match support cannot be verified"
            );
            false
        }
        Err(StorageError::NotFound) => false,
        Err(e) => {
            warn!("conditional probe: failed to read probe object for delete_if_match test: {e}");
            false
        }
    };

    // Cleanup — may already have been deleted by the delete_if_match test.
    if let Err(e) = store.delete(probe_key).await
        && !matches!(e, StorageError::NotFound)
    {
        warn!("conditional probe: cleanup failed for probe object {probe_key}: {e}");
    }
    guard.disarm();

    let capabilities = ConditionalCapabilities {
        put_if_none_match,
        put_if_match,
        delete_if_match,
    };

    info!(
        if_none_match = capabilities.put_if_none_match,
        if_match = capabilities.put_if_match,
        delete_if_match = capabilities.delete_if_match,
        "S3 conditional capability probe complete"
    );

    Ok(capabilities)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        metrics_provider,
        registry::metadata_store::{
            LockStrategy,
            s3::{BackendConfig, probe_conditional_capabilities},
        },
        secret::Secret,
    };
    use angos_s3_client as s3_client;
    use angos_storage::{ConditionalStore, Error as StorageError, s3::Backend as StorageS3Backend};

    use crate::registry::metadata_store::s3::probe::probe_conditional_capabilities_with_key;

    fn test_config() -> BackendConfig {
        metrics_provider::init_for_tests();
        BackendConfig {
            access_key_id: Secret::new("root".to_string()),
            secret_key: Secret::new("roottoor".to_string()),
            endpoint: "http://127.0.0.1:9000".to_string(),
            region: "region".to_string(),
            bucket: "registry".to_string(),
            key_prefix: format!("test-probe-{}", uuid::Uuid::new_v4()),
            lock_strategy: LockStrategy::Memory,
            link_cache_ttl: 0,
            access_time_debounce_secs: 0,
            capabilities: None,
        }
    }

    fn storage_for(config: &BackendConfig) -> StorageS3Backend {
        let http = s3_client::Backend::new(&config.to_data_store_config()).unwrap();
        StorageS3Backend::builder()
            .client(Arc::new(http))
            .build()
            .unwrap()
    }

    fn storage_for_bucket(config: &BackendConfig, bucket: &str) -> StorageS3Backend {
        let mut cfg = config.to_data_store_config();
        cfg.bucket = bucket.to_string();
        let http = s3_client::Backend::new(&cfg).unwrap();
        StorageS3Backend::builder()
            .client(Arc::new(http))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_probe_conditional_capabilities() {
        let config = test_config();
        let storage = storage_for(&config);

        let result = probe_conditional_capabilities(&storage).await;
        assert!(result.is_ok(), "Probe should pass on MinIO: {result:?}");
        let caps = result.unwrap();
        assert!(caps.put_if_none_match, "MinIO should support If-None-Match");
        assert!(caps.put_if_match, "MinIO should support If-Match");
        // MinIO does not enforce If-Match on DeleteObject (returns 200 regardless of ETag),
        // so delete_if_match is expected to be false.
        assert!(
            !caps.delete_if_match,
            "MinIO does not support conditional delete (ignores If-Match on DeleteObject)"
        );
    }

    /// After a successful probe the temporary object must not remain in the bucket.
    /// Uses a deterministic probe key so the cleanup can be confirmed via a direct
    /// `get_with_etag` call without having to list the entire bucket.
    #[tokio::test]
    async fn test_probe_cleanup_removes_probe_object() {
        let config = test_config();
        let storage = storage_for(&config);

        let probe_key = format!("_angos_probe_cleanup_{}", uuid::Uuid::new_v4());

        let result = probe_conditional_capabilities_with_key(&storage, &probe_key).await;
        assert!(
            result.is_ok(),
            "Probe should succeed against MinIO: {result:?}"
        );

        // The probe object must be absent after the call returns.
        let read_result = storage.get_with_etag(&probe_key).await;
        assert!(
            matches!(read_result, Err(StorageError::NotFound)),
            "Probe object '{probe_key}' should have been deleted after probe, got: {read_result:?}"
        );
    }

    /// Running the probe twice against the same bucket must succeed both times.
    /// This verifies that successful cleanup in the first run does not leave state
    /// that would cause the second run to fail (e.g. a leftover object that
    /// triggers an unexpected If-None-Match success on a fresh key).
    #[tokio::test]
    async fn test_probe_idempotent_back_to_back() {
        let config = test_config();
        let storage = storage_for(&config);

        let first = probe_conditional_capabilities(&storage).await;
        assert!(first.is_ok(), "First probe should succeed: {first:?}");

        let second = probe_conditional_capabilities(&storage).await;
        assert!(second.is_ok(), "Second probe should succeed: {second:?}");

        assert_eq!(
            first.unwrap(),
            second.unwrap(),
            "Probe must return consistent capabilities across successive invocations"
        );
    }

    /// When the configured bucket does not exist the probe must return an error
    /// rather than panicking or hanging, exercising the early-return branch where
    /// probe-object creation itself fails.
    #[tokio::test]
    async fn test_probe_with_nonexistent_bucket_returns_err() {
        let config = test_config();
        let storage = storage_for_bucket(&config, "angos-probe-nonexistent-bucket-xyzzy");

        let result = probe_conditional_capabilities(&storage).await;
        assert!(
            result.is_err(),
            "Probe against a non-existent bucket must return Err, got: {result:?}"
        );
    }
}
