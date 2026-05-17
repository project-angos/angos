use std::io::ErrorKind;

use tracing::{info, warn};

use crate::{
    registry::metadata_store::{ConditionalCapabilities, Error},
    s3_client,
};

/// RAII guard that warns if dropped while still armed.
///
/// Create after a probe object is successfully written; call `disarm()` after
/// explicit cleanup succeeds.  If the function exits early (panic or future
/// `?` propagation) while the guard is still armed, `Drop` emits a `warn!` so
/// the orphaned key surfaces in logs.
struct ProbeGuard {
    key: Option<String>,
}

impl ProbeGuard {
    fn new(key: String) -> Self {
        Self { key: Some(key) }
    }

    /// Disarm the guard after successful cleanup so `Drop` stays silent.
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
/// Returns the probed capabilities. The caller is responsible for failing startup
/// if any required capability is absent.
pub async fn probe_conditional_capabilities(
    store: &s3_client::Backend,
) -> Result<ConditionalCapabilities, Error> {
    let probe_key = format!("_angos_probe_{}", uuid::Uuid::new_v4());
    probe_conditional_capabilities_with_key(store, &probe_key).await
}

/// Inner implementation that accepts an explicit probe key.
///
/// Exposed as `pub(super)` so tests can pass a known key and verify cleanup
/// without having to discover the UUID-suffixed key after the fact.
pub(super) async fn probe_conditional_capabilities_with_key(
    store: &s3_client::Backend,
    probe_key: &str,
) -> Result<ConditionalCapabilities, Error> {
    let content: &[u8] = b"probe";

    store.put_object(probe_key, content).await.map_err(|e| {
        Error::Lock(format!(
            "conditional capability probe: failed to create probe object: {e}"
        ))
    })?;

    let mut guard = ProbeGuard::new(probe_key.to_string());

    // Test If-None-Match: * — expect 412 because the object already exists.
    let put_if_none_match = match store.put_object_if_not_exists(probe_key, content).await {
        Err(s3_client::Error::PreconditionFailed) => true,
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
    let put_if_match = match store.read_with_etag(probe_key).await {
        Ok((_, Some(etag))) => {
            let correct = store
                .put_object_if_match(probe_key, &etag, b"updated".to_vec())
                .await
                .is_ok();
            let bogus_rejected = matches!(
                store
                    .put_object_if_match(probe_key, "\"bogus\"", b"fail".to_vec())
                    .await,
                Err(s3_client::Error::PreconditionFailed)
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

    // Test DeleteObject If-Match: <etag> — bogus ETag must fail; correct ETag must succeed.
    // Re-read the current ETag after the put_if_match update may have changed it.
    let delete_if_match = match store.read_with_etag(probe_key).await {
        Ok((_, Some(etag))) => {
            // Bogus-ETag attempt first: if the provider ignores the condition and deletes
            // the object, the correct-ETag attempt below will hit NotFound and correct=false.
            let bogus_rejected = matches!(
                store.delete_if_match(probe_key, "\"bogus\"").await,
                Err(s3_client::Error::PreconditionFailed)
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
        Err(e) if e.kind() == ErrorKind::NotFound => false,
        Err(e) => {
            warn!("conditional probe: failed to read probe object for delete_if_match test: {e}");
            false
        }
    };

    // Cleanup — may already have been deleted by the delete_if_match test.
    if let Err(e) = store.delete(probe_key).await
        && e.kind() != ErrorKind::NotFound
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
