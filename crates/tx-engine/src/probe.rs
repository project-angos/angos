//! Conditional-write capability probing.
//!
//! [`probe_conditional_capabilities`] tests each S3 conditional operation
//! independently against the live store. The result drives the executor
//! choice (CAS or locked) in [`Store::new`](crate::store::Store::new).

use bytes::Bytes;
use tracing::{info, warn};

use angos_storage::{ConditionalStore, Error as StorageError, Etag};

use crate::{ConditionalCapabilities, error::Error};

/// Probe each conditional S3 operation independently.
///
/// Tests `PutObject If-None-Match: *`, `PutObject If-Match: <etag>`, and
/// `DeleteObject If-Match: <etag>` in sequence. Each probe is self-validating:
/// bogus-ETag attempts verify that the provider actually enforces the
/// condition.
///
/// # Errors
///
/// Returns [`Error::Storage`] when the initial probe object cannot be
/// written (e.g. the bucket does not exist or credentials are invalid).
pub async fn probe_conditional_capabilities(
    store: &impl ConditionalStore,
) -> Result<ConditionalCapabilities, Error> {
    let probe_key = format!("_angos_probe_{}", uuid::Uuid::new_v4());
    probe_conditional_capabilities_with_key(store, &probe_key).await
}

/// Inner implementation that accepts an explicit probe key.
///
/// Exposed so tests can pass a known key and verify cleanup without having
/// to discover the UUID-suffixed key after the fact.
///
/// # Errors
///
/// Returns [`Error::Storage`] when the initial probe write fails.
pub async fn probe_conditional_capabilities_with_key(
    store: &impl ConditionalStore,
    probe_key: &str,
) -> Result<ConditionalCapabilities, Error> {
    store
        .put(probe_key, Bytes::from_static(b"probe"))
        .await
        .map_err(|e| {
            Error::Storage(StorageError::Backend(format!(
                "conditional capability probe: failed to create probe object: {e}"
            )))
        })?;

    // Test If-None-Match: *: expect PreconditionFailed because the object already exists.
    let put_if_none_match = match store
        .put_if_absent(probe_key, Bytes::from_static(b"probe"))
        .await
    {
        Err(StorageError::PreconditionFailed) => true,
        Ok(_) => {
            warn!(
                "conditional probe: If-None-Match: * was accepted on existing key; \
                 provider does not enforce it"
            );
            false
        }
        Err(e) => {
            warn!("conditional probe: If-None-Match error: {e}");
            false
        }
    };

    // Test If-Match: <etag>: correct ETag must succeed; bogus ETag must fail.
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
                        Bytes::from_static(b"fail"),
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
                "conditional probe: ETag not returned; DeleteObject If-Match support \
                 cannot be verified"
            );
            false
        }
        Err(StorageError::NotFound) => false,
        Err(e) => {
            warn!("conditional probe: failed to read probe object for delete_if_match test: {e}");
            false
        }
    };

    // Cleanup: may already have been deleted by the delete_if_match test.
    if let Err(e) = store.delete(probe_key).await
        && !matches!(e, StorageError::NotFound)
    {
        warn!("conditional probe: cleanup failed for probe object {probe_key}: {e}");
    }

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
