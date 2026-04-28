use std::io::ErrorKind;

use tracing::{info, warn};

use crate::registry::{
    data_store,
    metadata_store::{ConditionalCapabilities, Error},
};

/// Probe each conditional S3 operation independently.
///
/// Tests `PutObject If-None-Match: *`, `PutObject If-Match: <etag>`, and
/// `DeleteObject If-Match: <etag>` in sequence. Each probe is self-validating:
/// bogus-ETag attempts verify that the provider actually enforces the condition.
///
/// Returns the probed capabilities. The caller is responsible for failing startup
/// if any required capability is absent.
pub async fn probe_conditional_capabilities(
    store: &data_store::s3::Backend,
) -> Result<ConditionalCapabilities, Error> {
    let probe_key = format!("_angos_probe_{}", uuid::Uuid::new_v4());
    let content: &[u8] = b"probe";

    store.put_object(&probe_key, content).await.map_err(|e| {
        Error::Lock(format!(
            "conditional capability probe: failed to create probe object: {e}"
        ))
    })?;

    // Test If-None-Match: * — expect 412 because the object already exists.
    let put_if_none_match = match store.put_object_if_not_exists(&probe_key, content).await {
        Err(data_store::Error::PreconditionFailed) => true,
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
    let put_if_match = match store.read_with_etag(&probe_key).await {
        Ok((_, Some(etag))) => {
            let correct = store
                .put_object_if_match(&probe_key, &etag, b"updated".to_vec())
                .await
                .is_ok();
            let bogus_rejected = matches!(
                store
                    .put_object_if_match(&probe_key, "\"bogus\"", b"fail".to_vec())
                    .await,
                Err(data_store::Error::PreconditionFailed)
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
    let delete_if_match = match store.read_with_etag(&probe_key).await {
        Ok((_, Some(etag))) => {
            // Bogus-ETag attempt first: if the provider ignores the condition and deletes
            // the object, the correct-ETag attempt below will hit NotFound and correct=false.
            let bogus_rejected = matches!(
                store.delete_if_match(&probe_key, "\"bogus\"").await,
                Err(data_store::Error::PreconditionFailed)
            );
            let correct = store.delete_if_match(&probe_key, &etag).await.is_ok();
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
    if let Err(e) = store.delete(&probe_key).await
        && e.kind() != ErrorKind::NotFound
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
