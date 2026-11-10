//! Maintenance of the OCI-1.0 referrers fallback tag on downstreams that do
//! not auto-index subjects: the push side merges a referrer's descriptor into
//! the subject's fallback index, the delete side prunes it, and both run their
//! GET/modify/PUT under one per-subject lock so concurrent jobs cannot drop
//! each other's update.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex as StdMutex, PoisonError};
use std::time::Duration;

use tokio::sync::Mutex as AsyncMutex;
use tokio::time::timeout;
use tracing::warn;

use angos_oci::manifest_accept_types;
use angos_oci::request::{DeleteManifestRequest, GetManifestRequest, PutManifestRequest};
use angos_oci::{Content, Descriptor, Digest, Manifest, MediaType, Namespace, Reference};
use angos_oci_client::{Error as ClientError, RegistryClient};

use crate::replication::Error;

const REFERRERS_MERGE_HTTP_TIMEOUT: Duration = Duration::from_mins(1);

/// Per-subject merge mutexes, keyed by `namespace:subject`. In-process only,
/// which covers this path because the queue drains each store from a single
/// worker process pool.
static SUBJECT_LOCKS: LazyLock<StdMutex<HashMap<String, Arc<AsyncMutex<()>>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

/// Merges the referrer `manifest` (`size` bytes, whose subject is `subject`) into the
/// subject's fallback tag (`<alg>-<hash>` of the subject digest), an image index
/// of referrer descriptors.
pub async fn push_referrers_fallback(
    downstream: &RegistryClient,
    namespace: &Namespace,
    downstream_namespace: &Namespace,
    subject: &Digest,
    digest: &Digest,
    mut manifest: Manifest,
    size: u64,
) -> Result<(), Error> {
    let fallback_tag = subject.referrers_fallback_tag();
    let reference = Reference::Tag(fallback_tag.clone());
    warn!(
        namespace = %namespace,
        %digest,
        %subject,
        %fallback_tag,
        "Downstream did not index subject (OCI-1.0); merging referrers fallback index"
    );
    with_subject_lock(namespace, subject, async || {
        let mut manifests =
            fetch_fallback_manifests(downstream, downstream_namespace, &reference).await?;
        // Dedup by digest so a re-run is idempotent.
        if !manifests.iter().any(|entry| entry.digest == *digest) {
            manifests.push(manifest.take_descriptor(digest.clone(), size));
        }
        put_fallback_manifests(downstream, downstream_namespace, &reference, manifests).await
    })
    .await
}

/// Deletes the fallback tag when no referrers remain.
pub async fn remove_referrers_fallback(
    downstream: &RegistryClient,
    namespace: &Namespace,
    downstream_namespace: &Namespace,
    subject: &Digest,
    referrer: &Digest,
) -> Result<(), Error> {
    let reference = Reference::Tag(subject.referrers_fallback_tag());
    with_subject_lock(namespace, subject, async || {
        let mut manifests =
            fetch_fallback_manifests(downstream, downstream_namespace, &reference).await?;
        let before = manifests.len();
        manifests.retain(|entry| entry.digest != *referrer);
        // Absent already: pruned by an earlier attempt, or a 1.1 downstream has
        // no fallback tag at all.
        if manifests.len() == before {
            return Ok(());
        }
        if !manifests.is_empty() {
            return put_fallback_manifests(downstream, downstream_namespace, &reference, manifests)
                .await;
        }
        // No referrers remain: drop the tag rather than leave an empty index.
        bounded(
            "DELETE",
            downstream_namespace,
            &reference,
            downstream.delete_manifest(DeleteManifestRequest {
                namespace: downstream_namespace.clone(),
                reference: reference.clone(),
                source_ts: None,
            }),
        )
        .await??;
        Ok(())
    })
    .await
}

/// Best-effort: any failure yields `None`, leaving the fallback index untouched.
pub async fn deleted_referrer_subject(
    downstream: &RegistryClient,
    downstream_namespace: &Namespace,
    digest: &Digest,
) -> Option<Digest> {
    let fetched = downstream
        .get_manifest(GetManifestRequest {
            namespace: downstream_namespace.clone(),
            reference: Reference::Digest(digest.clone()),
            accepted_types: manifest_accept_types(),
        })
        .await
        .ok()?;
    Manifest::from_slice(&fetched.body)
        .ok()?
        .subject
        .map(|subject| subject.digest)
}

/// The key is downstream-agnostic because the critical section is two short
/// HTTP calls; being in-process, the mutex cannot cover an unrelated sender
/// registry pushing to the same downstream.
async fn with_subject_lock(
    namespace: &Namespace,
    subject: &Digest,
    critical: impl AsyncFnOnce() -> Result<(), Error>,
) -> Result<(), Error> {
    let key = format!("replication-referrers:{namespace}:{subject}");
    let lock = {
        let mut locks = SUBJECT_LOCKS.lock().unwrap_or_else(PoisonError::into_inner);
        locks.entry(key.clone()).or_default().clone()
    };
    let result = {
        let _guard = lock.lock().await;
        critical().await
    };
    // Drop our clone, then forget the entry when no other job holds one, so
    // the map does not grow with every subject ever merged.
    drop(lock);
    let mut locks = SUBJECT_LOCKS.lock().unwrap_or_else(PoisonError::into_inner);
    if locks.get(&key).is_some_and(|l| Arc::strong_count(l) == 1) {
        locks.remove(&key);
    }
    result
}

/// Bounds a downstream call inside the merge lock, so a hung downstream cannot
/// starve the subject's other jobs.
async fn bounded<T>(
    method: &str,
    namespace: &Namespace,
    reference: &Reference,
    call: impl Future<Output = T>,
) -> Result<T, Error> {
    timeout(REFERRERS_MERGE_HTTP_TIMEOUT, call)
        .await
        .map_err(|_| {
            Error::Internal(format!(
                "referrers fallback {method} of '{namespace}/{reference}' timed out inside the \
                 merge lock"
            ))
        })
}

/// Only a `404` yields an empty base: any other failure, an unparseable body
/// included, propagates so the index is never rebuilt from empty, dropping the
/// subject's sibling referrers.
async fn fetch_fallback_manifests(
    downstream: &RegistryClient,
    namespace: &Namespace,
    reference: &Reference,
) -> Result<Vec<Descriptor>, Error> {
    let get = downstream.get_manifest(GetManifestRequest {
        namespace: namespace.clone(),
        reference: reference.clone(),
        accepted_types: manifest_accept_types(),
    });
    let body = match bounded("GET", namespace, reference, get).await? {
        Ok(fetched) => fetched.body,
        Err(ClientError::ManifestUnknown) => return Ok(Vec::new()),
        Err(e) => return Err(Error::Client(e)),
    };
    match Manifest::from_slice(&body).map(|index| index.content) {
        Ok(Content::Index { manifests }) => Ok(manifests),
        _ => Err(Error::Internal(format!(
            "downstream referrers fallback index at '{namespace}/{reference}' is not a parseable \
             image index; refusing to overwrite it"
        ))),
    }
}

/// Timestamp-less, so the receiver skips LWW: a set merge must never come back
/// superseded with a descriptor dropped.
async fn put_fallback_manifests(
    downstream: &RegistryClient,
    namespace: &Namespace,
    reference: &Reference,
    manifests: Vec<Descriptor>,
) -> Result<(), Error> {
    let index_body = serde_json::to_vec(&Manifest::oci_index(manifests)).map_err(|e| {
        Error::Internal(format!("failed to serialize referrers fallback index: {e}"))
    })?;
    bounded(
        "PUT",
        namespace,
        reference,
        downstream.put_manifest(
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: reference.clone(),
                content_type: Some(MediaType::oci_index()),
                tags: Vec::new(),
                source_ts: None,
            },
            index_body,
        ),
    )
    .await??;
    Ok(())
}
