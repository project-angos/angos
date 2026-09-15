//! Maintenance of the OCI-1.0 referrers fallback tag on downstreams that do
//! not auto-index subjects: the push side merges a referrer's descriptor into
//! the subject's fallback index, the delete side prunes it, and both run their
//! GET/modify/PUT under one per-subject lock so concurrent jobs cannot drop
//! each other's update.

use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex as StdMutex, PoisonError},
    time::Duration,
};

use tokio::{sync::Mutex as AsyncMutex, time::timeout};
use tracing::warn;

use angos_oci::manifest_accept_types;
use angos_oci::request::{DeleteManifestRequest, GetManifestRequest, PutManifestRequest};
use angos_oci::{Content, Descriptor, Digest, Manifest, MediaType, Namespace, Reference};
use angos_oci_client::{Error as ClientError, RegistryClient};

use crate::{registry::Error as RegistryError, replication::Error};

/// Upper bound on each downstream HTTP call inside the referrers-merge
/// critical section, so a hung downstream cannot hold the per-subject merge
/// mutex indefinitely and starve concurrent jobs of the same subject.
const REFERRERS_MERGE_HTTP_TIMEOUT: Duration = Duration::from_mins(1);

/// Per-subject merge mutexes, keyed by `namespace:subject`. In-process only,
/// which covers this path because the queue drains each store from a single
/// worker process pool.
static SUBJECT_LOCKS: LazyLock<StdMutex<HashMap<String, Arc<AsyncMutex<()>>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

/// Pushes the OCI-1.0 referrers fallback tag index for a subject-bearing
/// manifest the downstream did not auto-index.
///
/// The fallback tag (`<alg>-<hash>` of the subject digest) holds a merged image
/// index of referrer descriptors; its PUT is deliberately timestamp-less, since
/// a set merge must never lose an LWW comparison and drop a descriptor.
pub async fn push_referrers_fallback(
    downstream: &RegistryClient,
    namespace: &Namespace,
    downstream_namespace: &Namespace,
    digest: &Digest,
    manifest: Manifest,
    body: &[u8],
) -> Result<(), Error> {
    let Some(subject) = manifest
        .subject
        .as_ref()
        .map(|subject| subject.digest.clone())
    else {
        return Ok(());
    };
    let fallback_tag = subject.referrers_fallback_tag();
    let reference = Reference::Tag(fallback_tag.clone());
    warn!(
        namespace = %namespace,
        %digest,
        %subject,
        %fallback_tag,
        "Downstream did not index subject (OCI-1.0); merging referrers fallback index"
    );
    with_subject_lock(namespace, &subject, async || {
        merge_referrers_fallback(
            downstream,
            downstream_namespace,
            &reference,
            digest,
            manifest,
            body,
        )
        .await
    })
    .await
}

/// Drops `referrer`'s descriptor from the subject's OCI-1.0 referrers fallback
/// index, deleting the fallback tag when no referrers remain. Serialized under
/// the same per-subject lock as the push-side merge, so a concurrent add and
/// this removal cannot lose each other's update.
pub async fn remove_referrers_fallback(
    downstream: &RegistryClient,
    namespace: &Namespace,
    downstream_namespace: &Namespace,
    subject: &Digest,
    referrer: &Digest,
) -> Result<(), Error> {
    let fallback_tag = subject.referrers_fallback_tag();
    let reference = Reference::Tag(fallback_tag.clone());
    with_subject_lock(namespace, subject, async || {
        prune_fallback_descriptor(downstream, downstream_namespace, &reference, referrer).await
    })
    .await
}

/// GETs the manifest at `digest` from the downstream and returns its subject
/// when it is a referrer. Best-effort: any failure (absent, unparseable, or no
/// subject) yields `None`, leaving the fallback index untouched.
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
        .map_err(|e| RegistryError::manifest_invalid(&e))
        .ok()?
        .subject
        .map(|subject| subject.digest)
}

/// Runs `critical` (the GET/modify/PUT of one fallback index) under the
/// subject's merge mutex: two referrers of the same subject are distinct jobs
/// the queue runs concurrently, and unserialized merges read the same base index
/// and drop the loser's descriptor. The key is downstream-agnostic because the
/// critical section is two short HTTP calls, and being in-process the mutex
/// cannot cover an unrelated sender registry pushing to the same downstream.
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

/// The locked critical section of [`push_referrers_fallback`]: fetches the
/// downstream's current fallback index, appends this referrer's descriptor when
/// absent, and PUTs the merged index back.
async fn merge_referrers_fallback(
    downstream: &RegistryClient,
    namespace: &Namespace,
    reference: &Reference,
    digest: &Digest,
    mut manifest: Manifest,
    body: &[u8],
) -> Result<(), Error> {
    let mut manifests = fetch_fallback_manifests(downstream, namespace, reference).await?;

    // Dedup by digest so a re-run is idempotent.
    if !manifests.iter().any(|entry| entry.digest == *digest) {
        manifests.push(manifest.take_descriptor(digest.clone(), body.len() as u64));
    }

    put_fallback_manifests(downstream, namespace, reference, manifests).await
}

/// Locked critical section of [`remove_referrers_fallback`]: GET the index, drop
/// the descriptor, then PUT the remainder back, or DELETE the tag when empty.
async fn prune_fallback_descriptor(
    downstream: &RegistryClient,
    namespace: &Namespace,
    reference: &Reference,
    referrer: &Digest,
) -> Result<(), Error> {
    let mut manifests = fetch_fallback_manifests(downstream, namespace, reference).await?;

    let before = manifests.len();
    manifests.retain(|entry| entry.digest != *referrer);
    // Absent already: pruned by an earlier attempt, or a 1.1 downstream has no
    // fallback tag at all.
    if manifests.len() == before {
        return Ok(());
    }

    if manifests.is_empty() {
        // No referrers remain: drop the tag rather than leave an empty index.
        timeout(
            REFERRERS_MERGE_HTTP_TIMEOUT,
            downstream.delete_manifest(DeleteManifestRequest {
                namespace: namespace.clone(),
                reference: reference.clone(),
                source_ts: None,
            }),
        )
        .await
        .map_err(|_| {
            Error::Internal(format!(
                "referrers fallback DELETE of '{namespace}/{reference}' timed out inside the \
                 merge lock"
            ))
        })??;
        return Ok(());
    }

    put_fallback_manifests(downstream, namespace, reference, manifests).await
}

/// GETs the existing referrers fallback index at `reference` and returns its
/// `manifests[]` descriptors. Only a `404` yields an empty base; any other
/// error, including a `200` body that is not a parseable image index,
/// propagates so the caller never rebuilds the index from empty and drops the
/// subject's sibling referrers.
async fn fetch_fallback_manifests(
    downstream: &RegistryClient,
    namespace: &Namespace,
    reference: &Reference,
) -> Result<Vec<Descriptor>, Error> {
    let accept = manifest_accept_types();
    let get = downstream.get_manifest(GetManifestRequest {
        namespace: namespace.clone(),
        reference: reference.clone(),
        accepted_types: accept,
    });
    let body = match timeout(REFERRERS_MERGE_HTTP_TIMEOUT, get).await {
        Ok(Ok(fetched)) => fetched.body,
        Ok(Err(ClientError::ManifestUnknown)) => return Ok(Vec::new()),
        Ok(Err(e)) => return Err(Error::Client(e)),
        Err(_) => {
            return Err(Error::Internal(format!(
                "referrers fallback GET of '{namespace}/{reference}' timed out inside the merge \
                 lock"
            )));
        }
    };
    match Manifest::from_slice(&body).map(|index| index.content) {
        Ok(Content::Index { manifests }) => Ok(manifests),
        _ => Err(Error::Internal(format!(
            "downstream referrers fallback index at '{namespace}/{reference}' is not a parseable \
             image index; refusing to overwrite it"
        ))),
    }
}

/// Serializes `manifests` into an image index and PUTs it at `reference`.
/// Timestamp-less, so the receiver skips LWW and the merged index can never
/// come back superseded with a descriptor silently dropped.
async fn put_fallback_manifests(
    downstream: &RegistryClient,
    namespace: &Namespace,
    reference: &Reference,
    manifests: Vec<Descriptor>,
) -> Result<(), Error> {
    let index_body = serde_json::to_vec(&Manifest::oci_index(manifests)).map_err(|e| {
        Error::Internal(format!("failed to serialize referrers fallback index: {e}"))
    })?;
    timeout(
        REFERRERS_MERGE_HTTP_TIMEOUT,
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
    .await
    .map_err(|_| {
        Error::Internal(format!(
            "referrers fallback PUT of '{namespace}/{reference}' timed out inside the merge lock"
        ))
    })??;
    Ok(())
}
