//! Maintenance of the OCI-1.0 referrers fallback tag on downstreams that do
//! not auto-index subjects: the push side merges a referrer's descriptor into
//! the subject's fallback index, the delete side prunes it, and both run their
//! GET/modify/PUT under one per-subject lock so concurrent jobs cannot drop
//! each other's update.

use std::{str::FromStr, sync::Arc, time::Duration};

use serde_json::{Value, json};
use tokio::time::timeout;
use tracing::warn;

use crate::{
    oci::{Digest, Namespace, OCI_INDEX_MEDIA_TYPE, OCI_MANIFEST_MEDIA_TYPE, Reference},
    registry::{ParsedManifestDigests, metadata_store::MetadataStore, parse_manifest_digests},
    registry_client::{Error as ClientError, RegistryClient},
    replication::{Error, manifest_accept_types},
};

/// Upper bound on each downstream HTTP call inside the referrers-merge
/// critical section, kept well below the metadata executor lock's 300-second
/// max-hold lease (the tx-engine default) so a hung downstream cannot outlive
/// the lease and let a concurrent merge re-admit a lost update.
const REFERRERS_MERGE_HTTP_TIMEOUT: Duration = Duration::from_mins(1);

/// Pushes the OCI-1.0 referrers fallback tag index for a subject-bearing
/// manifest the downstream did not auto-index.
///
/// The fallback tag (`<alg>-<hash>` of the subject digest) holds a merged image
/// index of referrer descriptors; its PUT is deliberately timestamp-less, since
/// a set merge must never lose an LWW comparison and drop a descriptor.
pub async fn push_referrers_fallback(
    downstream: &RegistryClient,
    metadata_store: &Arc<MetadataStore>,
    namespace: &Namespace,
    downstream_namespace: &str,
    digest: &Digest,
    parsed: &ParsedManifestDigests,
    body: &[u8],
) -> Result<(), Error> {
    let Some(subject) = &parsed.subject else {
        return Ok(());
    };
    let (fallback_tag, location) =
        fallback_tag_and_location(downstream, downstream_namespace, subject)?;
    warn!(
        namespace = %namespace,
        %digest,
        %subject,
        fallback_tag,
        "Downstream did not index subject (OCI-1.0); merging referrers fallback index"
    );
    with_subject_lock(
        metadata_store,
        namespace,
        subject,
        &fallback_tag,
        async || merge_referrers_fallback(downstream, &location, digest, parsed, body).await,
    )
    .await
}

/// Drops `referrer`'s descriptor from the subject's OCI-1.0 referrers fallback
/// index, deleting the fallback tag when no referrers remain. Serialized under
/// the same per-subject lock as the push-side merge, so a concurrent add and
/// this removal cannot lose each other's update.
pub async fn remove_referrers_fallback(
    downstream: &RegistryClient,
    metadata_store: &Arc<MetadataStore>,
    namespace: &Namespace,
    downstream_namespace: &str,
    subject: &Digest,
    referrer: &Digest,
) -> Result<(), Error> {
    let (fallback_tag, location) =
        fallback_tag_and_location(downstream, downstream_namespace, subject)?;
    with_subject_lock(
        metadata_store,
        namespace,
        subject,
        &fallback_tag,
        async || prune_fallback_descriptor(downstream, &location, referrer).await,
    )
    .await
}

/// GETs the manifest at `digest` from the downstream and returns its subject
/// when it is a referrer. Best-effort: any failure (absent, unparseable, or no
/// subject) yields `None`, leaving the fallback index untouched.
pub async fn deleted_referrer_subject(
    downstream: &RegistryClient,
    downstream_namespace: &str,
    digest: &Digest,
) -> Option<Digest> {
    let location =
        downstream.get_manifest_path(downstream_namespace, &Reference::Digest(digest.clone()));
    let (_, _, body) = downstream
        .get_manifest(&manifest_accept_types(), &location)
        .await
        .ok()?;
    parse_manifest_digests(&body, None).ok()?.subject
}

/// The subject's fallback tag (`<alg>-<hash>`) and that tag's manifest
/// location on the downstream.
fn fallback_tag_and_location(
    downstream: &RegistryClient,
    downstream_namespace: &str,
    subject: &Digest,
) -> Result<(String, String), Error> {
    let fallback_tag = format!("{}-{}", subject.algorithm(), subject.hash());
    let reference = Reference::from_str(&fallback_tag).map_err(|e| {
        Error::Internal(format!(
            "invalid referrers fallback tag '{fallback_tag}': {e}"
        ))
    })?;
    let location = downstream.get_manifest_path(downstream_namespace, &reference);
    Ok((fallback_tag, location))
}

/// Runs `critical` (the GET/modify/PUT of one fallback index) under the
/// subject's merge lock: two referrers of the same subject are distinct jobs
/// the queue runs concurrently, and unserialized merges read the same base
/// index and drop the loser's descriptor. The lock lives on the metadata
/// executor, which every drain of this store shares, but cannot cover an
/// unrelated sender registry pushing to the same downstream.
///
/// The key is deliberately downstream-agnostic: the critical section is two
/// short HTTP calls, so cross-downstream contention never matters in practice.
async fn with_subject_lock(
    metadata_store: &Arc<MetadataStore>,
    namespace: &Namespace,
    subject: &Digest,
    fallback_tag: &str,
    critical: impl AsyncFnOnce() -> Result<(), Error>,
) -> Result<(), Error> {
    let lock_keys = [format!("replication-referrers:{namespace}:{subject}")];
    let session = metadata_store
        .executor()
        .acquire(&lock_keys)
        .await
        .map_err(|e| {
            Error::Internal(format!(
                "referrers fallback lock acquire failed for '{fallback_tag}': {e}"
            ))
        })?;
    let result = critical().await;
    session.release().await;
    result
}

/// The locked critical section of [`push_referrers_fallback`]: fetches the
/// downstream's current fallback index, appends this referrer's descriptor when
/// absent, and PUTs the merged index back.
async fn merge_referrers_fallback(
    downstream: &RegistryClient,
    location: &str,
    digest: &Digest,
    parsed: &ParsedManifestDigests,
    body: &[u8],
) -> Result<(), Error> {
    let mut manifests = fetch_fallback_manifests(downstream, location).await?;

    // The blob store is content-addressed, so `digest` is already the body's
    // digest; no re-hash or re-parse needed.
    let descriptor = referrer_descriptor(
        digest,
        body.len(),
        parsed.media_type.as_deref(),
        parsed.artifact_type.as_deref(),
    );

    // Dedup by digest so a re-run is idempotent.
    let digest_str = digest.to_string();
    let already_present = manifests
        .iter()
        .any(|m| m.get("digest").and_then(Value::as_str) == Some(digest_str.as_str()));
    if !already_present {
        manifests.push(descriptor);
    }

    put_fallback_manifests(downstream, location, manifests).await
}

/// Locked critical section of [`remove_referrers_fallback`]: GET the index, drop
/// the descriptor, then PUT the remainder back, or DELETE the tag when empty.
async fn prune_fallback_descriptor(
    downstream: &RegistryClient,
    location: &str,
    referrer: &Digest,
) -> Result<(), Error> {
    let mut manifests = fetch_fallback_manifests(downstream, location).await?;

    let referrer_str = referrer.to_string();
    let before = manifests.len();
    manifests.retain(|m| m.get("digest").and_then(Value::as_str) != Some(referrer_str.as_str()));
    // Descriptor absent (already pruned, or a 1.1 downstream has no fallback tag
    // and the GET returned an empty base): nothing to do.
    if manifests.len() == before {
        return Ok(());
    }

    if manifests.is_empty() {
        // No referrers remain: drop the fallback tag rather than leave an empty
        // index. Timestamp-less, mirroring the merge PUT.
        timeout(
            REFERRERS_MERGE_HTTP_TIMEOUT,
            downstream.delete_manifest(location, None),
        )
        .await
        .map_err(|_| {
            Error::Internal(format!(
                "referrers fallback DELETE at '{location}' timed out inside the merge lock"
            ))
        })??;
        return Ok(());
    }

    put_fallback_manifests(downstream, location, manifests).await
}

/// GETs the existing referrers fallback index at `location`, bounded by the
/// merge-lock HTTP timeout, and returns its `manifests[]` descriptors.
///
/// Only a `404` yields an empty base; any other error, including a `200` body
/// that is not a parseable image index, propagates so the caller never rebuilds
/// the index from an empty base and drops the subject's sibling referrers.
async fn fetch_fallback_manifests(
    downstream: &RegistryClient,
    location: &str,
) -> Result<Vec<Value>, Error> {
    let accept = manifest_accept_types();
    let get = downstream.get_manifest(&accept, location);
    let body = match timeout(REFERRERS_MERGE_HTTP_TIMEOUT, get).await {
        Ok(Ok((_, _, body))) => body,
        Ok(Err(ClientError::ManifestUnknown)) => return Ok(Vec::new()),
        Ok(Err(e)) => return Err(Error::Client(e)),
        Err(_) => {
            return Err(Error::Internal(format!(
                "referrers fallback GET at '{location}' timed out inside the merge lock"
            )));
        }
    };
    serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| v.get("manifests").and_then(Value::as_array).cloned())
        .ok_or_else(|| {
            Error::Internal(format!(
                "downstream referrers fallback index at '{location}' is not a parseable image \
                 index (missing or non-array `manifests`); refusing to overwrite it"
            ))
        })
}

/// Serializes `manifests` into an image index and PUTs it to `location`.
///
/// Timestamp-less: the receiver then skips LWW, so the merged index can never
/// come back superseded and silently drop a descriptor. Bounded by the
/// merge-lock HTTP timeout.
async fn put_fallback_manifests(
    downstream: &RegistryClient,
    location: &str,
    manifests: Vec<Value>,
) -> Result<(), Error> {
    let index = json!({
        "schemaVersion": 2,
        "mediaType": OCI_INDEX_MEDIA_TYPE,
        "manifests": manifests,
    });
    let index_body = serde_json::to_vec(&index).map_err(|e| {
        Error::Internal(format!("failed to serialize referrers fallback index: {e}"))
    })?;
    timeout(
        REFERRERS_MERGE_HTTP_TIMEOUT,
        downstream.put_manifest(location, Some(OCI_INDEX_MEDIA_TYPE), index_body, None),
    )
    .await
    .map_err(|_| {
        Error::Internal(format!(
            "referrers fallback PUT at '{location}' timed out inside the merge lock"
        ))
    })??;
    Ok(())
}

/// Builds the OCI descriptor for a referrer manifest to embed in the fallback
/// index; `media_type` defaults to the OCI image-manifest type when the body
/// declared none.
fn referrer_descriptor(
    referrer_digest: &Digest,
    size: usize,
    media_type: Option<&str>,
    artifact_type: Option<&str>,
) -> Value {
    let mut descriptor = json!({
        "mediaType": media_type.unwrap_or(OCI_MANIFEST_MEDIA_TYPE),
        "digest": referrer_digest.to_string(),
        "size": size,
    });
    if let Some(artifact_type) = artifact_type {
        descriptor["artifactType"] = json!(artifact_type);
    }
    descriptor
}
