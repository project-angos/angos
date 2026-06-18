use std::{future::Future, io::ErrorKind};

use tracing::warn;

use crate::{
    oci::{Descriptor, Digest, Manifest},
    registry::{
        metadata_store::{Error, LinkKind, LinkMetadata},
        path_builder,
    },
};

/// Resolves a single referrer entry to an [`OCI Descriptor`], applying an
/// optional `artifact_type` filter: returns the cached link descriptor when that
/// suffices, else falls back to reading and parsing the manifest blob.
/// `read_link` and `read_blob` are caller-supplied closures so the function
/// stays backend-agnostic without a new trait.
pub async fn resolve_referrer_descriptor<L, LFut, B, BFut>(
    subject_digest: &Digest,
    manifest_digest: Digest,
    artifact_type: Option<&String>,
    read_link: L,
    read_blob: B,
) -> Option<Descriptor>
where
    L: FnOnce(LinkKind) -> LFut,
    LFut: Future<Output = Result<LinkMetadata, Error>>,
    B: FnOnce(String) -> BFut,
    BFut: Future<Output = Result<Vec<u8>, std::io::Error>>,
{
    let referrer_link = LinkKind::Referrer(subject_digest.clone(), manifest_digest.clone());

    if let Ok(metadata) = read_link(referrer_link).await
        && let Some(desc) = metadata.descriptor
    {
        match artifact_type {
            Some(at) if desc.artifact_type.as_ref() == Some(at) => return Some(desc),
            None => return Some(desc),
            // Cached descriptor has no artifact_type; fall through to manifest
            // read so the filter can be evaluated against the full manifest data.
            Some(_) if desc.artifact_type.is_none() => {}
            Some(_) => return None,
        }
    }

    let blob_path = path_builder::blob_path(&manifest_digest);
    match read_blob(blob_path.clone()).await {
        Ok(data) => {
            let manifest_len = data.len();
            match Manifest::from_slice(&data) {
                Ok(mut manifest) => {
                    if !manifest.artifact_type_matches(artifact_type) {
                        return None;
                    }
                    manifest.take_descriptor(manifest_digest, manifest_len as u64)
                }
                Err(e) => {
                    warn!("Failed to parse manifest at {blob_path}: {e}");
                    None
                }
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            warn!("Referrer blob not found at {blob_path}, skipping");
            None
        }
        Err(e) => {
            warn!("Failed to read referrer blob at {blob_path}: {e}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io};

    use super::*;
    use crate::oci::{Descriptor, Manifest};

    // Two distinct 64-char lowercase hex strings for use as digest hashes.
    const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn digest(hex: &str) -> Digest {
        Digest::sha256(hex).unwrap()
    }

    fn descriptor_with(artifact_type: Option<&str>) -> Descriptor {
        Descriptor {
            media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
            digest: digest(HASH_A),
            size: 100,
            annotations: HashMap::new(),
            artifact_type: artifact_type.map(str::to_owned),
            platform: None,
        }
    }

    fn manifest_bytes(artifact_type: Option<&str>) -> Vec<u8> {
        let manifest = Manifest {
            schema_version: 2,
            media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
            artifact_type: artifact_type.map(str::to_owned),
            ..Manifest::default()
        };
        serde_json::to_vec(&manifest).expect("serialization must succeed")
    }

    fn link_err() -> impl FnOnce(LinkKind) -> std::future::Ready<Result<LinkMetadata, Error>> {
        |_| std::future::ready(Err(Error::ReferenceNotFound))
    }

    fn link_ok(
        desc: Option<Descriptor>,
    ) -> impl FnOnce(LinkKind) -> std::future::Ready<Result<LinkMetadata, Error>> {
        move |_| {
            let meta = LinkMetadata::from_digest(digest(HASH_A)).with_descriptor(desc);
            std::future::ready(Ok(meta))
        }
    }

    fn blob_ok(
        bytes: Vec<u8>,
    ) -> impl FnOnce(String) -> std::future::Ready<Result<Vec<u8>, io::Error>> {
        move |_| std::future::ready(Ok(bytes))
    }

    fn blob_err(
        kind: io::ErrorKind,
    ) -> impl FnOnce(String) -> std::future::Ready<Result<Vec<u8>, io::Error>> {
        move |_| std::future::ready(Err(io::Error::new(kind, "test error")))
    }

    #[tokio::test]
    async fn returns_cached_descriptor_when_no_filter() {
        let desc = descriptor_with(Some("application/vnd.foo"));
        let result = resolve_referrer_descriptor(
            &digest(HASH_A),
            digest(HASH_B),
            None,
            link_ok(Some(desc.clone())),
            blob_err(io::ErrorKind::Other),
        )
        .await;
        assert_eq!(result, Some(desc));
    }

    #[tokio::test]
    async fn returns_cached_descriptor_when_filter_matches() {
        let at = "application/vnd.foo".to_string();
        let desc = descriptor_with(Some(&at));
        let result = resolve_referrer_descriptor(
            &digest(HASH_A),
            digest(HASH_B),
            Some(&at),
            link_ok(Some(desc.clone())),
            blob_err(io::ErrorKind::Other),
        )
        .await;
        assert_eq!(result, Some(desc));
    }

    #[tokio::test]
    async fn returns_none_when_cached_descriptor_filter_mismatches() {
        let desc = descriptor_with(Some("application/vnd.foo"));
        let filter = "application/vnd.bar".to_string();
        let result = resolve_referrer_descriptor(
            &digest(HASH_A),
            digest(HASH_B),
            Some(&filter),
            link_ok(Some(desc)),
            blob_err(io::ErrorKind::Other),
        )
        .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn falls_through_to_blob_when_cached_descriptor_missing_artifact_type() {
        // Cached descriptor has artifact_type = None; filter is Some(...).
        // The function cannot evaluate the filter from the cache entry alone, so
        // it falls through to the manifest blob.
        let desc = descriptor_with(None);
        let at = "application/vnd.foo".to_string();
        let result = resolve_referrer_descriptor(
            &digest(HASH_A),
            digest(HASH_B),
            Some(&at),
            link_ok(Some(desc)),
            blob_ok(manifest_bytes(Some("application/vnd.foo"))),
        )
        .await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn falls_through_to_blob_when_link_read_fails() {
        let result = resolve_referrer_descriptor(
            &digest(HASH_A),
            digest(HASH_B),
            None,
            link_err(),
            blob_ok(manifest_bytes(None)),
        )
        .await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn falls_through_to_blob_when_link_metadata_descriptor_is_none() {
        let result = resolve_referrer_descriptor(
            &digest(HASH_A),
            digest(HASH_B),
            None,
            link_ok(None),
            blob_ok(manifest_bytes(None)),
        )
        .await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn returns_blob_descriptor_when_blob_filter_matches() {
        let at = "application/vnd.foo".to_string();
        let result = resolve_referrer_descriptor(
            &digest(HASH_A),
            digest(HASH_B),
            Some(&at),
            link_err(),
            blob_ok(manifest_bytes(Some("application/vnd.foo"))),
        )
        .await;
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().artifact_type.as_deref(),
            Some("application/vnd.foo")
        );
    }

    #[tokio::test]
    async fn returns_none_when_blob_filter_mismatches() {
        let filter = "application/vnd.bar".to_string();
        let result = resolve_referrer_descriptor(
            &digest(HASH_A),
            digest(HASH_B),
            Some(&filter),
            link_err(),
            blob_ok(manifest_bytes(Some("application/vnd.foo"))),
        )
        .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn returns_none_when_blob_not_found() {
        let result = resolve_referrer_descriptor(
            &digest(HASH_A),
            digest(HASH_B),
            None,
            link_err(),
            blob_err(io::ErrorKind::NotFound),
        )
        .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn returns_none_when_blob_io_error() {
        let result = resolve_referrer_descriptor(
            &digest(HASH_A),
            digest(HASH_B),
            None,
            link_err(),
            blob_err(io::ErrorKind::PermissionDenied),
        )
        .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn returns_none_when_blob_is_invalid_manifest_json() {
        let result = resolve_referrer_descriptor(
            &digest(HASH_A),
            digest(HASH_B),
            None,
            link_err(),
            blob_ok(b"not json".to_vec()),
        )
        .await;
        assert!(result.is_none());
    }
}
