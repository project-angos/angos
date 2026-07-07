use tracing::warn;

use angos_tx_engine::StorageError;

use crate::{
    oci::{Descriptor, Digest, Manifest, Namespace},
    registry::{
        metadata_store::{LinkKind, MetadataStore},
        path_builder,
    },
};

impl MetadataStore {
    /// Resolves a single referrer entry to an OCI [`Descriptor`], applying an
    /// optional `artifact_type` filter: returns the cached link descriptor
    /// when that suffices, else falls back to reading and parsing the
    /// manifest blob.
    pub async fn resolve_referrer_descriptor(
        &self,
        namespace: &Namespace,
        subject_digest: &Digest,
        manifest_digest: Digest,
        artifact_type: Option<&String>,
    ) -> Option<Descriptor> {
        let referrer_link = LinkKind::Referrer(subject_digest.clone(), manifest_digest.clone());

        if let Ok(metadata) = self.read_link_reference(namespace, &referrer_link).await
            && let Some(desc) = metadata.descriptor
        {
            match artifact_type {
                Some(at) if desc.artifact_type.as_deref() == Some(at.as_str()) => {
                    return Some(desc);
                }
                None => return Some(desc),
                // Cached descriptor has no artifact_type; fall through to manifest
                // read so the filter can be evaluated against the full manifest data.
                Some(_) if desc.artifact_type.is_none() => {}
                Some(_) => return None,
            }
        }

        let blob_path = path_builder::blob_path(&manifest_digest);
        match self.store().get(&blob_path).await {
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
            Err(StorageError::NotFound) => {
                warn!("Referrer blob not found at {blob_path}, skipping");
                None
            }
            Err(e) => {
                warn!("Failed to read referrer blob at {blob_path}: {e}");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{
        oci::{Descriptor, Manifest},
        registry::{
            metadata_store::LinkOperation,
            test_utils::{FsTestStack, fs_test_stack, media_type, put_blob_direct},
        },
    };

    // A 64-char lowercase hex string for a digest with no stored blob.
    const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn subject() -> Digest {
        Digest::sha256("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
    }

    fn namespace() -> Namespace {
        Namespace::new("test/repo").unwrap()
    }

    fn descriptor_with(artifact_type: Option<&str>, manifest_digest: &Digest) -> Descriptor {
        Descriptor {
            media_type: media_type("application/vnd.oci.image.manifest.v1+json"),
            digest: manifest_digest.clone(),
            size: 100,
            annotations: HashMap::new(),
            artifact_type: artifact_type.map(media_type),
            platform: None,
        }
    }

    fn manifest_bytes(artifact_type: Option<&str>) -> Vec<u8> {
        let manifest = Manifest {
            schema_version: 2,
            media_type: Some(media_type("application/vnd.oci.image.manifest.v1+json")),
            artifact_type: artifact_type.map(media_type),
            ..Manifest::default()
        };
        serde_json::to_vec(&manifest).expect("serialization must succeed")
    }

    /// Store fixture plus one referrer manifest blob whose bytes carry
    /// `blob_artifact_type`; the returned digest addresses that blob.
    async fn stack_with_blob(blob_artifact_type: Option<&str>) -> (FsTestStack, Digest) {
        let stack = fs_test_stack();
        let digest = put_blob_direct(
            stack.metadata_store.store(),
            &manifest_bytes(blob_artifact_type),
        )
        .await;
        (stack, digest)
    }

    /// Creates the referrer link `subject() -> manifest`, optionally carrying
    /// a cached descriptor.
    async fn create_referrer_link(
        m: &MetadataStore,
        manifest: &Digest,
        descriptor: Option<Descriptor>,
    ) {
        let ops = vec![LinkOperation::Create {
            link: LinkKind::Referrer(subject(), manifest.clone()),
            target: manifest.clone(),
            referrer: None,
            media_type: None,
            descriptor: descriptor.map(Box::new),
        }];
        m.update_links(&namespace(), &ops).await.unwrap();
    }

    #[tokio::test]
    async fn returns_cached_descriptor_when_no_filter() {
        // The blob is deliberately unparseable: the cached descriptor must
        // answer without any manifest read.
        let stack = fs_test_stack();
        let m = stack.metadata_store.as_ref();
        let manifest_digest = put_blob_direct(m.store(), b"not json").await;
        let desc = descriptor_with(Some("application/vnd.foo"), &manifest_digest);
        create_referrer_link(m, &manifest_digest, Some(desc.clone())).await;

        let result = m
            .resolve_referrer_descriptor(&namespace(), &subject(), manifest_digest, None)
            .await;
        assert_eq!(result, Some(desc));
    }

    #[tokio::test]
    async fn returns_cached_descriptor_when_filter_matches() {
        let stack = fs_test_stack();
        let m = stack.metadata_store.as_ref();
        let manifest_digest = put_blob_direct(m.store(), b"not json").await;
        let at = "application/vnd.foo".to_string();
        let desc = descriptor_with(Some(&at), &manifest_digest);
        create_referrer_link(m, &manifest_digest, Some(desc.clone())).await;

        let result = m
            .resolve_referrer_descriptor(&namespace(), &subject(), manifest_digest, Some(&at))
            .await;
        assert_eq!(result, Some(desc));
    }

    #[tokio::test]
    async fn returns_none_when_cached_descriptor_filter_mismatches() {
        // The stored manifest DOES match the filter: a wrong fall-through to
        // the blob would return Some, so this pins the cache-only decision.
        let (stack, manifest_digest) = stack_with_blob(Some("application/vnd.bar")).await;
        let m = stack.metadata_store.as_ref();
        let desc = descriptor_with(Some("application/vnd.foo"), &manifest_digest);
        create_referrer_link(m, &manifest_digest, Some(desc)).await;

        let filter = "application/vnd.bar".to_string();
        let result = m
            .resolve_referrer_descriptor(&namespace(), &subject(), manifest_digest, Some(&filter))
            .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn falls_through_to_blob_when_cached_descriptor_missing_artifact_type() {
        // Cached descriptor has artifact_type = None; filter is Some(...).
        // The filter cannot be evaluated from the cache entry alone, so the
        // manifest blob decides.
        let (stack, manifest_digest) = stack_with_blob(Some("application/vnd.foo")).await;
        let m = stack.metadata_store.as_ref();
        let desc = descriptor_with(None, &manifest_digest);
        create_referrer_link(m, &manifest_digest, Some(desc)).await;

        let at = "application/vnd.foo".to_string();
        let result = m
            .resolve_referrer_descriptor(&namespace(), &subject(), manifest_digest, Some(&at))
            .await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn falls_through_to_blob_when_link_is_absent() {
        let (stack, manifest_digest) = stack_with_blob(None).await;
        let m = stack.metadata_store.as_ref();

        let result = m
            .resolve_referrer_descriptor(&namespace(), &subject(), manifest_digest, None)
            .await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn falls_through_to_blob_when_link_carries_no_descriptor() {
        let (stack, manifest_digest) = stack_with_blob(None).await;
        let m = stack.metadata_store.as_ref();
        create_referrer_link(m, &manifest_digest, None).await;

        let result = m
            .resolve_referrer_descriptor(&namespace(), &subject(), manifest_digest, None)
            .await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn returns_blob_descriptor_when_blob_filter_matches() {
        let (stack, manifest_digest) = stack_with_blob(Some("application/vnd.foo")).await;
        let m = stack.metadata_store.as_ref();

        let at = "application/vnd.foo".to_string();
        let result = m
            .resolve_referrer_descriptor(&namespace(), &subject(), manifest_digest, Some(&at))
            .await;
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().artifact_type.as_deref(),
            Some("application/vnd.foo")
        );
    }

    #[tokio::test]
    async fn returns_none_when_blob_filter_mismatches() {
        let (stack, manifest_digest) = stack_with_blob(Some("application/vnd.foo")).await;
        let m = stack.metadata_store.as_ref();

        let filter = "application/vnd.bar".to_string();
        let result = m
            .resolve_referrer_descriptor(&namespace(), &subject(), manifest_digest, Some(&filter))
            .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn returns_none_when_blob_not_found() {
        let stack = fs_test_stack();
        let m = stack.metadata_store.as_ref();

        let result = m
            .resolve_referrer_descriptor(
                &namespace(),
                &subject(),
                Digest::sha256(HASH_B).unwrap(),
                None,
            )
            .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn returns_none_when_blob_is_invalid_manifest_json() {
        let stack = fs_test_stack();
        let m = stack.metadata_store.as_ref();
        let manifest_digest = put_blob_direct(m.store(), b"not json").await;

        let result = m
            .resolve_referrer_descriptor(&namespace(), &subject(), manifest_digest, None)
            .await;
        assert!(result.is_none());
    }
}
