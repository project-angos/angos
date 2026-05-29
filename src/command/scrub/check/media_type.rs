use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{Stream, StreamExt};
use tracing::{debug, error, warn};

use crate::{
    command::scrub::{
        action::Action,
        check::{NamespaceChecker, list_all},
        error::Error,
        executor::ActionSink,
    },
    oci::Manifest,
    registry::{
        blob_store,
        metadata_store::{MetadataStore, link_kind::LinkKind},
    },
};

pub struct MediaTypeChecker {
    blob_store: Arc<blob_store::BlobStore>,
    metadata_store: Arc<MetadataStore>,
}

impl MediaTypeChecker {
    pub fn new(blob_store: Arc<blob_store::BlobStore>, metadata_store: Arc<MetadataStore>) -> Self {
        Self {
            blob_store,
            metadata_store,
        }
    }

    async fn backfill_link(
        &self,
        namespace: &str,
        link: &LinkKind,
        display_name: &str,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        let metadata = self
            .metadata_store
            .read_link(namespace, link, false)
            .await?;

        if metadata.media_type.is_some() {
            debug!("{display_name} already has media_type, skipping");
            return Ok(());
        }

        let content = match self.blob_store.read(&metadata.target).await {
            Ok(content) => content,
            Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
                warn!(
                    "Manifest blob missing for {display_name} ({}); removing revision link",
                    metadata.target
                );
                return sink
                    .apply(Action::DeleteOrphanManifest {
                        namespace: namespace.to_string(),
                        digest: metadata.target,
                    })
                    .await;
            }
            Err(e) => return Err(e.into()),
        };

        let media_type = match serde_json::from_slice::<Manifest>(&content) {
            Ok(manifest) => manifest.media_type,
            Err(e) => {
                warn!(
                    "Failed to deserialize manifest for {}: {e}",
                    metadata.target
                );
                None
            }
        };

        let Some(media_type) = media_type else {
            debug!("{display_name} has no media_type in manifest, skipping");
            return Ok(());
        };

        sink.apply(Action::SetMediaType {
            namespace: namespace.to_string(),
            link: link.clone(),
            target: metadata.target,
            media_type,
            display_name: display_name.to_string(),
        })
        .await
    }

    async fn backfill_all<T, S, F>(
        &self,
        namespace: &str,
        item_kind: &str,
        mut items: S,
        to_link_and_name: F,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error>
    where
        T: std::fmt::Display,
        S: Stream<Item = Result<T, Error>> + Unpin,
        F: Fn(&T) -> (LinkKind, String),
    {
        while let Some(item) = items.next().await {
            let item = item?;
            let (link, display_name) = to_link_and_name(&item);
            if let Err(e) = self
                .backfill_link(namespace, &link, &display_name, sink)
                .await
            {
                error!(
                    "Failed to backfill media_type for '{namespace}' ({item_kind} '{item}'): {e}"
                );
            }
        }
        Ok(())
    }
}

#[async_trait]
impl NamespaceChecker for MediaTypeChecker {
    async fn check(
        &self,
        namespace: &str,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        debug!("Checking media_type field for namespace '{namespace}'");

        let revisions = list_all::revisions(&self.metadata_store, namespace);
        self.backfill_all(
            namespace,
            "revision",
            revisions,
            |d| (LinkKind::Digest(d.clone()), format!("revision {d}")),
            sink,
        )
        .await?;

        let tags = list_all::tags(&self.metadata_store, namespace);
        self.backfill_all(
            namespace,
            "tag",
            tags,
            |t| (LinkKind::Tag(t.clone()), format!("tag '{t}'")),
            sink,
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        command::scrub::{action::Action, executor::Executor},
        oci::Namespace,
        registry::{
            metadata_store::LinkOperation,
            test_utils::{self, backends, put_blob_direct},
        },
    };

    #[tokio::test]
    async fn test_media_type_checker_backfills_missing_media_type() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/app").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let media_type = "application/vnd.oci.image.manifest.v1+json";

            let (config_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"config content").await;

            let (layer_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"layer content").await;

            let manifest_content = format!(
                r#"{{
                "schemaVersion": 2,
                "mediaType": "{media_type}",
                "config": {{
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": "{config_digest}",
                    "size": 123
                }},
                "layers": [
                    {{
                        "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                        "digest": "{layer_digest}",
                        "size": 456
                    }}
                ]
            }}"#
            );

            let manifest_digest =
                put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;

            metadata_store
                .update_links(
                    namespace,
                    &[
                        LinkOperation::create(
                            LinkKind::Digest(manifest_digest.clone()),
                            manifest_digest.clone(),
                        ),
                        LinkOperation::create(
                            LinkKind::Tag("latest".to_string()),
                            manifest_digest.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false)
                .await
                .unwrap();
            assert!(digest_link.media_type.is_none());

            let mut executor = Executor::new(blob_store.clone(), metadata_store.clone());

            let checker = MediaTypeChecker::new(blob_store.clone(), metadata_store.clone());
            checker.check(namespace, &mut executor).await.unwrap();

            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false)
                .await
                .unwrap();
            assert_eq!(digest_link.media_type.as_deref(), Some(media_type));

            let tag_link = metadata_store
                .read_link(namespace, &LinkKind::Tag("latest".to_string()), false)
                .await
                .unwrap();
            assert_eq!(tag_link.media_type.as_deref(), Some(media_type));
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_media_type_checker_skips_links_with_media_type() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/skip").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let media_type = "application/vnd.oci.image.manifest.v1+json";

            let (config_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"config content").await;

            let manifest_content = format!(
                r#"{{
                "schemaVersion": 2,
                "mediaType": "{media_type}",
                "config": {{
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": "{config_digest}",
                    "size": 123
                }},
                "layers": []
            }}"#
            );

            let manifest_digest =
                put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;

            metadata_store
                .update_links(
                    namespace,
                    &[
                        LinkOperation::create_with_media_type(
                            LinkKind::Digest(manifest_digest.clone()),
                            manifest_digest.clone(),
                            Some(media_type.to_string()),
                        ),
                        LinkOperation::create_with_media_type(
                            LinkKind::Tag("latest".to_string()),
                            manifest_digest.clone(),
                            Some(media_type.to_string()),
                        ),
                    ],
                )
                .await
                .unwrap();

            let checker = MediaTypeChecker::new(blob_store.clone(), metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert!(
                sink.is_empty(),
                "No actions expected when media_type already set"
            );

            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false)
                .await
                .unwrap();
            assert_eq!(digest_link.media_type.as_deref(), Some(media_type));
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_media_type_checker_dry_run() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/dry-run").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let media_type = "application/vnd.oci.image.manifest.v1+json";

            let (config_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"config content").await;

            let manifest_content = format!(
                r#"{{
                "schemaVersion": 2,
                "mediaType": "{media_type}",
                "config": {{
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": "{config_digest}",
                    "size": 123
                }},
                "layers": []
            }}"#
            );

            let manifest_digest =
                put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;

            metadata_store
                .update_links(
                    namespace,
                    &[
                        LinkOperation::create(
                            LinkKind::Digest(manifest_digest.clone()),
                            manifest_digest.clone(),
                        ),
                        LinkOperation::create(
                            LinkKind::Tag("latest".to_string()),
                            manifest_digest.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            let checker = MediaTypeChecker::new(blob_store.clone(), metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            // Vec sink does not apply actions, so media_type remains None
            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false)
                .await
                .unwrap();
            assert!(
                digest_link.media_type.is_none(),
                "Vec sink must not write: media_type should remain None"
            );

            assert!(
                sink.iter()
                    .any(|a| matches!(a, Action::SetMediaType { .. })),
                "Vec sink must capture SetMediaType actions"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn media_type_checker_emits_delete_orphan_manifest_when_revision_blob_missing() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/mt-missing-rev").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let media_type = "application/vnd.oci.image.manifest.v1+json";

            let (config_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"config for mt-missing-rev")
                    .await;

            let manifest_content = format!(
                r#"{{
                "schemaVersion": 2,
                "mediaType": "{media_type}",
                "config": {{
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": "{config_digest}",
                    "size": 123
                }},
                "layers": []
            }}"#
            );

            let manifest_digest =
                put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(manifest_digest.clone()),
                        manifest_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            blob_store.delete_blob(&manifest_digest).await.unwrap();

            let checker = MediaTypeChecker::new(blob_store.clone(), metadata_store.clone());
            let mut executor = Executor::new(blob_store.clone(), metadata_store.clone());
            checker.check(namespace, &mut executor).await.unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false,)
                    .await
                    .is_err(),
                "revision link must be removed when manifest blob is missing"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn media_type_checker_emits_delete_orphan_manifest_when_tag_target_blob_missing() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/mt-missing-tag").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let media_type = "application/vnd.oci.image.manifest.v1+json";

            let (config_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"config for mt-missing-tag")
                    .await;

            let manifest_content = format!(
                r#"{{
                "schemaVersion": 2,
                "mediaType": "{media_type}",
                "config": {{
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": "{config_digest}",
                    "size": 123
                }},
                "layers": []
            }}"#
            );

            let manifest_digest =
                put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;

            metadata_store
                .update_links(
                    namespace,
                    &[
                        LinkOperation::create(
                            LinkKind::Digest(manifest_digest.clone()),
                            manifest_digest.clone(),
                        ),
                        LinkOperation::create(
                            LinkKind::Tag("dangling-mt".to_string()),
                            manifest_digest.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            blob_store.delete_blob(&manifest_digest).await.unwrap();

            let checker = MediaTypeChecker::new(blob_store.clone(), metadata_store.clone());
            let mut executor = Executor::new(blob_store.clone(), metadata_store.clone());
            checker.check(namespace, &mut executor).await.unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false,)
                    .await
                    .is_err(),
                "digest revision link must be removed when manifest blob is missing"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Tag("dangling-mt".to_string()), false,)
                    .await
                    .is_err(),
                "tag link must be removed when target manifest blob is missing"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn media_type_checker_dry_run_captures_delete_orphan_manifest() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/mt-dry-missing").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let media_type = "application/vnd.oci.image.manifest.v1+json";

            let (config_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"config for mt-dry-missing")
                    .await;

            let manifest_content = format!(
                r#"{{
                "schemaVersion": 2,
                "mediaType": "{media_type}",
                "config": {{
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": "{config_digest}",
                    "size": 123
                }},
                "layers": []
            }}"#
            );

            let manifest_digest =
                put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(manifest_digest.clone()),
                        manifest_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            blob_store.delete_blob(&manifest_digest).await.unwrap();

            let checker = MediaTypeChecker::new(blob_store.clone(), metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert!(
                sink.iter().any(|a| matches!(
                    a,
                    Action::DeleteOrphanManifest { digest, .. } if *digest == manifest_digest
                )),
                "Vec sink must capture DeleteOrphanManifest action"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false,)
                    .await
                    .is_ok(),
                "revision link must not be touched under Vec sink"
            );
            test_case.cleanup().await;
        }
    }
}
