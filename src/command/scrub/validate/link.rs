//! Link-key validation: one visit per link file replaces the old per-concern
//! walks (manifest link repair, `referenced_by` back-links, blob-index grant
//! reconcile, tag digest links, referrer liveness, the invalid-tag gate, and
//! the orphan-namespace clearing).

use tracing::warn;

use angos_tx_engine::StorageError;

use crate::{
    command::scrub::{
        action::{Action, WalkedStore},
        categorize::ParsedLink,
        error::Error,
        validate::Validator,
    },
    oci::{Digest, Namespace, Tag},
    registry::{
        Error as RegistryError,
        metadata_store::{LinkKind, LinkMetadata},
        parse_manifest_digests,
    },
};

impl Validator {
    /// Validate one link file in `namespace_raw`.
    pub async fn validate_link(
        &self,
        key: &str,
        namespace_raw: &str,
        link: ParsedLink,
    ) -> Result<(), Error> {
        let Ok(namespace) = Namespace::new(namespace_raw) else {
            return self.handle_invalid_namespace(namespace_raw).await;
        };

        match link {
            ParsedLink::Tag { name } => self.validate_tag_link(key, &namespace, &name).await,
            ParsedLink::Revision(digest) => {
                self.validate_revision_link(key, &namespace, &digest).await
            }
            ParsedLink::Referrer { subject, referrer } => {
                self.validate_referrer_link(key, &namespace, &subject, &referrer)
                    .await
            }
            ParsedLink::Blob(digest) => {
                self.validate_tracked_link(key, &namespace, LinkKind::Blob(digest))
                    .await
            }
            ParsedLink::Layer(digest) => {
                self.validate_tracked_link(key, &namespace, LinkKind::Layer(digest))
                    .await
            }
            ParsedLink::Config(digest) => {
                self.validate_tracked_link(key, &namespace, LinkKind::Config(digest))
                    .await
            }
            ParsedLink::ManifestIndex { index, child } => {
                self.validate_tracked_link(key, &namespace, LinkKind::Manifest(index, child))
                    .await
            }
        }
    }

    /// A raw namespace directory whose name fails validation can never be
    /// addressed by any angos API; reclaim it by prefix (deduped per name).
    async fn handle_invalid_namespace(&self, namespace_raw: &str) -> Result<(), Error> {
        if !self.claim(format!("invalid-ns:{namespace_raw}")) {
            return Ok(());
        }
        warn!("scrub: reclaiming invalid namespace directory '{namespace_raw}'");
        self.emit(Action::DeleteInvalidNamespace {
            name: namespace_raw.to_string(),
        })
        .await
    }

    /// A tag link: an invalid directory name is deleted; a valid one must
    /// parse, target existing blob bytes, and have its digest revision link.
    async fn validate_tag_link(
        &self,
        key: &str,
        namespace: &Namespace,
        name: &str,
    ) -> Result<(), Error> {
        let Ok(tag) = Tag::new(name) else {
            return self
                .emit(Action::DeleteInvalidTag {
                    namespace: namespace.clone(),
                    tag: name.to_string(),
                })
                .await;
        };
        let Some(metadata) = self.read_link_body(key).await? else {
            return Ok(());
        };
        let target = metadata.target;

        match self.blob_store.size(&target).await {
            Ok(_) => {
                self.ensure_link(namespace, &LinkKind::Digest(target.clone()), &target)
                    .await
            }
            Err(RegistryError::BlobUnknown | RegistryError::NotFound) => {
                warn!("scrub: tag '{namespace}:{tag}' targets missing blob '{target}'; removing");
                self.emit(Action::DeleteOrphanManifest {
                    namespace: namespace.clone(),
                    digest: target,
                })
                .await
            }
            Err(e) => Err(e.into()),
        }
    }

    /// A manifest revision link: the anchor of the derivable state. One
    /// manifest read drives child-link repair, `referenced_by` back-links,
    /// and blob-index grant reconciliation.
    async fn validate_revision_link(
        &self,
        key: &str,
        namespace: &Namespace,
        revision: &Digest,
    ) -> Result<(), Error> {
        let Some(metadata) = self.read_link_body(key).await? else {
            return Ok(());
        };
        if metadata.target != *revision {
            // The body targets a different digest than the path addresses;
            // rewrite it to the canonical self-target.
            self.emit(Action::RecreateLink {
                namespace: namespace.clone(),
                link: LinkKind::Digest(revision.clone()),
                target: revision.clone(),
            })
            .await?;
        }

        let content = match self.blob_store.read(revision).await {
            Ok(content) => content,
            Err(RegistryError::BlobUnknown | RegistryError::NotFound) => {
                warn!(
                    "scrub: manifest blob missing for revision '{namespace}@{revision}'; removing revision"
                );
                return self
                    .emit(Action::DeleteOrphanManifest {
                        namespace: namespace.clone(),
                        digest: revision.clone(),
                    })
                    .await;
            }
            Err(e) => return Err(e.into()),
        };
        let manifest = parse_manifest_digests(&content, None)?;

        // The revision's own grant pins the manifest blob.
        self.ensure_grant(namespace, revision, &LinkKind::Digest(revision.clone()))
            .await?;
        for (link, target) in manifest.links_for_revision(revision) {
            self.ensure_link(namespace, &link, &target).await?;
            self.ensure_grant(namespace, &target, &link).await?;
        }
        for (link, target) in manifest.referenced_links_for_revision(revision) {
            self.ensure_referenced_by(namespace, &link, &target, revision)
                .await?;
        }
        Ok(())
    }

    /// A referrer link is live only while its referrer manifest is a current
    /// revision.
    async fn validate_referrer_link(
        &self,
        key: &str,
        namespace: &Namespace,
        subject: &Digest,
        referrer: &Digest,
    ) -> Result<(), Error> {
        if self.read_link_body(key).await?.is_none() {
            return Ok(());
        }
        match self
            .metadata_store
            .read_link(namespace, &LinkKind::Digest(referrer.clone()))
            .await
        {
            Ok(_) => Ok(()),
            Err(RegistryError::NotFound) => {
                self.emit(Action::DeleteOrphanReferrer {
                    namespace: namespace.clone(),
                    subject: subject.clone(),
                    referrer: referrer.clone(),
                })
                .await
            }
            Err(e) => Err(e.into()),
        }
    }

    /// A blob/layer/config/index-child link: prune `referenced_by` entries
    /// whose revision link is gone (the executor cascades the link's deletion
    /// when the set empties).
    async fn validate_tracked_link(
        &self,
        key: &str,
        namespace: &Namespace,
        link: LinkKind,
    ) -> Result<(), Error> {
        let Some(metadata) = self.read_link_body(key).await? else {
            return Ok(());
        };
        for referrer in &metadata.referenced_by {
            match self
                .metadata_store
                .read_link(namespace, &LinkKind::Digest(referrer.clone()))
                .await
            {
                Ok(_) => {}
                Err(RegistryError::NotFound) => {
                    self.emit(Action::RemoveReferrer {
                        namespace: namespace.clone(),
                        link: link.clone(),
                        referrer: referrer.clone(),
                    })
                    .await?;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Read and parse the link body at `key`. `None` when the key vanished
    /// concurrently or its content was unreadable garbage (deleted here).
    async fn read_link_body(&self, key: &str) -> Result<Option<LinkMetadata>, Error> {
        let raw = match self.metadata_store.store().object_store().get(key).await {
            Ok(raw) => raw,
            Err(StorageError::NotFound) => return Ok(None),
            Err(e) => return Err(RegistryError::from(e).into()),
        };
        match serde_json::from_slice::<LinkMetadata>(&raw) {
            Ok(metadata) => Ok(Some(metadata)),
            Err(e) => {
                warn!("scrub: link '{key}' does not parse as link metadata ({e}); deleting");
                self.delete_corrupt(WalkedStore::Metadata, key).await?;
                Ok(None)
            }
        }
    }

    /// Recreate `link -> expected` when it is confirmed missing or
    /// mistargeted (absorbed from the old `link_repair::ensure_link`). Any
    /// other read error propagates: a repair write must not be based on a
    /// read that never succeeded, and an unreadable link is deleted when its
    /// own key is visited.
    async fn ensure_link(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
        expected: &Digest,
    ) -> Result<(), Error> {
        match self.metadata_store.read_link(namespace, link).await {
            Ok(metadata) if &metadata.target == expected => return Ok(()),
            Ok(_) | Err(RegistryError::NotFound) => {}
            Err(e) => return Err(e.into()),
        }
        self.emit(Action::RecreateLink {
            namespace: namespace.clone(),
            link: link.clone(),
            target: expected.clone(),
        })
        .await
    }

    /// Ensure the back-link from `link` to `referrer` is present (absorbed
    /// from the old `LinkReferencesChecker`). Stale-entry pruning happens
    /// where each tracked link is visited.
    async fn ensure_referenced_by(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
        target: &Digest,
        referrer: &Digest,
    ) -> Result<(), Error> {
        let metadata = match self.metadata_store.read_link(namespace, link).await {
            Ok(metadata) => metadata,
            Err(RegistryError::NotFound) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if metadata.referenced_by.contains(referrer) {
            return Ok(());
        }
        self.emit(Action::AddReferrer {
            namespace: namespace.clone(),
            link: link.clone(),
            target: target.clone(),
            referrer: referrer.clone(),
        })
        .await
    }

    /// Emit a grant for `link` on `blob` unless the index already records it,
    /// and only for bytes that still exist (absorbed from the old
    /// `BlobIndexChecker`).
    async fn ensure_grant(
        &self,
        namespace: &Namespace,
        blob: &Digest,
        link: &LinkKind,
    ) -> Result<(), Error> {
        match self
            .metadata_store
            .read_blob_index_namespace(namespace, blob)
            .await
        {
            Ok(links) if links.contains(link) => return Ok(()),
            Ok(_) | Err(RegistryError::NotFound) => {}
            Err(e) => return Err(e.into()),
        }
        match self.blob_store.size(blob).await {
            Ok(_) => {}
            Err(RegistryError::BlobUnknown | RegistryError::NotFound) => {
                // A manifest referencing missing bytes is a broken-manifest
                // problem; granting it would churn against the blob GC.
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }
        self.emit(Action::GrantBlobIndexLink {
            namespace: namespace.clone(),
            blob: blob.clone(),
            link: link.clone(),
        })
        .await
    }
}
