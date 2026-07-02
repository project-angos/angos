//! [`RebuildChecker`] re-derives each current revision's complete
//! link/grant/referrer/`media_type` set by feeding the parsed body through the
//! same planner a manifest push uses, then commits only the operations whose
//! stored state diverges from the derived set: a converged revision incurs no
//! write at all. It only adds or keeps-if-equal; it never deletes, and it never
//! erases a `media_type` or `descriptor` it cannot re-derive.
//!
//! The tag walk compares each tag link's `media_type` against its target's
//! derived value and restamps only on divergence, through the guarded
//! [`MetadataStore::restamp_links`] path (a concurrently-moved tag wins). A tag
//! whose target body is present but whose `Digest` revision link is lost has
//! the full closure restored under the blob-data locks.
//!
//! Each referenced config/layer/child blob is probed for byte-presence,
//! recording absent ones as report-only [`Findings`]. A dangling reference is
//! never a delete: a pull-through mirror may legitimately lack the content.
//!
//! Unlike other [`NamespaceChecker`]s, this one mutates outside the
//! `Action`/`Executor` path, so it holds its own `dry_run` field and skips the
//! writes under `--dry-run`. Any error leaving the walk fails the namespace,
//! excluding it from the destructive sweep passes (keep over reap).

use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    fmt,
    sync::Arc,
};

use async_trait::async_trait;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    command::scrub::{
        check::{NamespaceChecker, list_all},
        error::Error,
        executor::ActionSink,
        report::Findings,
    },
    oci::{Digest, Manifest, MediaType, Namespace, OCI_MANIFEST_SCHEMA_VERSION, Reference, Tag},
    registry::{
        blob_store::{self, BlobStore},
        manifest::{link_plan, parse_and_validate_manifest},
        metadata_store::{
            BlobIndexOperation, Error as MetadataError, LinkKind, LinkOperation, MetadataStore,
        },
    },
};

/// Scrub-local probe that rejects any unknown top-level manifest field via
/// `deny_unknown_fields`. Deliberately not the shared [`Manifest`], whose
/// serving path must keep accepting future/vendor keys. The field names are the
/// known-key allowlist; their values are discarded, and an unknown key fails the
/// namespace. Keep the list in sync with [`Manifest`]'s serialized keys.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[allow(dead_code)]
struct KnownManifestShape {
    #[serde(default)]
    schema_version: serde::de::IgnoredAny,
    #[serde(default)]
    media_type: serde::de::IgnoredAny,
    #[serde(default)]
    config: serde::de::IgnoredAny,
    #[serde(default)]
    layers: serde::de::IgnoredAny,
    #[serde(default)]
    manifests: serde::de::IgnoredAny,
    #[serde(default)]
    subject: serde::de::IgnoredAny,
    #[serde(default)]
    annotations: serde::de::IgnoredAny,
    #[serde(default)]
    artifact_type: serde::de::IgnoredAny,
}

/// The kind of content a manifest references, for reporting a dangling reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceKind {
    Config,
    Layer,
    /// A child manifest of a multi-arch image index.
    ChildManifest,
}

impl fmt::Display for ReferenceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ReferenceKind::Config => "config blob",
            ReferenceKind::Layer => "layer blob",
            ReferenceKind::ChildManifest => "child manifest",
        })
    }
}

/// A manifest reference whose backing content is absent on this registry.
/// Report-only: scrub surfaces the finding but never deletes on this signal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DanglingReference {
    pub kind: ReferenceKind,
    pub digest: Digest,
}

/// Per-namespace walk state threaded through the revision and tag passes.
#[derive(Default)]
struct WalkState {
    /// Revision to derived effective `media_type` for every body read and
    /// planned this run; its `Digest` self-link is verified or restored, so the
    /// tag walk can stamp from it without re-reading the body.
    rebuilt: HashMap<Digest, Option<MediaType>>,
    /// Revisions whose body is missing (the sweep's domain) or failed to parse;
    /// the tag walk must neither stamp from nor repair these.
    unavailable: HashSet<Digest>,
    /// Blob-index grant memo per target digest (empty set = no shard), so a
    /// layer shared by many revisions is read once per namespace.
    grants: HashMap<Digest, HashSet<LinkKind>>,
    /// Digests confirmed byte-present this pass, so a base layer referenced by
    /// many revisions is probed once per namespace. Only present results are
    /// memoized: a stale present merely defers a report-only finding to the next
    /// run, whereas caching absent could hide a newly-missing blob.
    present: HashSet<Digest>,
}

/// Re-derives each current revision's link/grant/referrer/`media_type` set
/// through the shared manifest-push planner and commits only the diverged
/// subset.
pub struct RebuildChecker {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    findings: Findings,
    /// When set, the rebuild probes and records findings but performs no write.
    dry_run: bool,
    /// Run-level cancellation, checked per revision and per tag.
    cancel: CancellationToken,
}

impl RebuildChecker {
    pub fn new(
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
        findings: Findings,
        dry_run: bool,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
            findings,
            dry_run,
            cancel,
        }
    }

    /// Rebuild one revision: read and parse the body, plan through
    /// [`link_plan::push`], filter out the already-converged operations, and
    /// commit the diverged remainder (unless `dry_run`), then record any
    /// dangling references. A missing body is left to the sweep; an invalid
    /// body fails the namespace. Neither ever deletes.
    async fn rebuild_revision(
        &self,
        namespace: &Namespace,
        revision: &Digest,
        state: &mut WalkState,
    ) -> Result<(), Error> {
        let content = match self.blob_store.read(revision).await {
            Ok(content) => content,
            // Missing body: the sweep owns this case. Leave links intact.
            Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
                debug!("Rebuild skipping missing-body revision '{namespace}@{revision}'");
                state.unavailable.insert(revision.clone());
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        let mut manifest = match Self::parse_known_manifest(namespace, revision, &content) {
            Ok(manifest) => manifest,
            Err(e) => {
                state.unavailable.insert(revision.clone());
                return Err(e);
            }
        };

        let effective_media_type = manifest.media_type.clone();
        let ops = link_plan::push(
            &mut manifest,
            revision,
            &Reference::Digest(revision.clone()),
            effective_media_type.as_ref(),
            content.len() as u64,
            &[],
        );

        // Read-compare-skip: a fully converged revision commits nothing.
        let pending = self.filter_diverged(namespace, &ops, state).await?;
        if !self.dry_run && !pending.is_empty() {
            self.commit_pending(namespace, &pending).await?;
        }
        state.rebuilt.insert(revision.clone(), effective_media_type);

        // A dangling-probe failure is informational; it never fails the
        // namespace (the grants/links above are already committed).
        if let Err(e) = self
            .report_dangling(namespace, revision, &manifest, state)
            .await
        {
            warn!("Dangling-reference probe failed for '{namespace}@{revision}': {e}");
        }
        Ok(())
    }

    /// Parse and validate one body, rejecting future formats. An error here
    /// fails the namespace: the planner cannot derive a trustworthy set.
    fn parse_known_manifest(
        namespace: &Namespace,
        revision: &Digest,
        content: &[u8],
    ) -> Result<Manifest, Error> {
        let manifest = parse_and_validate_manifest(content, None).map_err(|e| {
            Error::RebuildIncomplete(format!("invalid manifest '{namespace}@{revision}': {e}"))
        })?;
        if manifest.schema_version != OCI_MANIFEST_SCHEMA_VERSION {
            return Err(Error::RebuildIncomplete(format!(
                "future schemaVersion {} on '{namespace}@{revision}'",
                manifest.schema_version
            )));
        }
        if let Err(e) = serde_json::from_slice::<KnownManifestShape>(content) {
            return Err(Error::RebuildIncomplete(format!(
                "unknown top-level field on '{namespace}@{revision}': {e}"
            )));
        }
        Ok(manifest)
    }

    /// Keep only the `Create` operations whose stored state diverges from the
    /// derived set, and heal a present link's missing blob-ownership grant on
    /// the way (a kept create repairs its own grant; a merge does not).
    async fn filter_diverged(
        &self,
        namespace: &Namespace,
        ops: &[LinkOperation],
        state: &mut WalkState,
    ) -> Result<Vec<LinkOperation>, Error> {
        let mut pending = Vec::new();
        for op in ops {
            let LinkOperation::Create {
                link,
                target,
                referrer,
                media_type,
                descriptor,
            } = op
            else {
                pending.push(op.clone());
                continue;
            };

            let existing = match self.metadata_store.read_link(namespace, link).await {
                Ok(existing) => existing,
                // Absent: the create restores the link and its grant.
                Err(MetadataError::ReferenceNotFound) => {
                    pending.push(op.clone());
                    continue;
                }
                Err(e) => return Err(e.into()),
            };

            // The link is present: its grant must be too, or the orphan-blob
            // pass would see the blob unreferenced.
            self.ensure_grant(namespace, target, link, state).await?;

            if link.is_tracked() && referrer.is_some() {
                if existing.target != *target {
                    // The merge path never rewrites a tracked binding, so a
                    // contradicting target cannot be repaired here: fail the
                    // namespace (keep over reap) instead of rewriting forever.
                    return Err(Error::RebuildIncomplete(format!(
                        "tracked link '{link}' targets '{}' but the body derives '{target}'",
                        existing.target
                    )));
                }
                if referrer
                    .as_ref()
                    .is_some_and(|r| !existing.referenced_by.contains(r))
                {
                    pending.push(op.clone());
                }
            } else {
                let repoint = existing.target != *target;
                // Never downgrade: a stored value the body cannot re-derive is
                // kept, so only a derived Some that differs counts as diverged.
                let stamp_media_type = media_type.is_some() && existing.media_type != *media_type;
                let stamp_descriptor =
                    descriptor.is_some() && existing.descriptor.as_ref() != descriptor.as_deref();
                if repoint || stamp_media_type || stamp_descriptor {
                    pending.push(op.clone());
                }
            }
        }
        Ok(pending)
    }

    /// Ensure `link` is granted in `target`'s blob index, healing a stripped
    /// grant under the blob-data lock (the grant-insert protocol every writer
    /// follows). The per-namespace memo keeps shared layers to one read.
    async fn ensure_grant(
        &self,
        namespace: &Namespace,
        target: &Digest,
        link: &LinkKind,
        state: &mut WalkState,
    ) -> Result<(), Error> {
        let grants = match state.grants.entry(target.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let set = match self
                    .metadata_store
                    .read_blob_index_namespace(namespace, target)
                    .await
                {
                    Ok(set) => set,
                    Err(MetadataError::ReferenceNotFound) => HashSet::new(),
                    Err(e) => return Err(e.into()),
                };
                entry.insert(set)
            }
        };
        if grants.contains(link) {
            return Ok(());
        }
        if self.dry_run {
            info!(
                "DRY RUN: would restore missing blob-ownership grant '{link}' for \
                 '{namespace}/{target}'"
            );
            return Ok(());
        }
        let session = self.metadata_store.acquire_blob_data_lock(target).await?;
        let result = self
            .metadata_store
            .update_blob_index(namespace, target, BlobIndexOperation::Insert(link.clone()))
            .await;
        session.release().await;
        result?;
        info!("Rebuild restored missing blob-ownership grant '{link}' for '{namespace}/{target}'");
        grants.insert(link.clone());
        Ok(())
    }

    /// Commit the diverged operations. Tracked grant inserts commit only under
    /// their blob-data locks, so a refusal from the planner splits the batch:
    /// only the tracked creates retry through the locked `store_manifest` path
    /// (with the refused set acquired, grown if a concurrent change enlarges
    /// it), while the rest re-commits through `update_links`. The split keeps
    /// a non-tracked repair rewrite off the push path's `created_at` stamping,
    /// so a legacy link's `None` is never turned into manufactured LWW
    /// freshness by being batched with a refused tracked insert.
    async fn commit_pending(
        &self,
        namespace: &Namespace,
        pending: &[LinkOperation],
    ) -> Result<(), Error> {
        let mut needs = match self.metadata_store.update_links(namespace, pending).await {
            Ok(()) => return Ok(()),
            Err(MetadataError::TrackedInsertWithoutLock(needs)) => needs,
            Err(e) => return Err(e.into()),
        };
        let (tracked, rest): (Vec<LinkOperation>, Vec<LinkOperation>) =
            pending.iter().cloned().partition(|op| {
                matches!(
                    op,
                    LinkOperation::Create {
                        link,
                        referrer: Some(_),
                        ..
                    } if link.is_tracked()
                )
            });
        if !rest.is_empty() {
            self.metadata_store.update_links(namespace, &rest).await?;
        }
        for _ in 0..3 {
            let granted: HashSet<Digest> = needs.iter().cloned().collect();
            let session = self.metadata_store.acquire_blob_data_locks(&needs).await?;
            let result = self
                .metadata_store
                .store_manifest(namespace, &tracked, None, &granted)
                .await;
            session.release().await;
            let commit = result?;
            if commit.needs_locks.is_empty() {
                return Ok(());
            }
            needs.extend(commit.needs_locks);
        }
        Err(Error::RebuildIncomplete(
            "tracked grant heal did not converge after growing the lock set".into(),
        ))
    }

    /// Probe every config/layer/child blob a manifest references and record the
    /// ones whose bytes are absent. Report-only; never a delete.
    async fn report_dangling(
        &self,
        namespace: &Namespace,
        revision: &Digest,
        manifest: &Manifest,
        state: &mut WalkState,
    ) -> Result<(), Error> {
        for dangling in self.find_dangling_references(manifest, state).await? {
            warn!(
                "Manifest '{namespace}@{revision}' references missing {} '{}'",
                dangling.kind, dangling.digest
            );
            self.findings
                .record_dangling(namespace, revision, &dangling)
                .await;
        }
        Ok(())
    }

    /// Collect the config/layer/child references whose bytes are absent.
    async fn find_dangling_references(
        &self,
        manifest: &Manifest,
        state: &mut WalkState,
    ) -> Result<Vec<DanglingReference>, Error> {
        let config = manifest
            .config
            .iter()
            .map(|descriptor| (ReferenceKind::Config, &descriptor.digest));
        let layers = manifest
            .layers
            .iter()
            .map(|descriptor| (ReferenceKind::Layer, &descriptor.digest));
        let children = manifest
            .manifests
            .iter()
            .map(|descriptor| (ReferenceKind::ChildManifest, &descriptor.digest));

        let mut dangling = Vec::new();
        for (kind, digest) in config.chain(layers).chain(children) {
            if !self.blob_present(digest, state).await? {
                dangling.push(DanglingReference {
                    kind,
                    digest: digest.clone(),
                });
            }
        }
        Ok(dangling)
    }

    /// Whether `digest`'s bytes are present; a not-found reports absent. Confirmed
    /// present digests are memoized in `state`, so a shared blob is probed once
    /// per namespace; absent results are never cached (a blob may reappear or
    /// vanish before the next revision).
    async fn blob_present(&self, digest: &Digest, state: &mut WalkState) -> Result<bool, Error> {
        if state.present.contains(digest) {
            return Ok(true);
        }
        match self.blob_store.size(digest).await {
            Ok(_) => {
                state.present.insert(digest.clone());
                Ok(true)
            }
            Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Compare one tag link's `media_type` against its target's derived value
    /// and restamp only on divergence, via the guarded restamp (a concurrently
    /// moved tag wins). A target the revision walk did not visit is derived
    /// here, restoring a lost `Digest` revision link when the body is present.
    async fn stamp_tag(
        &self,
        namespace: &Namespace,
        tag: &Tag,
        state: &mut WalkState,
    ) -> Result<(), Error> {
        let link = LinkKind::Tag(tag.clone());
        let metadata = match self.metadata_store.read_link(namespace, &link).await {
            Ok(metadata) => metadata,
            Err(MetadataError::ReferenceNotFound) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if state.unavailable.contains(&metadata.target) {
            return Ok(());
        }

        let derived = match state.rebuilt.get(&metadata.target) {
            Some(media_type) => media_type.clone(),
            None => match self
                .derive_unenumerated(namespace, &metadata.target, state)
                .await?
            {
                Some(media_type) => media_type,
                None => return Ok(()),
            },
        };

        // Never downgrade: restamp only when the body derives a media_type the
        // tag link does not carry.
        if derived.is_none() || metadata.media_type == derived {
            return Ok(());
        }
        if self.dry_run {
            info!("DRY RUN: would restamp media_type on tag '{namespace}:{tag}'");
            return Ok(());
        }
        self.metadata_store
            .restamp_links(
                namespace,
                &[LinkOperation::create_with_media_type(
                    link,
                    metadata.target,
                    derived,
                )],
            )
            .await?;
        Ok(())
    }

    /// Derive the effective `media_type` of a tag target the revision walk did
    /// not visit, restoring its lost `Digest` revision link (and full closure)
    /// when the link is absent but the body is present. Returns `None` when the
    /// body is missing (a dangling tag, the sweep's domain) or the repair was
    /// skipped.
    async fn derive_unenumerated(
        &self,
        namespace: &Namespace,
        target: &Digest,
        state: &mut WalkState,
    ) -> Result<Option<Option<MediaType>>, Error> {
        let content = match self.blob_store.read(target).await {
            Ok(content) => content,
            Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
                state.unavailable.insert(target.clone());
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };
        let mut manifest = match Self::parse_known_manifest(namespace, target, &content) {
            Ok(manifest) => manifest,
            Err(e) => {
                state.unavailable.insert(target.clone());
                return Err(e);
            }
        };
        let media_type = manifest.media_type.clone();

        match self
            .metadata_store
            .read_link(namespace, &LinkKind::Digest(target.clone()))
            .await
        {
            // Present: a tag pushed mid-run; the push converged its closure.
            Ok(_) => {}
            Err(MetadataError::ReferenceNotFound) => {
                if !self
                    .repair_lost_revision(namespace, target, &mut manifest, content.len() as u64)
                    .await?
                {
                    return Ok(None);
                }
            }
            Err(e) => return Err(e.into()),
        }
        state.rebuilt.insert(target.clone(), media_type.clone());
        Ok(Some(media_type))
    }

    /// Restore a lost `Digest` revision link (and the full derived closure) from
    /// a live tag's in-hand body, as if the manifest were re-pushed: all
    /// blob-data locks are taken in one all-or-nothing acquisition and the
    /// bytes are re-checked under them, so a concurrent delete wins. Returns
    /// whether the repair was applied.
    async fn repair_lost_revision(
        &self,
        namespace: &Namespace,
        revision: &Digest,
        manifest: &mut Manifest,
        content_len: u64,
    ) -> Result<bool, Error> {
        let effective_media_type = manifest.media_type.clone();
        let ops = link_plan::push(
            manifest,
            revision,
            &Reference::Digest(revision.clone()),
            effective_media_type.as_ref(),
            content_len,
            &[],
        );
        if self.dry_run {
            info!(
                "DRY RUN: would restore the lost revision link '{namespace}@{revision}' from its \
                 live tag"
            );
            return Ok(true);
        }

        let mut lock_set: Vec<Digest> = ops
            .iter()
            .filter_map(|op| match op {
                LinkOperation::Create { target, .. } => Some(target.clone()),
                LinkOperation::Delete { .. } => None,
            })
            .collect();
        lock_set.push(revision.clone());
        let session = self
            .metadata_store
            .acquire_blob_data_locks(&lock_set)
            .await?;
        let result = async {
            // Byte re-check under the lock: deletes decide byte removal under
            // it, so an absent body means a concurrent delete won.
            match self.blob_store.size(revision).await {
                Ok(_) => {}
                Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
                    warn!(
                        "Skipping lost revision link repair: body vanished for \
                         '{namespace}@{revision}'"
                    );
                    return Ok(false);
                }
                Err(e) => return Err(Error::from(e)),
            }
            let granted: HashSet<Digest> = lock_set.iter().cloned().collect();
            let commit = self
                .metadata_store
                .store_manifest(namespace, &ops, None, &granted)
                .await?;
            if !commit.needs_locks.is_empty() {
                return Err(Error::RebuildIncomplete(format!(
                    "lost revision link repair for '{namespace}@{revision}' still needs locks"
                )));
            }
            info!(
                "Rebuild restored the lost revision link '{namespace}@{revision}' from its live tag"
            );
            Ok(true)
        }
        .await;
        session.release().await;
        result
    }
}

#[async_trait]
impl NamespaceChecker for RebuildChecker {
    async fn check(
        &self,
        namespace: &Namespace,
        _sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        debug!("Rebuilding manifest-derived state for namespace '{namespace}'");

        let mut state = WalkState::default();
        let mut failed = false;

        let mut revisions = list_all::revisions(&self.metadata_store, namespace);
        while let Some(revision) = revisions.next().await {
            if self.cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let revision = match revision {
                Ok(revision) => revision,
                Err(e) => {
                    error!("Rebuild revision enumeration failed for '{namespace}': {e}");
                    return Err(e);
                }
            };
            if let Err(e) = self
                .rebuild_revision(namespace, &revision, &mut state)
                .await
            {
                error!("Rebuild failed for '{namespace}' (revision '{revision}'): {e}");
                failed = true;
            }
        }

        let mut tags = list_all::tags(&self.metadata_store, namespace);
        while let Some(tag) = tags.next().await {
            if self.cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let tag = match tag {
                Ok(tag) => tag,
                Err(e) => {
                    error!("Rebuild tag enumeration failed for '{namespace}': {e}");
                    return Err(e);
                }
            };
            if let Err(e) = self.stamp_tag(namespace, &tag, &mut state).await {
                error!("Rebuild tag stamp failed for '{namespace}:{tag}': {e}");
                failed = true;
            }
        }

        if failed {
            return Err(Error::RebuildIncomplete(format!(
                "namespace '{namespace}' could not be fully re-derived"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    };

    use angos_storage::{
        BoxedReader, ByteStream, ChildrenPage, Error as StorageError, MultipartUploadPage,
        ObjectMeta, ObjectStore, Page, fs::Backend as StorageFsBackend,
    };
    use bytes::Bytes;
    use chrono::{DateTime, Utc};
    use tempfile::TempDir;

    use super::*;
    use crate::{
        command::scrub::action::Action,
        oci::MediaType,
        registry::{
            blob_store::{BlobStoreConfig, FsBackendConfig},
            metadata_store::{BlobIndexOperation, LinkMetadata},
            path_builder,
            test_utils::{
                RegistryTestCase, backends, build_store, locked_executor_over, put_blob_direct,
                put_link_raw,
            },
        },
    };

    const MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";

    /// Seed a fully-correct pushed revision (config blob, layer blob, and a
    /// manifest referencing both) through the same planner a push uses. Returns
    /// `(revision, config, layer)`.
    async fn create_pushed_revision(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
    ) -> (Digest, Digest, Digest) {
        let config = put_blob_direct(metadata_store.store(), b"rebuild config bytes").await;
        let layer = put_blob_direct(metadata_store.store(), b"rebuild layer bytes").await;

        let manifest_content = format!(
            r#"{{
            "schemaVersion": 2,
            "mediaType": "{MANIFEST_MEDIA_TYPE}",
            "config": {{
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "{config}",
                "size": 20
            }},
            "layers": [
                {{
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": "{layer}",
                    "size": 19
                }}
            ]
        }}"#
        );
        let revision = put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;

        let mut manifest = parse_and_validate_manifest(manifest_content.as_bytes(), None).unwrap();
        let effective_media_type = manifest.media_type.clone();
        let ops = link_plan::push(
            &mut manifest,
            &revision,
            &Reference::Digest(revision.clone()),
            effective_media_type.as_ref(),
            manifest_content.len() as u64,
            &[],
        );
        metadata_store.seed_links(namespace, &ops).await.unwrap();

        (revision, config, layer)
    }

    fn rebuild(test_case: &dyn RegistryTestCase, dry_run: bool) -> RebuildChecker {
        RebuildChecker::new(
            test_case.blob_store(),
            test_case.metadata_store(),
            Findings::default(),
            dry_run,
            CancellationToken::new(),
        )
    }

    /// A fully-correct revision is left unchanged: link targets, `media_type`,
    /// `referenced_by`, and grants all match the snapshot.
    #[tokio::test]
    async fn rebuild_is_content_noop_for_correct_revision() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/noop").unwrap();
            let metadata_store = test_case.metadata_store();

            let (revision, config, layer) =
                create_pushed_revision(&metadata_store, namespace).await;

            let before_digest = metadata_store
                .read_link(namespace, &LinkKind::Digest(revision.clone()))
                .await
                .unwrap();
            let before_config = metadata_store
                .read_link(namespace, &LinkKind::Config(config.clone()))
                .await
                .unwrap();
            let before_layer = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer.clone()))
                .await
                .unwrap();
            let before_config_grant = metadata_store
                .read_blob_index_namespace(namespace, &config)
                .await
                .unwrap();
            let before_layer_grant = metadata_store
                .read_blob_index_namespace(namespace, &layer)
                .await
                .unwrap();

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            let after_digest = metadata_store
                .read_link(namespace, &LinkKind::Digest(revision.clone()))
                .await
                .unwrap();
            let after_config = metadata_store
                .read_link(namespace, &LinkKind::Config(config.clone()))
                .await
                .unwrap();
            let after_layer = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer.clone()))
                .await
                .unwrap();

            assert_eq!(before_digest.target, after_digest.target);
            assert_eq!(before_digest.media_type, after_digest.media_type);
            assert_eq!(before_config.target, after_config.target);
            assert_eq!(before_config.referenced_by, after_config.referenced_by);
            assert_eq!(before_layer.target, after_layer.target);
            assert_eq!(before_layer.referenced_by, after_layer.referenced_by);
            assert_eq!(
                before_config_grant,
                metadata_store
                    .read_blob_index_namespace(namespace, &config)
                    .await
                    .unwrap()
            );
            assert_eq!(
                before_layer_grant,
                metadata_store
                    .read_blob_index_namespace(namespace, &layer)
                    .await
                    .unwrap()
            );
            test_case.cleanup().await;
        }
    }

    /// An `ObjectStore` wrapper counting every mutation, so a test can assert a
    /// converged rebuild performs literally zero writes.
    struct CountingObjectStore {
        inner: Arc<dyn ObjectStore>,
        puts: AtomicU64,
        deletes: AtomicU64,
        heads: Mutex<HashMap<String, u64>>,
    }

    impl CountingObjectStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                puts: AtomicU64::new(0),
                deletes: AtomicU64::new(0),
                heads: Mutex::new(HashMap::new()),
            }
        }

        fn reset(&self) {
            self.puts.store(0, Ordering::SeqCst);
            self.deletes.store(0, Ordering::SeqCst);
            self.heads.lock().unwrap().clear();
        }

        fn mutations(&self) -> (u64, u64) {
            (
                self.puts.load(Ordering::SeqCst),
                self.deletes.load(Ordering::SeqCst),
            )
        }

        /// HEAD probes recorded against `key` since the last reset.
        fn heads_for(&self, key: &str) -> u64 {
            self.heads.lock().unwrap().get(key).copied().unwrap_or(0)
        }
    }

    #[async_trait]
    impl ObjectStore for CountingObjectStore {
        async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
            self.inner.get(key).await
        }
        async fn get_stream(
            &self,
            key: &str,
            offset: Option<u64>,
        ) -> Result<(BoxedReader, u64), StorageError> {
            self.inner.get_stream(key, offset).await
        }
        async fn put(&self, key: &str, data: Bytes) -> Result<(), StorageError> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            self.inner.put(key, data).await
        }
        async fn delete(&self, key: &str) -> Result<(), StorageError> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            self.inner.delete(key).await
        }
        async fn delete_prefix(&self, prefix: &str) -> Result<(), StorageError> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            self.inner.delete_prefix(prefix).await
        }
        async fn head(&self, key: &str) -> Result<ObjectMeta, StorageError> {
            *self
                .heads
                .lock()
                .unwrap()
                .entry(key.to_string())
                .or_insert(0) += 1;
            self.inner.head(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            n: u16,
            token: Option<String>,
        ) -> Result<Page<String>, StorageError> {
            self.inner.list(prefix, n, token).await
        }
        async fn list_children(
            &self,
            prefix: &str,
            n: u16,
            token: Option<String>,
            start_after: Option<String>,
        ) -> Result<ChildrenPage, StorageError> {
            self.inner
                .list_children(prefix, n, token, start_after)
                .await
        }
        async fn copy(&self, source: &str, destination: &str) -> Result<(), StorageError> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            self.inner.copy(source, destination).await
        }
        async fn create_upload(&self, key: &str) -> Result<(), StorageError> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            self.inner.create_upload(key).await
        }
        async fn write_upload(
            &self,
            key: &str,
            body: ByteStream,
            len: Option<u64>,
        ) -> Result<u64, StorageError> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            self.inner.write_upload(key, body, len).await
        }
        async fn complete_upload(&self, key: &str) -> Result<(), StorageError> {
            self.inner.complete_upload(key).await
        }
        async fn abort_upload(&self, key: &str) -> Result<(), StorageError> {
            self.inner.abort_upload(key).await
        }
        async fn list_multipart_uploads(
            &self,
            key_marker: Option<&str>,
            upload_id_marker: Option<&str>,
        ) -> Result<MultipartUploadPage, StorageError> {
            self.inner
                .list_multipart_uploads(key_marker, upload_id_marker)
                .await
        }
    }

    /// Fixture over a counting FS store: the metadata store AND its transaction
    /// executor both route through the counter, so any engine or link write is
    /// visible; the blob store reads the same root.
    fn counting_fixture(
        root: &TempDir,
    ) -> (Arc<CountingObjectStore>, Arc<MetadataStore>, Arc<BlobStore>) {
        let path = root.path().to_string_lossy().to_string();
        let counting = Arc::new(CountingObjectStore::new(Arc::new(
            StorageFsBackend::builder(path.as_str()).build(),
        )));
        let object: Arc<dyn ObjectStore> = counting.clone();
        let executor = locked_executor_over(object.clone());
        let metadata_store = Arc::new(
            MetadataStore::builder(build_store(object, executor))
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build(),
        );
        let blob_store = Arc::new(
            BlobStoreConfig::FS(FsBackendConfig {
                root_dir: path,
                sync_to_disk: false,
            })
            .build_backend()
            .unwrap(),
        );
        (counting, metadata_store, blob_store)
    }

    /// A converged registry sees ZERO rebuild writes: no link put, no grant put,
    /// no engine transaction traffic at all.
    #[tokio::test]
    async fn rebuild_is_zero_write_for_converged_namespace() {
        let root = TempDir::new().unwrap();
        let (counting, metadata_store, blob_store) = counting_fixture(&root);
        let namespace = &Namespace::new("test-repo/zero-write").unwrap();

        let (revision, _config, _layer) = create_pushed_revision(&metadata_store, namespace).await;
        metadata_store
            .update_links(
                namespace,
                &[LinkOperation::create_with_media_type(
                    LinkKind::Tag(Tag::new("latest").unwrap()),
                    revision,
                    Some(MediaType::new(MANIFEST_MEDIA_TYPE).unwrap()),
                )],
            )
            .await
            .unwrap();

        counting.reset();
        let checker = RebuildChecker::new(
            blob_store,
            metadata_store,
            Findings::default(),
            false,
            CancellationToken::new(),
        );
        let mut sink: Vec<Action> = Vec::new();
        checker.check(namespace, &mut sink).await.unwrap();

        assert_eq!(
            counting.mutations(),
            (0, 0),
            "a converged namespace must incur zero rebuild writes"
        );
    }

    /// Raw-rewrite one link file with `accessed_at` (and optionally
    /// `created_at`) altered, simulating a pull stamp.
    async fn set_accessed_at(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
        link: &LinkKind,
        accessed_at: DateTime<Utc>,
    ) -> LinkMetadata {
        let path = path_builder::link_path(link, namespace);
        let body = metadata_store.store().get(&path).await.unwrap();
        let mut metadata: LinkMetadata = serde_json::from_slice(&body).unwrap();
        metadata.accessed_at = Some(accessed_at);
        metadata_store
            .store()
            .put(&path, Bytes::from(serde_json::to_vec(&metadata).unwrap()))
            .await
            .unwrap();
        metadata
    }

    /// `accessed_at` (retention's last-pull time) and `created_at` survive a
    /// rebuild, both on the converged revision links and on a tag the walk
    /// restamps.
    #[tokio::test]
    async fn rebuild_preserves_accessed_at_on_links() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/accessed").unwrap();
            let metadata_store = test_case.metadata_store();

            let (revision, _config, _layer) =
                create_pushed_revision(&metadata_store, namespace).await;
            // A tag missing its media_type, so the tag walk restamps it.
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Tag(Tag::new("latest").unwrap()),
                        revision.clone(),
                    )],
                )
                .await
                .unwrap();

            let pulled_at = Utc::now() - chrono::Duration::hours(3);
            let digest_before = set_accessed_at(
                &metadata_store,
                namespace,
                &LinkKind::Digest(revision.clone()),
                pulled_at,
            )
            .await;
            let tag_before = set_accessed_at(
                &metadata_store,
                namespace,
                &LinkKind::Tag(Tag::new("latest").unwrap()),
                pulled_at,
            )
            .await;

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            let digest_after = metadata_store
                .read_link(namespace, &LinkKind::Digest(revision.clone()))
                .await
                .unwrap();
            let tag_after = metadata_store
                .read_link(namespace, &LinkKind::Tag(Tag::new("latest").unwrap()))
                .await
                .unwrap();
            assert_eq!(
                digest_after.accessed_at, digest_before.accessed_at,
                "a converged revision link's accessed_at must survive the rebuild"
            );
            assert_eq!(digest_after.created_at, digest_before.created_at);
            assert_eq!(
                tag_after.accessed_at, tag_before.accessed_at,
                "a restamped tag's accessed_at must survive the rebuild"
            );
            assert_eq!(
                tag_after.created_at, tag_before.created_at,
                "a restamped tag must not manufacture LWW freshness"
            );
            assert_eq!(
                tag_after.media_type.as_deref(),
                Some(MANIFEST_MEDIA_TYPE),
                "the restamp must still upgrade the missing media_type"
            );
            test_case.cleanup().await;
        }
    }

    /// A legacy bare-digest tag link (`created_at` None) restamped by the rebuild
    /// keeps `created_at` None, so it still never wins last-writer-wins.
    #[tokio::test]
    async fn restamp_preserves_legacy_none_created_at() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/legacy-tag").unwrap();
            let metadata_store = test_case.metadata_store();

            let (revision, _config, _layer) =
                create_pushed_revision(&metadata_store, namespace).await;
            // Legacy link files: the bare digest string, created_at None. The
            // digest link takes the update_links stamp path, the tag link the
            // guarded restamp path.
            let tag_link = LinkKind::Tag(Tag::new("legacy").unwrap());
            put_link_raw(
                metadata_store.store(),
                namespace,
                &tag_link,
                revision.to_string().as_bytes(),
            )
            .await;
            let digest_link = LinkKind::Digest(revision.clone());
            put_link_raw(
                metadata_store.store(),
                namespace,
                &digest_link,
                revision.to_string().as_bytes(),
            )
            .await;

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            let after = metadata_store
                .read_link(namespace, &tag_link)
                .await
                .unwrap();
            assert_eq!(
                after.media_type.as_deref(),
                Some(MANIFEST_MEDIA_TYPE),
                "the legacy tag must gain the derived media_type"
            );
            assert!(
                after.created_at.is_none(),
                "a legacy link's created_at must stay None (never manufacture LWW freshness)"
            );
            let digest_after = metadata_store
                .read_link(namespace, &digest_link)
                .await
                .unwrap();
            assert_eq!(
                digest_after.media_type.as_deref(),
                Some(MANIFEST_MEDIA_TYPE),
                "the legacy digest link must gain the derived media_type"
            );
            assert!(
                digest_after.created_at.is_none(),
                "a legacy digest link's created_at must stay None through update_links"
            );
            test_case.cleanup().await;
        }
    }

    /// A stripped layer link and grant are restored, with the revision recorded
    /// as referrer and the layer pinned against orphan GC. The absent tracked
    /// link routes the commit through the locked grant-heal path.
    #[tokio::test]
    async fn rebuild_restores_missing_layer_link_and_grant() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/restore").unwrap();
            let metadata_store = test_case.metadata_store();

            let (revision, _config, layer) =
                create_pushed_revision(&metadata_store, namespace).await;

            // Strip the layer link and both grant entries (corrupted out-of-band).
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::delete_with_referrer(
                        LinkKind::Layer(layer.clone()),
                        revision.clone(),
                    )],
                )
                .await
                .unwrap();
            for link in [
                LinkKind::Layer(layer.clone()),
                LinkKind::Blob(layer.clone()),
            ] {
                metadata_store
                    .update_blob_index(namespace, &layer, BlobIndexOperation::Remove(link))
                    .await
                    .unwrap();
            }
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Layer(layer.clone()))
                    .await
                    .is_err(),
                "precondition: the layer link is stripped"
            );

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            let restored = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer.clone()))
                .await
                .unwrap();
            assert_eq!(restored.target, layer);
            assert!(
                restored.referenced_by.contains(&revision),
                "the restored layer link must record the revision as referrer"
            );
            let grant = metadata_store
                .read_blob_index_namespace(namespace, &layer)
                .await
                .unwrap();
            assert!(grant.contains(&LinkKind::Layer(layer.clone())));
            assert!(metadata_store.has_blob_references(&layer).await.unwrap());
            test_case.cleanup().await;
        }
    }

    /// A present link whose blob-ownership grant was stripped gets the grant
    /// healed through the locked path, re-pinning the bytes.
    #[tokio::test]
    async fn rebuild_restores_stripped_grant_when_link_present() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/grant-heal").unwrap();
            let metadata_store = test_case.metadata_store();

            let (_revision, _config, layer) =
                create_pushed_revision(&metadata_store, namespace).await;

            for link in [
                LinkKind::Layer(layer.clone()),
                LinkKind::Blob(layer.clone()),
            ] {
                metadata_store
                    .update_blob_index(namespace, &layer, BlobIndexOperation::Remove(link))
                    .await
                    .unwrap();
            }
            assert!(
                metadata_store
                    .read_blob_index_namespace(namespace, &layer)
                    .await
                    .is_err(),
                "precondition: the layer grant is stripped while the link is present"
            );

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            let grant = metadata_store
                .read_blob_index_namespace(namespace, &layer)
                .await
                .unwrap();
            assert!(
                grant.contains(&LinkKind::Layer(layer.clone())),
                "the stripped grant must be healed while the link is present"
            );
            test_case.cleanup().await;
        }
    }

    /// A digest link with the correct target but no `media_type` gets it stamped.
    #[tokio::test]
    async fn rebuild_stamps_missing_media_type_on_digest_link() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/mt-digest").unwrap();
            let metadata_store = test_case.metadata_store();

            let (revision, _config, _layer) =
                create_pushed_revision(&metadata_store, namespace).await;

            // Overwrite the digest link with the correct target but no media_type.
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create_with_media_type(
                        LinkKind::Digest(revision.clone()),
                        revision.clone(),
                        None,
                    )],
                )
                .await
                .unwrap();
            let before = metadata_store
                .read_link(namespace, &LinkKind::Digest(revision.clone()))
                .await
                .unwrap();
            assert_eq!(before.target, revision, "precondition: target correct");
            assert!(
                before.media_type.is_none(),
                "precondition: media_type absent on a same-target digest link"
            );

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            let after = metadata_store
                .read_link(namespace, &LinkKind::Digest(revision.clone()))
                .await
                .unwrap();
            assert_eq!(
                after.media_type.as_deref(),
                Some(MANIFEST_MEDIA_TYPE),
                "the rebuild must re-stamp media_type on a same-target digest link"
            );
            test_case.cleanup().await;
        }
    }

    /// A stored `media_type` the body cannot re-derive is never erased: a
    /// manifest without an embedded mediaType leaves the link's Some(...) alone
    /// (and the converged namespace sees no write for it).
    #[tokio::test]
    async fn rebuild_never_downgrades_media_type() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/no-downgrade").unwrap();
            let metadata_store = test_case.metadata_store();

            // A valid manifest with NO top-level mediaType (it came from the
            // push Content-Type header instead).
            let manifest_content = r#"{"schemaVersion":2,"config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0},"layers":[]}"#;
            let revision =
                put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;
            let header_media_type = MediaType::new(MANIFEST_MEDIA_TYPE).unwrap();
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create_with_media_type(
                        LinkKind::Digest(revision.clone()),
                        revision.clone(),
                        Some(header_media_type.clone()),
                    )],
                )
                .await
                .unwrap();

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            let after = metadata_store
                .read_link(namespace, &LinkKind::Digest(revision.clone()))
                .await
                .unwrap();
            assert_eq!(
                after.media_type,
                Some(header_media_type),
                "the rebuild must never erase a media_type it cannot re-derive"
            );
            test_case.cleanup().await;
        }
    }

    /// A revision and its tag link, both missing `media_type`, both get stamped.
    #[tokio::test]
    async fn rebuild_stamps_media_type_on_tag_link() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/mt-tag").unwrap();
            let metadata_store = test_case.metadata_store();

            let (revision, _config, _layer) =
                create_pushed_revision(&metadata_store, namespace).await;

            // Seed a tag link with the correct target but no media_type, and drop
            // the digest link's media_type too.
            metadata_store
                .update_links(
                    namespace,
                    &[
                        LinkOperation::create_with_media_type(
                            LinkKind::Digest(revision.clone()),
                            revision.clone(),
                            None,
                        ),
                        LinkOperation::create_with_media_type(
                            LinkKind::Tag(Tag::new("latest").unwrap()),
                            revision.clone(),
                            None,
                        ),
                    ],
                )
                .await
                .unwrap();

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(revision.clone()))
                .await
                .unwrap();
            let tag_link = metadata_store
                .read_link(namespace, &LinkKind::Tag(Tag::new("latest").unwrap()))
                .await
                .unwrap();
            assert_eq!(
                digest_link.media_type.as_deref(),
                Some(MANIFEST_MEDIA_TYPE),
                "the revision digest link must be stamped"
            );
            assert_eq!(
                tag_link.media_type.as_deref(),
                Some(MANIFEST_MEDIA_TYPE),
                "the tag link must be stamped by the tag walk"
            );
            test_case.cleanup().await;
        }
    }

    /// A non-tracked link whose stored target contradicts the body (here the
    /// `Digest` self-link raw-rewritten at a wrong digest) is re-pointed at
    /// the derived target, with the derived `media_type` stamped.
    #[tokio::test]
    async fn rebuild_repoints_non_tracked_link_with_mismatched_target() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/repoint").unwrap();
            let metadata_store = test_case.metadata_store();

            let (revision, _config, _layer) =
                create_pushed_revision(&metadata_store, namespace).await;
            let wrong = put_blob_direct(metadata_store.store(), b"wrong repoint target").await;
            put_link_raw(
                metadata_store.store(),
                namespace,
                &LinkKind::Digest(revision.clone()),
                wrong.to_string().as_bytes(),
            )
            .await;
            let before = metadata_store
                .read_link(namespace, &LinkKind::Digest(revision.clone()))
                .await
                .unwrap();
            assert_eq!(before.target, wrong, "precondition: mismatched target");

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            let after = metadata_store
                .read_link(namespace, &LinkKind::Digest(revision.clone()))
                .await
                .unwrap();
            assert_eq!(
                after.target, revision,
                "the mismatched non-tracked link must be re-pointed at the derived target"
            );
            assert_eq!(
                after.media_type.as_deref(),
                Some(MANIFEST_MEDIA_TYPE),
                "the repoint must carry the derived media_type"
            );
            test_case.cleanup().await;
        }
    }

    /// A tracked link whose stored target contradicts the body fails the
    /// namespace (`RebuildIncomplete`, keep over reap): the merge path never
    /// rewrites a tracked binding, and the failure excludes the namespace from
    /// the destructive sweep passes.
    #[tokio::test]
    async fn rebuild_fails_namespace_on_contradicting_tracked_target() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/contradict").unwrap();
            let metadata_store = test_case.metadata_store();

            let (_revision, _config, layer) =
                create_pushed_revision(&metadata_store, namespace).await;
            let wrong = put_blob_direct(metadata_store.store(), b"wrong tracked target").await;
            put_link_raw(
                metadata_store.store(),
                namespace,
                &LinkKind::Layer(layer.clone()),
                wrong.to_string().as_bytes(),
            )
            .await;

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            assert!(
                matches!(
                    checker.check(namespace, &mut sink).await,
                    Err(Error::RebuildIncomplete(_))
                ),
                "a contradicting tracked target must fail the namespace"
            );
            let kept = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer.clone()))
                .await
                .unwrap();
            assert_eq!(
                kept.target, wrong,
                "the contradicting tracked link is kept, never rewritten"
            );
            test_case.cleanup().await;
        }
    }

    /// A legacy non-tracked link (`created_at` None) batched with a refused
    /// tracked insert keeps `created_at` None: the grant-heal split routes the
    /// non-tracked repair through `update_links`, off the push path's
    /// `created_at` stamping, so a repair rewrite never manufactures LWW
    /// freshness.
    #[tokio::test]
    async fn grant_heal_batch_preserves_legacy_none_created_at() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/legacy-batch").unwrap();
            let metadata_store = test_case.metadata_store();

            let (revision, _config, layer) =
                create_pushed_revision(&metadata_store, namespace).await;

            // Strip the layer link and grants so the batch carries a refused
            // tracked insert...
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::delete_with_referrer(
                        LinkKind::Layer(layer.clone()),
                        revision.clone(),
                    )],
                )
                .await
                .unwrap();
            for link in [
                LinkKind::Layer(layer.clone()),
                LinkKind::Blob(layer.clone()),
            ] {
                metadata_store
                    .update_blob_index(namespace, &layer, BlobIndexOperation::Remove(link))
                    .await
                    .unwrap();
            }
            // ...and make the digest self-link a legacy bare-digest body
            // (created_at None, no media_type) so it is diverged and batched.
            put_link_raw(
                metadata_store.store(),
                namespace,
                &LinkKind::Digest(revision.clone()),
                revision.to_string().as_bytes(),
            )
            .await;

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Layer(layer.clone()))
                    .await
                    .is_ok(),
                "the refused tracked insert must still be healed via the locked path"
            );
            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(revision.clone()))
                .await
                .unwrap();
            assert_eq!(
                digest_link.media_type.as_deref(),
                Some(MANIFEST_MEDIA_TYPE),
                "the legacy digest link must still gain the derived media_type"
            );
            assert!(
                digest_link.created_at.is_none(),
                "a legacy link batched with a refused tracked insert must keep created_at None"
            );
            test_case.cleanup().await;
        }
    }

    /// A lost `Digest` revision link with a live tag is restored from the tag's
    /// in-hand body: the self-link, the tracked closure, and the grants all
    /// come back before the sweep could classify against their absence.
    #[tokio::test]
    async fn stamp_tag_recreates_lost_digest_revision_link() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/lost-link").unwrap();
            let metadata_store = test_case.metadata_store();

            let (revision, _config, layer) =
                create_pushed_revision(&metadata_store, namespace).await;
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Tag(Tag::new("live").unwrap()),
                        revision.clone(),
                    )],
                )
                .await
                .unwrap();

            // Raw-delete the Digest revision link file (out-of-band loss).
            metadata_store
                .store()
                .delete(&path_builder::link_path(
                    &LinkKind::Digest(revision.clone()),
                    namespace,
                ))
                .await
                .unwrap();
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(revision.clone()))
                    .await
                    .is_err(),
                "precondition: the revision link is lost"
            );

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            let restored = metadata_store
                .read_link(namespace, &LinkKind::Digest(revision.clone()))
                .await
                .unwrap();
            assert_eq!(restored.target, revision);
            assert_eq!(
                restored.media_type.as_deref(),
                Some(MANIFEST_MEDIA_TYPE),
                "the restored revision link must carry the derived media_type"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Layer(layer.clone()))
                    .await
                    .is_ok(),
                "the layer closure must be intact after the repair"
            );
            assert!(
                metadata_store.has_blob_references(&layer).await.unwrap(),
                "the layer grant must still pin the bytes"
            );
            test_case.cleanup().await;
        }
    }

    /// A tag whose target body is absent is left to the missing-body sweep: no
    /// revision link is fabricated and the namespace is not failed.
    #[tokio::test]
    async fn stamp_tag_skips_lost_link_when_body_absent() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/lost-body").unwrap();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (revision, _config, _layer) =
                create_pushed_revision(&metadata_store, namespace).await;
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Tag(Tag::new("dangling").unwrap()),
                        revision.clone(),
                    )],
                )
                .await
                .unwrap();
            metadata_store
                .store()
                .delete(&path_builder::link_path(
                    &LinkKind::Digest(revision.clone()),
                    namespace,
                ))
                .await
                .unwrap();
            blob_store.delete_blob(&revision).await.unwrap();

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            checker
                .check(namespace, &mut sink)
                .await
                .expect("a body-less dangling tag must not fail the namespace");

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(revision.clone()))
                    .await
                    .is_err(),
                "no revision link may be fabricated without a body"
            );
            test_case.cleanup().await;
        }
    }

    /// A subject's stripped referrer back-link is re-derived, carrying the
    /// descriptor since `media_type` is set.
    #[tokio::test]
    async fn rebuild_derives_subject_referrer_backlink() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/referrer").unwrap();
            let metadata_store = test_case.metadata_store();

            let subject = put_blob_direct(metadata_store.store(), b"subject bytes").await;
            let manifest_content = format!(
                r#"{{
                "schemaVersion": 2,
                "mediaType": "{MANIFEST_MEDIA_TYPE}",
                "subject": {{
                    "mediaType": "{MANIFEST_MEDIA_TYPE}",
                    "digest": "{subject}",
                    "size": 13
                }},
                "config": {{
                    "mediaType": "application/vnd.oci.empty.v1+json",
                    "digest": "{subject}",
                    "size": 13
                }},
                "layers": []
            }}"#
            );
            let revision =
                put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;

            let mut manifest =
                parse_and_validate_manifest(manifest_content.as_bytes(), None).unwrap();
            let effective_media_type = manifest.media_type.clone();
            let ops = link_plan::push(
                &mut manifest,
                &revision,
                &Reference::Digest(revision.clone()),
                effective_media_type.as_ref(),
                manifest_content.len() as u64,
                &[],
            );
            metadata_store.seed_links(namespace, &ops).await.unwrap();

            // Strip the subject referrer back-link.
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::delete(LinkKind::Referrer(
                        subject.clone(),
                        revision.clone(),
                    ))],
                )
                .await
                .unwrap();
            assert!(
                metadata_store
                    .read_link(
                        namespace,
                        &LinkKind::Referrer(subject.clone(), revision.clone())
                    )
                    .await
                    .is_err(),
                "precondition: the referrer back-link is stripped"
            );

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            let referrer = metadata_store
                .read_link(
                    namespace,
                    &LinkKind::Referrer(subject.clone(), revision.clone()),
                )
                .await
                .unwrap();
            assert_eq!(referrer.target, revision);
            assert!(
                referrer.descriptor.is_some(),
                "the referrer must carry a descriptor since media_type is set"
            );
            test_case.cleanup().await;
        }
    }

    /// A revision whose manifest body is gone is left intact for the sweep.
    #[tokio::test]
    async fn rebuild_skips_missing_body_revision() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/missing-body").unwrap();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let manifest_content = format!(
                r#"{{"schemaVersion":2,"mediaType":"{MANIFEST_MEDIA_TYPE}","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0}},"layers":[]}}"#
            );
            let revision =
                put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;
            metadata_store
                .update_links(
                    namespace,
                    &[
                        LinkOperation::create(LinkKind::Digest(revision.clone()), revision.clone()),
                        LinkOperation::create(
                            LinkKind::Tag(Tag::new("latest").unwrap()),
                            revision.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            blob_store.delete_blob(&revision).await.unwrap();

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(revision.clone()))
                    .await
                    .is_ok(),
                "the revision self-link must be left intact for the sweep"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Tag(Tag::new("latest").unwrap()))
                    .await
                    .is_ok(),
                "the tag link must be left intact for the sweep"
            );
            test_case.cleanup().await;
        }
    }

    /// Under `--dry-run` the rebuild performs no write: a stripped grant stays
    /// stripped.
    #[tokio::test]
    async fn rebuild_dry_run_makes_no_writes() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/dry-run").unwrap();
            let metadata_store = test_case.metadata_store();

            let (_revision, _config, layer) =
                create_pushed_revision(&metadata_store, namespace).await;

            for link in [
                LinkKind::Layer(layer.clone()),
                LinkKind::Blob(layer.clone()),
            ] {
                metadata_store
                    .update_blob_index(namespace, &layer, BlobIndexOperation::Remove(link))
                    .await
                    .unwrap();
            }
            assert!(
                metadata_store
                    .read_blob_index_namespace(namespace, &layer)
                    .await
                    .is_err(),
                "precondition: the layer grant is stripped"
            );

            let checker = rebuild(test_case.as_ref(), true);
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert!(
                metadata_store
                    .read_blob_index_namespace(namespace, &layer)
                    .await
                    .is_err(),
                "--dry-run must not restore the stripped grant"
            );
            test_case.cleanup().await;
        }
    }

    /// A layer whose bytes are deleted is a dangling layer finding, with no
    /// delete and both links intact.
    #[tokio::test]
    async fn rebuild_reports_dangling_layer_without_deleting() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/dangling").unwrap();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (revision, _config, layer) =
                create_pushed_revision(&metadata_store, namespace).await;

            // Delete the layer's bytes only, leaving every link intact.
            blob_store.delete_blob(&layer).await.unwrap();

            let findings = Findings::default();
            let checker = RebuildChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                findings.clone(),
                false,
                CancellationToken::new(),
            );
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            let snapshot = findings.snapshot().await;
            assert_eq!(
                snapshot.dangling_layer, 1,
                "the layer whose bytes are absent must be a report-only finding"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(revision.clone()))
                    .await
                    .is_ok(),
                "the revision link must remain intact"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Layer(layer.clone()))
                    .await
                    .is_ok(),
                "the layer link must remain intact (a dangling ref is never deleted)"
            );
            test_case.cleanup().await;
        }
    }

    /// A base layer (and config) shared by two distinct revisions is byte-probed
    /// once per namespace: the dangling-reference presence memo collapses the
    /// repeated HEAD that would otherwise cost O(revisions x layers) round-trips.
    #[tokio::test]
    async fn dangling_probe_memoizes_shared_blob() {
        let root = TempDir::new().unwrap();
        let path = root.path().to_string_lossy().to_string();
        let counting = Arc::new(CountingObjectStore::new(Arc::new(
            StorageFsBackend::builder(path.as_str()).build(),
        )));
        let object: Arc<dyn ObjectStore> = counting.clone();
        let metadata_store = Arc::new(
            MetadataStore::builder(build_store(
                object.clone(),
                locked_executor_over(object.clone()),
            ))
            .link_cache_ttl(0)
            .access_time_debounce_secs(0)
            .build(),
        );
        let blob_store = Arc::new(BlobStore::new(build_store(
            object.clone(),
            locked_executor_over(object),
        )));
        let namespace = &Namespace::new("test-repo/shared-blob").unwrap();

        let config = put_blob_direct(metadata_store.store(), b"shared config bytes").await;
        let layer = put_blob_direct(metadata_store.store(), b"shared layer bytes").await;

        // Two distinct manifest bodies (distinct annotation => distinct revision)
        // that both reference the same config and layer.
        for tag in ["one", "two"] {
            let manifest_content = format!(
                r#"{{"schemaVersion":2,"mediaType":"{MANIFEST_MEDIA_TYPE}","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"{config}","size":19}},"layers":[{{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":"{layer}","size":18}}],"annotations":{{"tag":"{tag}"}}}}"#
            );
            let revision =
                put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;
            let mut manifest =
                parse_and_validate_manifest(manifest_content.as_bytes(), None).unwrap();
            let effective_media_type = manifest.media_type.clone();
            let ops = link_plan::push(
                &mut manifest,
                &revision,
                &Reference::Digest(revision.clone()),
                effective_media_type.as_ref(),
                manifest_content.len() as u64,
                &[],
            );
            metadata_store.seed_links(namespace, &ops).await.unwrap();
        }

        counting.reset();
        let checker = RebuildChecker::new(
            blob_store,
            metadata_store.clone(),
            Findings::default(),
            false,
            CancellationToken::new(),
        );
        let mut sink: Vec<Action> = Vec::new();
        checker.check(namespace, &mut sink).await.unwrap();

        assert_eq!(
            counting.heads_for(&path_builder::blob_path(&layer)),
            1,
            "a layer shared by two revisions must be byte-probed once per namespace"
        );
        assert_eq!(
            counting.heads_for(&path_builder::blob_path(&config)),
            1,
            "a config shared by two revisions must be byte-probed once per namespace"
        );
    }

    /// Seed one revision (digest self-link plus a tag) pointing at the given
    /// bytes without parsing them, so a body that would fail parse or is a future
    /// format can still be seeded as a current revision.
    async fn seed_revision_with_body(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
        body: &[u8],
    ) -> Digest {
        let revision = put_blob_direct(metadata_store.store(), body).await;
        metadata_store
            .update_links(
                namespace,
                &[
                    LinkOperation::create(LinkKind::Digest(revision.clone()), revision.clone()),
                    LinkOperation::create(
                        LinkKind::Tag(Tag::new("latest").unwrap()),
                        revision.clone(),
                    ),
                ],
            )
            .await
            .unwrap();
        revision
    }

    /// A present but non-JSON body fails the namespace, while a missing-body
    /// and a dangling-reference revision do not.
    #[tokio::test]
    async fn rebuild_fails_namespace_on_corrupt_body() {
        for test_case in backends() {
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            // Corrupt body => the namespace fails.
            let corrupt_ns = &Namespace::new("test-repo/corrupt").unwrap();
            seed_revision_with_body(&metadata_store, corrupt_ns, b"this is not json {{{").await;

            // Missing body => not failed (the sweep owns it).
            let missing_ns = &Namespace::new("test-repo/missing").unwrap();
            let missing_revision = create_pushed_revision(&metadata_store, missing_ns).await.0;
            blob_store.delete_blob(&missing_revision).await.unwrap();

            // Dangling reference => not failed (report-only).
            let dangling_ns = &Namespace::new("test-repo/dangling-fatal").unwrap();
            let (_rev, _config, layer) = create_pushed_revision(&metadata_store, dangling_ns).await;
            blob_store.delete_blob(&layer).await.unwrap();

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            assert!(
                matches!(
                    checker.check(corrupt_ns, &mut sink).await,
                    Err(Error::RebuildIncomplete(_))
                ),
                "a corrupt manifest body must fail the namespace"
            );
            checker
                .check(missing_ns, &mut sink)
                .await
                .expect("a missing-body revision must NOT fail the namespace (the sweep owns it)");
            checker
                .check(dangling_ns, &mut sink)
                .await
                .expect("a dangling config/layer reference must NOT fail the namespace");
            test_case.cleanup().await;
        }
    }

    /// A `schemaVersion: 3` body fails the namespace, a valid v2 body with
    /// annotations does not, and an unknown top-level field does.
    #[tokio::test]
    async fn rebuild_fails_namespace_on_future_format() {
        for test_case in backends() {
            let metadata_store = test_case.metadata_store();

            // Future schemaVersion => failed.
            let future_ns = &Namespace::new("test-repo/future-schema").unwrap();
            let future_body = format!(
                r#"{{"schemaVersion":3,"mediaType":"{MANIFEST_MEDIA_TYPE}","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0}},"layers":[]}}"#
            );
            seed_revision_with_body(&metadata_store, future_ns, future_body.as_bytes()).await;

            // Valid v2 body with annotations => not failed.
            let ok_ns = &Namespace::new("test-repo/v2-annotated").unwrap();
            let ok_body = format!(
                r#"{{"schemaVersion":2,"mediaType":"{MANIFEST_MEDIA_TYPE}","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0}},"layers":[],"annotations":{{"org.opencontainers.image.title":"ok"}}}}"#
            );
            seed_revision_with_body(&metadata_store, ok_ns, ok_body.as_bytes()).await;

            // Unknown top-level field => failed (the deny_unknown_fields probe).
            let unknown_ns = &Namespace::new("test-repo/unknown-field").unwrap();
            let unknown_body = format!(
                r#"{{"schemaVersion":2,"mediaType":"{MANIFEST_MEDIA_TYPE}","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0}},"layers":[],"futureField":"surprise"}}"#
            );
            seed_revision_with_body(&metadata_store, unknown_ns, unknown_body.as_bytes()).await;

            let checker = rebuild(test_case.as_ref(), false);
            let mut sink: Vec<Action> = Vec::new();
            assert!(
                matches!(
                    checker.check(future_ns, &mut sink).await,
                    Err(Error::RebuildIncomplete(_))
                ),
                "a future schemaVersion must fail the namespace"
            );
            checker
                .check(ok_ns, &mut sink)
                .await
                .expect("a valid v2 body with annotations must NOT fail the namespace");
            assert!(
                matches!(
                    checker.check(unknown_ns, &mut sink).await,
                    Err(Error::RebuildIncomplete(_))
                ),
                "an unknown top-level field must fail the namespace"
            );
            test_case.cleanup().await;
        }
    }

    /// A pre-cancelled run performs no rebuild writes and reports `Cancelled`,
    /// so the namespace counts as failed and the sweep keeps everything.
    #[tokio::test]
    async fn rebuild_cancelled_repairs_nothing() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/cancelled").unwrap();
            let metadata_store = test_case.metadata_store();

            let (revision, _config, layer) =
                create_pushed_revision(&metadata_store, namespace).await;
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::delete_with_referrer(
                        LinkKind::Layer(layer.clone()),
                        revision.clone(),
                    )],
                )
                .await
                .unwrap();

            let cancel = CancellationToken::new();
            cancel.cancel();
            let checker = RebuildChecker::new(
                test_case.blob_store(),
                metadata_store.clone(),
                Findings::default(),
                false,
                cancel,
            );
            let mut sink: Vec<Action> = Vec::new();
            assert!(
                matches!(
                    checker.check(namespace, &mut sink).await,
                    Err(Error::Cancelled)
                ),
                "a cancelled rebuild must surface Cancelled"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Layer(layer.clone()))
                    .await
                    .is_err(),
                "a cancelled rebuild must not repair anything"
            );
            test_case.cleanup().await;
        }
    }

    /// A same-target digest create through `update_links` re-puts the link body
    /// with `media_type` even when the target is unchanged, pinning the
    /// keep-if-equal restamp at the store level.
    #[tokio::test]
    async fn update_links_restamps_media_type_on_same_target_create() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/restamp").unwrap();
            let metadata_store = test_case.metadata_store();

            let digest = put_blob_direct(metadata_store.store(), b"restamp body").await;
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(digest.clone()),
                        digest.clone(),
                    )],
                )
                .await
                .unwrap();
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(digest.clone()))
                    .await
                    .unwrap()
                    .media_type
                    .is_none(),
                "precondition: no media_type"
            );

            let media_type = MediaType::new(MANIFEST_MEDIA_TYPE).unwrap();
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create_with_media_type(
                        LinkKind::Digest(digest.clone()),
                        digest.clone(),
                        Some(media_type.clone()),
                    )],
                )
                .await
                .unwrap();

            let after = metadata_store
                .read_link(namespace, &LinkKind::Digest(digest.clone()))
                .await
                .unwrap();
            assert_eq!(after.media_type, Some(media_type));
            test_case.cleanup().await;
        }
    }

    /// The guarded restamp drops the op when the stored tag moved to another
    /// target: the concurrent writer wins and nothing is re-pointed.
    #[tokio::test]
    async fn restamp_links_noops_when_target_moved() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/moved-tag").unwrap();
            let metadata_store = test_case.metadata_store();

            let old_target = put_blob_direct(metadata_store.store(), b"old target body").await;
            let new_target = put_blob_direct(metadata_store.store(), b"new target body").await;
            let tag = LinkKind::Tag(Tag::new("racing").unwrap());
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(tag.clone(), new_target.clone())],
                )
                .await
                .unwrap();

            // A restamp planned against the OLD target (the tag moved since the
            // caller's read) must be dropped, not re-pointed.
            metadata_store
                .restamp_links(
                    namespace,
                    &[LinkOperation::create_with_media_type(
                        tag.clone(),
                        old_target.clone(),
                        Some(MediaType::new(MANIFEST_MEDIA_TYPE).unwrap()),
                    )],
                )
                .await
                .unwrap();

            let after = metadata_store.read_link(namespace, &tag).await.unwrap();
            assert_eq!(
                after.target, new_target,
                "the moved tag must keep its new target (the concurrent writer wins)"
            );
            assert!(
                after.media_type.is_none(),
                "the dropped restamp must not stamp the moved tag"
            );
            test_case.cleanup().await;
        }
    }
}
