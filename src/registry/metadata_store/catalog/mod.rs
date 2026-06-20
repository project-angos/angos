//! The namespace / tag / revision / referrer catalog: the content-derived
//! enumeration endpoints, the namespace tree-walk they build on, and the prune
//! of the dead pre-1.3 `_registry/` namespace index.

use std::io;

use futures_util::stream::{self, StreamExt};
use tracing::{debug, instrument};

use angos_tx_engine::StorageError;

use crate::{
    oci::{Algorithm, Descriptor, Digest},
    registry::{
        metadata_store::{Error, LinkKind, MetadataStore},
        pagination,
        pagination::collect_all_pages,
        path_builder,
    },
};

mod referrer_resolver;
use referrer_resolver::resolve_referrer_descriptor;

/// Prefix of the pre-1.3 maintained namespace-registry index. The catalog is now
/// derived from content, so these objects are dead and are pruned by scrub.
const LEGACY_NAMESPACE_REGISTRY_PREFIX: &str = "_registry";

impl MetadataStore {
    #[instrument(skip(self))]
    pub async fn list_namespaces(
        &self,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        debug!("Fetching {n} namespace(s) with continuation token: {last:?}");

        let namespaces = self
            .collect_namespaces(path_builder::repository_dir(), "")
            .await?;

        Ok(pagination::paginate_sorted(&namespaces, n, last.as_deref()))
    }

    /// Lists namespaces holding an `_uploads` directory; unlike
    /// [`Self::list_namespaces`] these include namespaces with no manifest
    /// content, which orphan-namespace scrub needs to sweep stranded uploads.
    #[instrument(skip(self))]
    pub async fn list_upload_namespaces(
        &self,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        debug!("Fetching {n} upload namespace(s) with continuation token: {last:?}");

        let namespaces = self
            .collect_namespaces_with_marker(path_builder::repository_dir(), "", "_uploads")
            .await?;

        Ok(pagination::paginate_sorted(&namespaces, n, last.as_deref()))
    }

    #[instrument(skip(self))]
    pub async fn list_tags(
        &self,
        namespace: &str,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        debug!("Listing {n} tag(s) for namespace '{namespace}' starting with last '{last:?}'");
        let tags_dir = path_builder::manifest_tags_dir(namespace);

        // Paginate in memory like `list_namespaces`: backend `start-after`
        // ordering and exclusive-`last` semantics aren't portable across backends.
        let mut tags = Vec::new();
        let mut token = None;
        loop {
            let page = self
                .store()
                .list_children(&tags_dir, 1000, token, None)
                .await?;
            tags.extend(page.sub_prefixes);
            match page.next_token {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        tags.sort();

        Ok(pagination::paginate_sorted(&tags, n, last.as_deref()))
    }

    /// Returns the `LinkKind::Tag` entries in `namespace` that currently point at
    /// `digest`. Reads bypass the link cache, since this set gates the
    /// digest-delete LWW guard and must not omit a tag re-pointed on another
    /// replica within the cache TTL.
    #[instrument(skip(self))]
    pub async fn find_tags_pointing_at(
        &self,
        namespace: &str,
        digest: &Digest,
    ) -> Result<Vec<LinkKind>, Error> {
        let all_tags =
            collect_all_pages(|marker| async move { self.list_tags(namespace, 100, marker).await })
                .await?;

        let matching = stream::iter(all_tags)
            .map(|tag| async move {
                let result = self
                    .read_link_reference(namespace, &LinkKind::Tag(tag.clone()))
                    .await;
                (tag, result)
            })
            .buffer_unordered(20)
            .filter_map(|(tag, result)| async move {
                if let Ok(metadata) = result
                    && &metadata.target == digest
                {
                    Some(LinkKind::Tag(tag))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .await;

        Ok(matching)
    }

    #[instrument(skip(self))]
    pub async fn list_referrers(
        &self,
        namespace: &str,
        digest: &Digest,
        artifact_type: Option<String>,
    ) -> Result<Vec<Descriptor>, Error> {
        let referrers_dir = path_builder::manifest_referrers_dir(namespace, digest);

        let mut referrers = Vec::new();
        let mut token = None;

        loop {
            let page = self.store().list(&referrers_dir, 100, token).await?;

            let digest_entries: Vec<Digest> = page
                .items
                .iter()
                .filter_map(|key| {
                    let parts: Vec<&str> = key.split('/').collect();
                    if parts.len() < 2 {
                        return None;
                    }
                    let algorithm = parts[0].parse::<Algorithm>().ok()?;
                    Digest::with_algorithm(algorithm, parts[1]).ok()
                })
                .collect();

            let results: Vec<Option<Descriptor>> = stream::iter(digest_entries)
                .map(|manifest_digest| {
                    let artifact_type = artifact_type.as_ref();
                    async move {
                        resolve_referrer_descriptor(
                            digest,
                            manifest_digest,
                            artifact_type,
                            |link| async move { self.read_link_reference(namespace, &link).await },
                            |path| async move {
                                self.store().get(&path).await.map_err(|e| match e {
                                    StorageError::NotFound => {
                                        io::Error::new(io::ErrorKind::NotFound, e.to_string())
                                    }
                                    other => io::Error::other(other.to_string()),
                                })
                            },
                        )
                        .await
                    }
                })
                .buffer_unordered(10)
                .collect()
                .await;

            referrers.extend(results.into_iter().flatten());

            token = page.next_token;
            if token.is_none() {
                break;
            }
        }

        referrers.sort_by(|a, b| a.digest.cmp(&b.digest));
        Ok(referrers)
    }

    pub async fn has_referrers(&self, namespace: &str, subject: &Digest) -> Result<bool, Error> {
        let referrers_dir = path_builder::manifest_referrers_dir(namespace, subject);
        let page = self.store().list(&referrers_dir, 1, None).await?;
        Ok(!page.items.is_empty())
    }

    pub async fn list_revisions(
        &self,
        namespace: &str,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<Digest>, Option<String>), Error> {
        debug!(
            "Fetching {n} revision(s) for namespace '{namespace}' with continuation token: {continuation_token:?}"
        );

        // Manifest revisions are sharded by algorithm (`revisions/<algo>/<hash>`).
        pagination::paginate_by_algorithm(
            n,
            continuation_token,
            |algorithm, limit, cursor| async move {
                let revisions_dir =
                    path_builder::manifest_revisions_link_root_dir(namespace, algorithm.as_str());
                let page = self
                    .store()
                    .list_children(&revisions_dir, limit, cursor, None)
                    .await?;
                let revisions = page
                    .sub_prefixes
                    .into_iter()
                    .filter_map(|key| Digest::with_algorithm(algorithm, key).ok())
                    .collect();
                Ok((revisions, page.next_token))
            },
        )
        .await
    }

    pub async fn count_manifests(&self, namespace: &str) -> Result<usize, Error> {
        let mut count = 0;
        for &algorithm in Algorithm::supported_algorithms() {
            let revisions_dir =
                path_builder::manifest_revisions_link_root_dir(namespace, algorithm.as_str());
            let mut token = None;
            loop {
                let page = self
                    .store()
                    .list_children(&revisions_dir, 1000, token, None)
                    .await?;

                count += page.sub_prefixes.len();
                token = page.next_token;
                if token.is_none() {
                    break;
                }
            }
        }

        Ok(count)
    }

    /// Delete the dead pre-1.3 namespace-registry objects (`_registry/`).
    /// Idempotent: a no-op once the prefix is gone.
    pub async fn delete_legacy_namespace_registry(&self) -> Result<(), Error> {
        self.store()
            .delete_prefix(LEGACY_NAMESPACE_REGISTRY_PREFIX)
            .await
            .map_err(Error::from)
    }

    /// Walk the repository tree under `root_path` and yield every path that is a
    /// namespace, i.e. has a `_manifests` child (an `_uploads`-only path is
    /// skipped). `_`-prefixed children are never descended into, so
    /// manifest/upload/blob substructure is not mistaken for nested namespaces.
    async fn collect_namespaces(
        &self,
        root_path: &str,
        root_prefix: &str,
    ) -> Result<Vec<String>, Error> {
        self.collect_namespaces_with_marker(root_path, root_prefix, "_manifests")
            .await
    }

    /// Like [`Self::collect_namespaces`] but keys a namespace off the given
    /// `marker` child; orphan-namespace scrub passes `_uploads` to reach
    /// upload-only namespaces that the `_manifests`-keyed catalog omits.
    async fn collect_namespaces_with_marker(
        &self,
        root_path: &str,
        root_prefix: &str,
        marker: &str,
    ) -> Result<Vec<String>, Error> {
        let mut stack: Vec<(String, String)> =
            vec![(root_path.to_string(), root_prefix.to_string())];
        let mut namespaces = Vec::new();

        while let Some((path, prefix)) = stack.pop() {
            let mut token = None;
            let mut is_namespace = false;
            let mut children = Vec::new();
            loop {
                let page = self.store().list_children(&path, 1000, token, None).await?;

                for entry in &page.sub_prefixes {
                    if entry == marker {
                        is_namespace = true;
                        continue;
                    }
                    if entry.starts_with('_') {
                        continue;
                    }
                    let child_path = format!("{path}/{entry}");
                    let child_prefix = format!("{prefix}{entry}/");
                    children.push((child_path, child_prefix));
                }

                token = page.next_token;
                if token.is_none() {
                    break;
                }
            }

            if is_namespace {
                let namespace = prefix.strip_suffix('/').unwrap_or(&prefix);
                if !namespace.is_empty() {
                    namespaces.push(namespace.to_string());
                }
            }
            for child in children.into_iter().rev() {
                stack.push(child);
            }
        }

        Ok(namespaces)
    }
}
