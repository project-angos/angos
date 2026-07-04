//! The namespace / tag / revision / referrer catalog: the content-derived
//! enumeration endpoints, the namespace tree-walk they build on, and the prune
//! of the dead pre-1.3 `_registry/` namespace index.

use std::io;

use futures_util::stream::{self, StreamExt};
use tracing::{debug, instrument};

use angos_tx_engine::StorageError;

use crate::{
    oci::{Algorithm, Descriptor, Digest, Namespace, Tag},
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
    /// Lists the namespaces holding manifest content (a `_manifests` child);
    /// an `_uploads`-only namespace is not a catalog entry and is discovered
    /// through the blob store instead, where upload sessions live.
    #[instrument(skip(self))]
    pub async fn list_namespaces(
        &self,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        debug!("Fetching {n} namespace(s) with continuation token: {last:?}");

        let namespaces = pagination::collect_namespaces_with_marker(
            path_builder::repository_dir(),
            "_manifests",
            |path, token| async move {
                self.store()
                    .list_children(&path, 1000, token, None)
                    .await
                    .map_err(Error::from)
            },
        )
        .await?;

        Ok(pagination::paginate_sorted(&namespaces, n, last.as_deref()))
    }

    #[instrument(skip(self))]
    pub async fn list_tags(
        &self,
        namespace: &Namespace,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<Tag>, Option<String>), Error> {
        debug!("Listing {n} tag(s) for namespace '{namespace}' starting with last '{last:?}'");

        // Tags are validated on write; a malformed directory name here is
        // defensive and is dropped rather than surfaced as a tag. Scrub now
        // reports and removes such directories, so the drop is silent.
        let mut tags: Vec<Tag> = self
            .collect_tag_dir_names(namespace)
            .await?
            .into_iter()
            .filter_map(|name| Tag::try_from(name).ok())
            .collect();
        tags.sort();

        Ok(pagination::paginate_sorted(&tags, n, last.as_deref()))
    }

    /// Lists the RAW tag directory names in `namespace` with NO `Tag`
    /// validation. Scrub enumerates these so it can detect (and delete)
    /// directories whose names do not satisfy the `oci::Tag` grammar, which
    /// [`Self::list_tags`] silently drops.
    #[instrument(skip(self))]
    pub async fn list_tag_names(
        &self,
        namespace: &Namespace,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        debug!("Listing {n} tag name(s) for namespace '{namespace}' starting with last '{last:?}'");

        let mut names = self.collect_tag_dir_names(namespace).await?;
        names.sort();

        Ok(pagination::paginate_sorted(&names, n, last.as_deref()))
    }

    /// Enumerates every raw tag directory name under `namespace`'s tags dir.
    /// Paginates in memory like `list_namespaces`: backend `start-after`
    /// ordering and exclusive-`last` semantics aren't portable across backends.
    async fn collect_tag_dir_names(&self, namespace: &Namespace) -> Result<Vec<String>, Error> {
        let tags_dir = path_builder::manifest_tags_dir(namespace);

        let mut names = Vec::new();
        let mut token = None;
        loop {
            let page = self
                .store()
                .list_children(&tags_dir, 1000, token, None)
                .await?;
            names.extend(page.sub_prefixes);
            match page.next_token {
                Some(next) => token = Some(next),
                None => break,
            }
        }

        Ok(names)
    }

    /// Returns the `LinkKind::Tag` entries in `namespace` that currently point at
    /// `digest`. Reads bypass the link cache, since this set gates the
    /// digest-delete LWW guard and must not omit a tag re-pointed on another
    /// replica within the cache TTL.
    #[instrument(skip(self))]
    pub async fn find_tags_pointing_at(
        &self,
        namespace: &Namespace,
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
        namespace: &Namespace,
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

    pub async fn has_referrers(
        &self,
        namespace: &Namespace,
        subject: &Digest,
    ) -> Result<bool, Error> {
        let referrers_dir = path_builder::manifest_referrers_dir(namespace, subject);
        let page = self.store().list(&referrers_dir, 1, None).await?;
        Ok(!page.items.is_empty())
    }

    pub async fn list_revisions(
        &self,
        namespace: &Namespace,
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

    pub async fn count_manifests(&self, namespace: &Namespace) -> Result<usize, Error> {
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

    /// Delete an entire tag directory by prefix. Used by scrub for an invalid
    /// tag name, which cannot form a typed `LinkKind::Tag` for a link delete.
    ///
    /// `tag_name` must be a single path segment: a name containing `/`, `..`, or
    /// `.` could escape the tags directory and delete an unrelated prefix, so it
    /// is rejected rather than deleted.
    pub async fn delete_tag_directory(
        &self,
        namespace: &Namespace,
        tag_name: &str,
    ) -> Result<(), Error> {
        if tag_name.is_empty() || tag_name.contains('/') || tag_name == "." || tag_name == ".." {
            return Err(Error::InvalidData(format!(
                "unsafe tag directory name: '{tag_name}'"
            )));
        }
        self.store()
            .delete_prefix(&path_builder::manifest_tag_dir(namespace, tag_name))
            .await
            .map_err(Error::from)
    }

    /// Delete a namespace's entire repository subtree by raw on-disk name. Used
    /// by scrub to reclaim a directory whose name fails `Namespace` validation
    /// and so cannot form typed links for a per-link delete.
    pub async fn delete_namespace_directory(&self, name: &str) -> Result<(), Error> {
        let prefix = path_builder::namespace_dir(name).ok_or_else(|| {
            Error::InvalidData(format!("unsafe namespace directory name: '{name}'"))
        })?;
        self.store()
            .delete_prefix(&prefix)
            .await
            .map_err(Error::from)
    }
}
