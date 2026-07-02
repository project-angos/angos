//! The namespace / tag / revision / referrer catalog: content-derived
//! enumeration endpoints, the namespace tree-walk they build on, and the prune
//! of the dead pre-1.3 `_registry/` index.

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
        path_builder::{self, NAMESPACE_MARKERS},
    },
};

mod referrer_resolver;
use referrer_resolver::resolve_referrer_descriptor;

/// Prefix of the dead pre-1.3 namespace-registry index; the catalog is now
/// content-derived, so scrub prunes these objects.
const LEGACY_NAMESPACE_REGISTRY_PREFIX: &str = "_registry";

/// Parse a `<algorithm>/<hash>[/...]` referrers-dir entry into the referrer
/// manifest digest, ignoring trailing segments (e.g. `/link`).
fn parse_referrer_digest(key: &str) -> Option<Digest> {
    let mut parts = key.split('/');
    let algorithm = parts.next()?.parse::<Algorithm>().ok()?;
    Digest::with_algorithm(algorithm, parts.next()?).ok()
}

impl MetadataStore {
    #[instrument(skip(self))]
    pub async fn list_namespaces(
        &self,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        debug!("Fetching {n} namespace(s) with continuation token: {last:?}");

        let namespaces = self
            .collect_namespaces_with_any_marker(path_builder::repository_dir(), "", &["_manifests"])
            .await?;

        Ok(pagination::paginate_sorted(&namespaces, n, last.as_deref()))
    }

    /// Every namespace marked by any registry subtree, sorted, from ONE tree
    /// walk. Unlike [`Self::list_namespaces`] (keyed off `_manifests` only) it
    /// also visits a namespace holding only stranded uploads or dangling
    /// layer/config links.
    pub async fn collect_all_namespaces(&self) -> Result<Vec<String>, Error> {
        let mut namespaces = self
            .collect_namespaces_with_any_marker(
                path_builder::repository_dir(),
                "",
                &NAMESPACE_MARKERS,
            )
            .await?;
        namespaces.sort();
        Ok(namespaces)
    }

    #[instrument(skip(self))]
    pub async fn list_tags(
        &self,
        namespace: &Namespace,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<Tag>, Option<String>), Error> {
        debug!("Listing {n} tag(s) for namespace '{namespace}' starting with last '{last:?}'");

        // Tags are validated on write; a malformed directory name is dropped
        // rather than surfaced as a tag.
        let mut tags: Vec<Tag> = self
            .collect_tag_dir_names(namespace)
            .await?
            .into_iter()
            .filter_map(|name| Tag::try_from(name).ok())
            .collect();
        tags.sort();

        Ok(pagination::paginate_sorted(&tags, n, last.as_deref()))
    }

    /// Lists the raw tag directory names in `namespace` without `Tag` validation,
    /// so scrub can detect and delete directories that fail the `oci::Tag` grammar
    /// (which [`Self::list_tags`] silently drops).
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

    /// Enumerates every raw tag directory name under `namespace`'s tags dir,
    /// looping pagination to exhaustion.
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

    /// The `LinkKind::Tag` entries in `namespace` pointing at `digest`. Reads
    /// bypass the link cache so a tag re-pointed on another replica within the
    /// cache TTL is not omitted from the digest-delete LWW guard.
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
                .filter_map(|key| parse_referrer_digest(key))
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

    /// Enumerate the raw referrer manifest digests under `subject`, resolving no
    /// descriptors, so scrub can see and reclaim orphan referrer links that
    /// [`Self::list_referrers`] silently drops.
    pub async fn list_referrer_digests(
        &self,
        namespace: &Namespace,
        subject: &Digest,
    ) -> Result<Vec<Digest>, Error> {
        let referrers_dir = path_builder::manifest_referrers_dir(namespace, subject);
        let mut digests = Vec::new();
        let mut token = None;
        loop {
            let page = self.store().list(&referrers_dir, 100, token).await?;
            digests.extend(
                page.items
                    .iter()
                    .filter_map(|key| parse_referrer_digest(key)),
            );
            token = page.next_token;
            if token.is_none() {
                break;
            }
        }
        Ok(digests)
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

        // Revisions are sharded by algorithm (`revisions/<algo>/<hash>`).
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

    /// Enumerate a namespace's layer links (`_layers/<algo>/<hash>`), sharded by
    /// algorithm like [`Self::list_revisions`], so scrub can reach layer links no
    /// current manifest references.
    pub async fn list_layer_links(
        &self,
        namespace: &Namespace,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<Digest>, Option<String>), Error> {
        self.list_sharded_links(n, continuation_token, |algorithm| {
            path_builder::layers_link_root_dir(namespace, algorithm.as_str())
        })
        .await
    }

    /// Enumerate a namespace's config links (`_config/<algo>/<hash>`), sharded by
    /// algorithm like [`Self::list_revisions`].
    pub async fn list_config_links(
        &self,
        namespace: &Namespace,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<Digest>, Option<String>), Error> {
        self.list_sharded_links(n, continuation_token, |algorithm| {
            path_builder::config_link_root_dir(namespace, algorithm.as_str())
        })
        .await
    }

    /// Shared driver for the algorithm-sharded link enumerators: maps each
    /// `root_dir(algorithm)`'s `<hash>` children to digests.
    async fn list_sharded_links<F>(
        &self,
        n: u16,
        continuation_token: Option<String>,
        root_dir: F,
    ) -> Result<(Vec<Digest>, Option<String>), Error>
    where
        F: Fn(Algorithm) -> String,
    {
        pagination::paginate_by_algorithm(n, continuation_token, |algorithm, limit, cursor| {
            let dir = root_dir(algorithm);
            async move {
                let page = self
                    .store()
                    .list_children(&dir, limit, cursor, None)
                    .await?;
                let digests = page
                    .sub_prefixes
                    .into_iter()
                    .filter_map(|key| Digest::with_algorithm(algorithm, key).ok())
                    .collect();
                Ok((digests, page.next_token))
            }
        })
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

    /// Delete an entire tag directory by prefix, for a scrub of an invalid tag
    /// name that cannot form a typed `LinkKind::Tag`. `tag_name` must be a single
    /// path segment; a `/`, `.`, or `..` that could escape the tags dir is
    /// rejected.
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

    /// Delete a namespace's repository subtree by raw on-disk name, for a scrub of
    /// a directory whose name fails `Namespace` validation.
    pub async fn delete_namespace_directory(&self, name: &str) -> Result<(), Error> {
        let prefix = path_builder::namespace_dir(name).ok_or_else(|| {
            Error::InvalidData(format!("unsafe namespace directory name: '{name}'"))
        })?;
        self.store()
            .delete_prefix(&prefix)
            .await
            .map_err(Error::from)
    }

    /// Walk the repository tree under `root_path`, yielding every path holding
    /// any of `markers` as a child. `_`-prefixed children are never descended,
    /// so manifest/upload/blob substructure is not mistaken for nested
    /// namespaces.
    async fn collect_namespaces_with_any_marker(
        &self,
        root_path: &str,
        root_prefix: &str,
        markers: &[&str],
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
                    if markers.contains(&entry.as_str()) {
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
