//! The namespace / tag / revision / referrer catalog: the content-derived
//! enumeration endpoints and the namespace tree-walk they build on.

use futures_util::stream::{self, Stream, StreamExt, TryStreamExt};
use tracing::{debug, instrument};

use angos_storage::paginated;

use crate::{
    oci::{Algorithm, Digest, Namespace, Tag},
    registry::{
        Error,
        metadata_store::{LinkKind, MetadataStore},
        pagination, path_builder,
    },
};

/// Fan-out for the tag link reads behind [`MetadataStore::find_tags_pointing_at`].
const TAG_LINK_READ_CONCURRENCY: usize = 20;

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

        let mut namespaces = self.collect_namespaces(None).await?;
        namespaces.sort_unstable();

        Ok(pagination::paginate_sorted(&namespaces, n, last.as_deref()))
    }

    /// Walks the manifest catalog in a single concurrent tree walk and returns
    /// every namespace, unpaginated and unsorted. `scope` restricts the walk to
    /// one repository's subtree; `None` walks the whole store.
    #[instrument(skip(self))]
    pub async fn collect_namespaces(&self, scope: Option<&str>) -> Result<Vec<String>, Error> {
        let (root, prefix) = path_builder::namespace_walk_root(scope);

        pagination::collect_namespaces_with_marker(
            &root,
            &prefix,
            "_manifests",
            self.namespace_walk_concurrency,
            |path| async move {
                let (sub_prefixes, _) =
                    self.store().object_store().list_all_children(&path).await?;
                Ok(sub_prefixes)
            },
        )
        .await
    }

    #[instrument(skip(self))]
    pub async fn list_tags(
        &self,
        namespace: &Namespace,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<Tag>, Option<String>), Error> {
        debug!("Listing {n} tag(s) for namespace '{namespace}' starting with last '{last:?}'");

        let mut tags: Vec<Tag> = self.stream_tags(namespace).try_collect().await?;
        tags.sort();

        Ok(pagination::paginate_sorted(&tags, n, last.as_deref()))
    }

    /// Streams every valid tag in `namespace`, unsorted, sourced from the
    /// backend's complete children enumeration (one directory read on FS,
    /// concurrent name-range scans on S3). Tags are validated on write; a
    /// malformed directory name here is defensive and is dropped rather than
    /// surfaced as a tag (scrub reports and removes such directories, so the
    /// drop is silent).
    pub fn stream_tags(
        &self,
        namespace: &Namespace,
    ) -> impl Stream<Item = Result<Tag, Error>> + Send + '_ {
        let tags_dir = path_builder::manifest_tags_dir(namespace);
        stream::once(async move {
            let (tag_dirs, _) = self
                .store()
                .object_store()
                .list_all_children(&tags_dir)
                .await?;
            Ok::<_, Error>(stream::iter(
                tag_dirs
                    .into_iter()
                    .filter_map(|name| Tag::try_from(name).ok())
                    .map(Ok),
            ))
        })
        .try_flatten()
    }

    /// Lists the RAW tag directory names in `namespace` with NO `Tag`
    /// validation, so tests can observe directories whose names do not
    /// satisfy the `oci::Tag` grammar, which [`Self::list_tags`] silently
    /// drops (production code walks raw keys through scrub's categorizer).
    #[cfg(test)]
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
    /// for [`Self::list_tag_names`]'s no-validation contract.
    #[cfg(test)]
    async fn collect_tag_dir_names(&self, namespace: &Namespace) -> Result<Vec<String>, Error> {
        let tags_dir = path_builder::manifest_tags_dir(namespace);
        let (tag_dirs, _) = self
            .store()
            .object_store()
            .list_all_children(&tags_dir)
            .await?;
        Ok(tag_dirs)
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
        // Tags stream off the listing straight into the link reads; a tag
        // whose read fails is skipped rather than matched.
        self.stream_tags(namespace)
            .map_ok(|tag| async move {
                let result = self
                    .read_link_reference(namespace, &LinkKind::Tag(tag.clone()))
                    .await;
                Ok::<_, Error>((tag, result))
            })
            .try_buffer_unordered(TAG_LINK_READ_CONCURRENCY)
            .try_filter_map(|(tag, result)| async move {
                Ok(match result {
                    Ok(metadata) if &metadata.target == digest => Some(LinkKind::Tag(tag)),
                    _ => None,
                })
            })
            .try_collect()
            .await
    }

    /// Streams the candidate referrer manifest digests recorded under
    /// `digest`'s referrers directory, unresolved and unordered. Callers
    /// resolve each candidate to a descriptor at registry altitude, where the
    /// blob store holding manifest bodies is in reach.
    pub fn stream_referrer_digests(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> impl Stream<Item = Result<Digest, Error>> + Send + '_ {
        let referrers_dir = path_builder::manifest_referrers_dir(namespace, digest);
        paginated(move |token| {
            let referrers_dir = referrers_dir.clone();
            async move {
                let page = self
                    .store()
                    .object_store()
                    .list(&referrers_dir, 100, token)
                    .await?;
                let digest_entries = page
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
                Ok((digest_entries, page.next_token))
            }
        })
    }

    pub async fn has_referrers(
        &self,
        namespace: &Namespace,
        subject: &Digest,
    ) -> Result<bool, Error> {
        let referrers_dir = path_builder::manifest_referrers_dir(namespace, subject);
        let page = self
            .store()
            .object_store()
            .list(&referrers_dir, 1, None)
            .await?;
        Ok(!page.items.is_empty())
    }

    /// Streams every manifest revision digest in `namespace` lazily: the
    /// per-algorithm shards (`revisions/<algo>/<hash>`) are chained in
    /// algorithm order, at most one listing page buffered.
    pub fn stream_revisions<'a>(
        &'a self,
        namespace: &'a Namespace,
    ) -> impl Stream<Item = Result<Digest, Error>> + Send + 'a {
        stream::iter(Algorithm::supported_algorithms()).flat_map(move |algorithm| {
            let revisions_dir =
                path_builder::manifest_revisions_link_root_dir(namespace, algorithm.as_str());
            paginated(move |token| {
                let revisions_dir = revisions_dir.clone();
                async move {
                    let page = self
                        .store()
                        .object_store()
                        .list_children(&revisions_dir, 1000, token, None)
                        .await?;
                    let revisions = page
                        .sub_prefixes
                        .into_iter()
                        .filter_map(|key| Digest::with_algorithm(*algorithm, key).ok())
                        .collect();
                    Ok((revisions, page.next_token))
                }
            })
        })
    }

    pub async fn count_manifests(&self, namespace: &Namespace) -> Result<usize, Error> {
        self.stream_revisions(namespace)
            .try_fold(0, |count, _| async move { Ok(count + 1) })
            .await
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
            return Err(Error::Internal(format!(
                "unsafe tag directory name: '{tag_name}'"
            )));
        }
        self.store()
            .object_store()
            .delete_prefix(&path_builder::manifest_tag_dir(namespace, tag_name))
            .await
            .map_err(Error::from)
    }

    /// Delete a namespace's entire repository subtree by raw on-disk name. Used
    /// by scrub to reclaim a directory whose name fails `Namespace` validation
    /// and so cannot form typed links for a per-link delete.
    pub async fn delete_namespace_directory(&self, name: &str) -> Result<(), Error> {
        let prefix = path_builder::namespace_dir(name)
            .ok_or_else(|| Error::Internal(format!("unsafe namespace directory name: '{name}'")))?;
        self.store()
            .object_store()
            .delete_prefix(&prefix)
            .await
            .map_err(Error::from)
    }
}
