//! The `/_ext` admin surface: repository/namespace info for the web UI and
//! the jobs list/retry/delete responses, assembled as [`JsonResponse`] values.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tracing::instrument;

use crate::{
    configuration::RegexPattern,
    jobs::store as job_store,
    jobs::{JobState, Queue},
    oci::{
        DOCKER_REFERENCE_DIGEST, Descriptor, Digest, IN_TOTO_PREDICATE_TYPE, Manifest, MediaType,
        Namespace, Platform as OciPlatform, Tag, namespace_belongs_to,
    },
    registry::{
        APPLICATION_JSON, Error, HeaderMap, JsonResponse, Registry, ResponseHeaders,
        metadata_store::LinkKind, pagination::collect_all_pages,
    },
};

/// Default page size for the durable job-queue listing endpoints when the
/// client supplies no `?n=`. Bounded so an admin scan reads at most this many
/// envelope bodies per request.
const DEFAULT_JOBS_PAGE: u16 = 100;

fn json_headers() -> HeaderMap {
    ResponseHeaders::new()
        .content_type(APPLICATION_JSON)
        .into_inner()
}

#[derive(Serialize, Debug)]
struct RepositoryInfo {
    name: String,
    namespace_count: usize,
    pull_through_cache: bool,
    immutable_tags: bool,
}

#[derive(Serialize, Debug)]
struct RepositoriesBody {
    repositories: Vec<RepositoryInfo>,
}

#[derive(Serialize, Debug)]
struct NamespaceInfo {
    name: String,
    manifest_count: usize,
    upload_count: usize,
}

#[derive(Serialize, Debug)]
struct NamespacesBody {
    repository: String,
    namespaces: Vec<NamespaceInfo>,
    pull_through_cache: bool,
    upstream_urls: Vec<String>,
    immutable_tags: bool,
    immutable_tags_exclusions: Vec<RegexPattern>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
struct ExtPlatform {
    os: String,
    architecture: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    variant: Option<String>,
}

impl From<OciPlatform> for ExtPlatform {
    fn from(p: OciPlatform) -> Self {
        ExtPlatform {
            os: p.os,
            architecture: p.architecture,
            variant: p.variant,
        }
    }
}

#[derive(Serialize, Debug, Clone)]
struct ParentRef {
    digest: String,
    tags: Vec<Tag>,
    #[serde(skip_serializing_if = "Option::is_none")]
    platform: Option<ExtPlatform>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct ReferrerInfo {
    digest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact_type: Option<MediaType>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    annotations: HashMap<String, String>,
}

impl From<Descriptor> for ReferrerInfo {
    fn from(descriptor: Descriptor) -> Self {
        Self {
            digest: descriptor.digest.to_string(),
            artifact_type: descriptor.artifact_type,
            annotations: descriptor.annotations,
        }
    }
}

#[derive(Serialize, Debug)]
struct ManifestEntry {
    digest: String,
    tags: Vec<Tag>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    parents: Vec<ParentRef>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    referrers: Vec<ReferrerInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pushed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_pulled_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Debug)]
struct RevisionsBody {
    name: String,
    manifests: Vec<ManifestEntry>,
}

#[derive(Serialize, Debug)]
struct UploadEntry {
    uuid: String,
    size: u64,
    started_at: DateTime<Utc>,
}

#[derive(Serialize, Debug)]
struct UploadsBody {
    name: String,
    uploads: Vec<UploadEntry>,
}

/// A pending (or in-flight) durable job. `storage_key` is the opaque-to-the-UI
/// address used by the retry/delete mutations; `not_before` is decoded from the
/// key's time prefix so the UI can label backed-off retries.
#[derive(Serialize, Debug)]
struct JobEntry {
    storage_key: String,
    id: String,
    kind: String,
    lock_key: String,
    attempts: u32,
    max_attempts: u32,
    created_at: DateTime<Utc>,
    not_before: DateTime<Utc>,
}

#[derive(Serialize, Debug)]
struct JobsBody {
    jobs: Vec<JobEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next: Option<String>,
}

/// A dead-letter (exhausted-retry) job, carrying the failure reason and instant.
#[derive(Serialize, Debug)]
struct FailedJobEntry {
    storage_key: String,
    id: String,
    kind: String,
    lock_key: String,
    attempts: u32,
    max_attempts: u32,
    created_at: DateTime<Utc>,
    failed_at: DateTime<Utc>,
    last_error: String,
}

#[derive(Serialize, Debug)]
struct FailedJobsBody {
    failed: Vec<FailedJobEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next: Option<String>,
}

struct RepositoryConfig {
    pull_through_cache: bool,
    upstream_urls: Vec<String>,
    immutable_tags: bool,
    immutable_tags_exclusions: Vec<RegexPattern>,
}

/// Detected Docker-style referrer: a child descriptor that points back at a
/// `subject` via the Docker reference digest annotation.
struct DockerReferrerCandidate {
    /// The subject manifest this referrer points to.
    subject: Digest,
    /// The referrer manifest's own digest (used to fetch its body for in-toto
    /// predicate enrichment).
    child_digest: Digest,
    /// Pre-built referrer record. Annotations are NOT yet enriched with the
    /// in-toto predicate type; the caller does that after reading the child
    /// manifest body.
    info: ReferrerInfo,
}

/// Returns the Docker-style referrer candidate carried by `descriptor`, if any.
/// Pure, no I/O. Returns `None` when the descriptor has no
/// Docker reference digest annotation or when the annotation value
/// cannot be parsed as a `Digest`.
fn extract_docker_referrer(descriptor: &Descriptor) -> Option<DockerReferrerCandidate> {
    let subject_str = descriptor.annotations.get(DOCKER_REFERENCE_DIGEST)?;
    let subject = subject_str.parse::<Digest>().ok()?;
    Some(DockerReferrerCandidate {
        subject,
        child_digest: descriptor.digest.clone(),
        info: ReferrerInfo {
            digest: descriptor.digest.to_string(),
            artifact_type: descriptor.artifact_type.clone(),
            annotations: descriptor.annotations.clone(),
        },
    })
}

/// Returns the in-toto predicate type annotation value from the first
/// layer that carries it, if any. Pure, no I/O.
fn extract_in_toto_predicate(child_manifest: &Manifest) -> Option<String> {
    child_manifest
        .layers
        .iter()
        .find_map(|layer| layer.annotations.get(IN_TOTO_PREDICATE_TYPE).cloned())
}

/// Pure analysis of a parent manifest's `manifests` array, partitioning each
/// child descriptor into either a parent-link (regular index child) or a
/// Docker-style referrer candidate.
struct ManifestAnalysis {
    /// Index children that are NOT referrers: `(child_digest, platform)` pairs
    /// where the analyzed manifest is the parent.
    parent_links: Vec<(Digest, Option<ExtPlatform>)>,
    /// Docker-style referrer candidates carried by this manifest.
    referrer_candidates: Vec<DockerReferrerCandidate>,
}

/// Partitions all child descriptors in `manifest.manifests` into parent-links
/// and Docker-style referrer candidates. Pure, no I/O.
fn analyze_manifest(manifest: &Manifest) -> ManifestAnalysis {
    let mut parent_links = Vec::new();
    let mut referrer_candidates = Vec::new();
    for child in &manifest.manifests {
        if let Some(referrer) = extract_docker_referrer(child) {
            referrer_candidates.push(referrer);
        } else {
            parent_links.push((
                child.digest.clone(),
                child.platform.clone().map(ExtPlatform::from),
            ));
        }
    }
    ManifestAnalysis {
        parent_links,
        referrer_candidates,
    }
}

/// Groups `(tag, digest)` pairs by digest, collecting tags in encounter order.
///
/// This is the pure aggregation step that follows the async I/O that resolves
/// each tag to its target digest.
fn build_digest_to_tags_map_from_pairs(tag_links: Vec<(Tag, Digest)>) -> HashMap<Digest, Vec<Tag>> {
    let mut map: HashMap<Digest, Vec<Tag>> = HashMap::new();
    for (tag, digest) in tag_links {
        map.entry(digest).or_default().push(tag);
    }
    map
}

/// Returns the `ParentRef` list for `digest` using the pre-built parent and
/// tag maps. Produces an empty `Vec` when `digest` has no recorded parents.
fn parent_refs_for(
    digest: &Digest,
    child_to_parents: &HashMap<Digest, Vec<(Digest, Option<ExtPlatform>)>>,
    digest_to_tags: &HashMap<Digest, Vec<Tag>>,
) -> Vec<ParentRef> {
    child_to_parents
        .get(digest)
        .map(|parents| {
            parents
                .iter()
                .map(|(parent_digest, platform)| ParentRef {
                    digest: parent_digest.to_string(),
                    tags: digest_to_tags
                        .get(parent_digest)
                        .cloned()
                        .unwrap_or_default(),
                    platform: platform.clone(),
                })
                .collect()
        })
        .unwrap_or_default()
}

impl Registry {
    #[instrument(skip(self))]
    pub async fn get_repositories_info(&self) -> Result<JsonResponse, Error> {
        let mut repositories = Vec::with_capacity(self.resolver.len());

        for name in self.resolver.keys() {
            let namespaces = self.list_repository_namespaces(name).await?;
            let config = self.get_repository_config(name);
            repositories.push(RepositoryInfo {
                name: name.to_string(),
                namespace_count: namespaces.len(),
                pull_through_cache: config.pull_through_cache,
                immutable_tags: config.immutable_tags,
            });
        }

        repositories.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(JsonResponse {
            headers: json_headers(),
            body: serde_json::to_vec(&RepositoriesBody { repositories })?,
        })
    }

    #[instrument(skip(self))]
    pub async fn get_namespaces_info(&self, repository: &str) -> Result<JsonResponse, Error> {
        let namespace_names = self.list_repository_namespaces(repository).await?;
        let mut namespaces = Vec::with_capacity(namespace_names.len());

        for name_str in namespace_names {
            let name = Namespace::new(&name_str).map_err(|_| Error::NameInvalid)?;
            let manifest_count = self.metadata_store.count_manifests(&name).await?;
            let upload_count = self.count_uploads(&name).await?;
            namespaces.push(NamespaceInfo {
                name: name_str,
                manifest_count,
                upload_count,
            });
        }

        let config = self.get_repository_config(repository);

        Ok(JsonResponse {
            headers: json_headers(),
            body: serde_json::to_vec(&NamespacesBody {
                repository: repository.to_string(),
                namespaces,
                pull_through_cache: config.pull_through_cache,
                upstream_urls: config.upstream_urls,
                immutable_tags: config.immutable_tags,
                immutable_tags_exclusions: config.immutable_tags_exclusions,
            })?,
        })
    }

    #[instrument(skip(self))]
    pub async fn get_revisions_info(&self, namespace: &Namespace) -> Result<JsonResponse, Error> {
        let all_revisions = collect_all_pages(|token| async move {
            self.metadata_store
                .list_revisions(namespace, 1000, token)
                .await
                .map_err(Error::from)
        })
        .await?;
        let digest_to_tags = self.build_digest_to_tags_map(namespace).await?;
        let (child_to_parents, docker_referrers) =
            self.build_parent_and_referrer_maps(&all_revisions).await;
        let manifests = self
            .build_manifest_entries(
                namespace,
                all_revisions,
                &digest_to_tags,
                child_to_parents,
                docker_referrers,
            )
            .await;

        Ok(JsonResponse {
            headers: json_headers(),
            body: serde_json::to_vec(&RevisionsBody {
                name: namespace.to_string(),
                manifests,
            })?,
        })
    }

    #[instrument(skip(self))]
    pub async fn get_uploads_info(&self, namespace: &Namespace) -> Result<JsonResponse, Error> {
        let uuids = collect_all_pages(|token| async move {
            self.blob_store
                .list_uploads(namespace, 1000, token)
                .await
                .map_err(Error::from)
        })
        .await?;

        let mut all_uploads = Vec::new();
        for uuid in uuids {
            if let Ok(summary) = self.blob_store.upload_summary(namespace, &uuid).await {
                all_uploads.push(UploadEntry {
                    uuid,
                    size: summary.size,
                    started_at: summary.started_at,
                });
            }
        }

        Ok(JsonResponse {
            headers: json_headers(),
            body: serde_json::to_vec(&UploadsBody {
                name: namespace.to_string(),
                uploads: all_uploads,
            })?,
        })
    }

    /// One keyset page of pending/in-flight durable jobs on `queue`. `after` is
    /// the plain storage key from a previous page's `next` (non-opaque). Each
    /// row reads the envelope body; a row deleted mid-scan is silently skipped.
    #[instrument(skip(self))]
    pub async fn get_jobs_info(
        &self,
        queue: Queue,
        n: Option<u16>,
        after: Option<String>,
    ) -> Result<JsonResponse, Error> {
        let n = n.unwrap_or(DEFAULT_JOBS_PAGE);
        let (keys, next) = self
            .job_queue
            .list_pending_page(queue, n, after.as_deref())
            .await?;

        let mut jobs = Vec::with_capacity(keys.len());
        for storage_key in keys {
            match self.job_queue.read_pending(queue, &storage_key).await {
                Ok(envelope) => {
                    let not_before =
                        job_store::parse_not_before(&storage_key).unwrap_or(envelope.created_at);
                    jobs.push(JobEntry {
                        storage_key,
                        id: envelope.id,
                        kind: envelope.kind,
                        lock_key: envelope.lock_key,
                        attempts: envelope.attempts,
                        max_attempts: envelope.max_attempts,
                        created_at: envelope.created_at,
                        not_before,
                    });
                }
                Err(job_store::Error::NotFound) => {}
                Err(e) => return Err(e.into()),
            }
        }

        Ok(JsonResponse {
            headers: json_headers(),
            body: serde_json::to_vec(&JobsBody { jobs, next })?,
        })
    }

    /// One keyset page of dead-letter (exhausted-retry) jobs on `queue`. See
    /// [`Self::get_jobs_info`] for the cursor and skip semantics.
    #[instrument(skip(self))]
    pub async fn get_failed_jobs_info(
        &self,
        queue: Queue,
        n: Option<u16>,
        after: Option<String>,
    ) -> Result<JsonResponse, Error> {
        let n = n.unwrap_or(DEFAULT_JOBS_PAGE);
        let (keys, next) = self
            .job_queue
            .list_failed_page(queue, n, after.as_deref())
            .await?;

        let mut failed = Vec::with_capacity(keys.len());
        for storage_key in keys {
            match self.job_queue.read_failed(queue, &storage_key).await {
                Ok(record) => failed.push(FailedJobEntry {
                    storage_key,
                    id: record.envelope.id,
                    kind: record.envelope.kind,
                    lock_key: record.envelope.lock_key,
                    attempts: record.envelope.attempts,
                    max_attempts: record.envelope.max_attempts,
                    created_at: record.envelope.created_at,
                    failed_at: record.failed_at,
                    last_error: record.last_error,
                }),
                Err(job_store::Error::NotFound) => {}
                Err(e) => return Err(e.into()),
            }
        }

        Ok(JsonResponse {
            headers: json_headers(),
            body: serde_json::to_vec(&FailedJobsBody { failed, next })?,
        })
    }

    /// Requeue a dead-letter job (attempts reset to zero) on `queue`. Delegates
    /// to the durable queue; a stale key surfaces as [`Error::NotFound`] (404).
    #[instrument(skip(self))]
    pub async fn retry_failed_job(&self, queue: Queue, storage_key: &str) -> Result<(), Error> {
        self.job_queue
            .retry_failed(queue, storage_key)
            .await
            .map_err(Error::from)
    }

    /// Delete a job on `queue` in the given partition. A stale key surfaces as
    /// [`Error::NotFound`] (404).
    #[instrument(skip(self))]
    pub async fn delete_job(
        &self,
        queue: Queue,
        state: JobState,
        storage_key: &str,
    ) -> Result<(), Error> {
        self.job_queue
            .delete_job(queue, state, storage_key)
            .await
            .map_err(Error::from)
    }

    fn get_repository_config(&self, name: &str) -> RepositoryConfig {
        let global_exclusions = || self.global_immutable_tags_exclusions.clone();

        let Some(repo) = self.resolver.get(name) else {
            return RepositoryConfig {
                pull_through_cache: false,
                upstream_urls: Vec::new(),
                immutable_tags: self.global_immutable_tags,
                immutable_tags_exclusions: global_exclusions(),
            };
        };

        let upstream_urls: Vec<String> = repo
            .upstreams
            .iter()
            .map(|u| u.client.url.clone())
            .collect();
        let immutable_tags_exclusions = if repo.immutable_tags_exclusions.is_empty() {
            global_exclusions()
        } else {
            repo.immutable_tags_exclusions.clone()
        };
        RepositoryConfig {
            pull_through_cache: !upstream_urls.is_empty(),
            upstream_urls,
            immutable_tags: repo.immutable_tags || self.global_immutable_tags,
            immutable_tags_exclusions,
        }
    }

    async fn build_parent_and_referrer_maps(
        &self,
        all_revisions: &[Digest],
    ) -> (
        HashMap<Digest, Vec<(Digest, Option<ExtPlatform>)>>,
        HashMap<Digest, Vec<ReferrerInfo>>,
    ) {
        let mut child_to_parents: HashMap<Digest, Vec<(Digest, Option<ExtPlatform>)>> =
            HashMap::new();
        let mut docker_referrers: HashMap<Digest, Vec<ReferrerInfo>> = HashMap::new();

        for digest in all_revisions {
            let Some(manifest) = self.read_manifest(digest).await else {
                continue;
            };
            let analysis = analyze_manifest(&manifest);

            for (child_digest, platform) in analysis.parent_links {
                child_to_parents
                    .entry(child_digest)
                    .or_default()
                    .push((digest.clone(), platform));
            }

            for referrer in analysis.referrer_candidates {
                let info = self
                    .enrich_referrer_with_predicate(referrer.info, &referrer.child_digest)
                    .await;
                docker_referrers
                    .entry(referrer.subject)
                    .or_default()
                    .push(info);
            }
        }

        (child_to_parents, docker_referrers)
    }

    /// Reads the child manifest and enriches `info` with the in-toto predicate
    /// type annotation when the manifest carries one.
    async fn enrich_referrer_with_predicate(
        &self,
        mut info: ReferrerInfo,
        child_digest: &Digest,
    ) -> ReferrerInfo {
        if let Some(child_manifest) = self.read_manifest(child_digest).await
            && let Some(predicate) = extract_in_toto_predicate(&child_manifest)
        {
            info.annotations
                .insert(IN_TOTO_PREDICATE_TYPE.to_string(), predicate);
        }
        info
    }

    async fn build_manifest_entries(
        &self,
        namespace: &Namespace,
        all_revisions: Vec<Digest>,
        digest_to_tags: &HashMap<Digest, Vec<Tag>>,
        child_to_parents: HashMap<Digest, Vec<(Digest, Option<ExtPlatform>)>>,
        mut docker_referrers: HashMap<Digest, Vec<ReferrerInfo>>,
    ) -> Vec<ManifestEntry> {
        let mut manifests: Vec<ManifestEntry> = Vec::with_capacity(all_revisions.len());

        for digest in all_revisions {
            let tags = digest_to_tags.get(&digest).cloned().unwrap_or_default();
            let parents = parent_refs_for(&digest, &child_to_parents, digest_to_tags);

            let mut referrers: Vec<ReferrerInfo> =
                docker_referrers.remove(&digest).unwrap_or_default();

            if let Ok(oci_referrers) = self
                .metadata_store
                .list_referrers(namespace, &digest, None)
                .await
            {
                referrers.extend(oci_referrers.into_iter().map(ReferrerInfo::from));
            }

            let (pushed_at, last_pulled_at) = self
                .metadata_store
                .read_link(namespace, &LinkKind::Digest(digest.clone()))
                .await
                .map_or((None, None), |m| (m.created_at, m.accessed_at));

            manifests.push(ManifestEntry {
                digest: digest.to_string(),
                tags,
                parents,
                referrers,
                pushed_at,
                last_pulled_at,
            });
        }

        manifests
    }

    async fn count_uploads(&self, namespace: &Namespace) -> Result<usize, Error> {
        let uploads = collect_all_pages(|token| async move {
            self.blob_store
                .list_uploads(namespace, 1000, token)
                .await
                .map_err(Error::from)
        })
        .await?;
        Ok(uploads.len())
    }

    async fn build_digest_to_tags_map(
        &self,
        namespace: &Namespace,
    ) -> Result<HashMap<Digest, Vec<Tag>>, Error> {
        let all_tags = collect_all_pages(|last| async move {
            self.metadata_store
                .list_tags(namespace, 1000, last)
                .await
                .map_err(Error::from)
        })
        .await?;

        let mut tag_links: Vec<(Tag, Digest)> = Vec::with_capacity(all_tags.len());
        for tag in all_tags {
            let link = LinkKind::Tag(tag.clone());
            if let Ok(link_metadata) = self.metadata_store.read_link(namespace, &link).await {
                tag_links.push((tag, link_metadata.target));
            }
        }

        Ok(build_digest_to_tags_map_from_pairs(tag_links))
    }

    async fn list_repository_namespaces(&self, repository: &str) -> Result<Vec<String>, Error> {
        if !self.resolver.contains_key(repository) {
            return Err(Error::NameUnknown);
        }

        let manifest_namespaces = collect_all_pages(|token| async move {
            self.metadata_store
                .list_namespaces(1000, token)
                .await
                .map_err(Error::from)
        })
        .await?;

        // The catalog keys namespaces off `_manifests`, so a namespace holding
        // only in-progress uploads is absent from it; merge the blob store's
        // `_uploads`-keyed listing so its pending uploads still surface here.
        let upload_namespaces = collect_all_pages(|token| async move {
            self.blob_store
                .list_upload_namespaces(1000, token)
                .await
                .map_err(Error::from)
        })
        .await?;

        let mut matching_namespaces: Vec<String> = manifest_namespaces
            .into_iter()
            .chain(upload_namespaces)
            .filter(|ns| namespace_belongs_to(ns, repository))
            .collect();

        matching_namespaces.sort_unstable();
        matching_namespaces.dedup();
        Ok(matching_namespaces)
    }

    async fn read_manifest(&self, digest: &Digest) -> Option<Manifest> {
        let blob = self.blob_store.read(digest).await.ok()?;
        Manifest::from_slice(&blob).ok()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        ExtPlatform, analyze_manifest, build_digest_to_tags_map_from_pairs,
        extract_docker_referrer, extract_in_toto_predicate, parent_refs_for,
    };
    use crate::{
        oci::{
            DOCKER_REFERENCE_DIGEST, Descriptor, Digest, IN_TOTO_PREDICATE_TYPE, Manifest,
            Namespace, Platform as OciPlatform, Tag, UploadSessionId,
        },
        registry::test_utils::{
            FSRegistryTestCase, create_test_blob, for_each_backend, media_type,
        },
    };

    fn digest(hex_suffix: &str) -> Digest {
        // Pad to 64 hex chars with the suffix at the end.
        let padded = format!("{hex_suffix:0>64}");
        format!("sha256:{padded}").parse().unwrap()
    }

    fn test_digest() -> Digest {
        digest("abc1")
    }

    fn descriptor_with_annotations(annotations: HashMap<String, String>) -> Descriptor {
        Descriptor {
            media_type: media_type("application/vnd.oci.image.manifest.v1+json"),
            digest: test_digest(),
            size: 0,
            annotations,
            artifact_type: None,
            platform: None,
        }
    }

    fn manifest_with_layers(layer_annotations: Vec<HashMap<String, String>>) -> Manifest {
        let layers: Vec<Descriptor> = layer_annotations
            .into_iter()
            .map(|ann| Descriptor {
                media_type: media_type("application/vnd.oci.image.layer.v1.tar+gzip"),
                digest: test_digest(),
                size: 0,
                annotations: ann,
                artifact_type: None,
                platform: None,
            })
            .collect();
        Manifest {
            layers,
            ..Manifest::default()
        }
    }

    // --- extract_in_toto_predicate ---

    #[test]
    fn extract_in_toto_predicate_returns_none_for_no_layers() {
        let manifest = manifest_with_layers(vec![]);
        assert_eq!(extract_in_toto_predicate(&manifest), None);
    }

    #[test]
    fn extract_in_toto_predicate_returns_none_when_annotation_absent() {
        let manifest = manifest_with_layers(vec![HashMap::from([(
            "some.other.key".to_string(),
            "value".to_string(),
        )])]);
        assert_eq!(extract_in_toto_predicate(&manifest), None);
    }

    #[test]
    fn extract_in_toto_predicate_returns_first_match_across_layers() {
        let manifest = manifest_with_layers(vec![
            HashMap::from([(
                IN_TOTO_PREDICATE_TYPE.to_string(),
                "https://slsa.dev/provenance/v0.2".to_string(),
            )]),
            HashMap::from([(
                IN_TOTO_PREDICATE_TYPE.to_string(),
                "https://slsa.dev/provenance/v1".to_string(),
            )]),
        ]);
        assert_eq!(
            extract_in_toto_predicate(&manifest),
            Some("https://slsa.dev/provenance/v0.2".to_string()),
        );
    }

    #[test]
    fn extract_in_toto_predicate_returns_value_when_single_layer_has_it() {
        let manifest = manifest_with_layers(vec![HashMap::from([(
            IN_TOTO_PREDICATE_TYPE.to_string(),
            "https://slsa.dev/provenance/v0.2".to_string(),
        )])]);
        assert_eq!(
            extract_in_toto_predicate(&manifest),
            Some("https://slsa.dev/provenance/v0.2".to_string()),
        );
    }

    // --- extract_docker_referrer ---

    #[test]
    fn extract_docker_referrer_returns_none_when_annotation_absent() {
        let descriptor = descriptor_with_annotations(HashMap::new());
        assert!(extract_docker_referrer(&descriptor).is_none());
    }

    #[test]
    fn extract_docker_referrer_returns_none_when_annotation_not_a_valid_digest() {
        let descriptor = descriptor_with_annotations(HashMap::from([(
            DOCKER_REFERENCE_DIGEST.to_string(),
            "not-a-valid-digest".to_string(),
        )]));
        assert!(extract_docker_referrer(&descriptor).is_none());
    }

    #[test]
    fn extract_docker_referrer_returns_candidate_with_parsed_subject() {
        let subject = digest("beef");
        let child = digest("cafe");
        let mut descriptor = descriptor_with_annotations(HashMap::from([(
            DOCKER_REFERENCE_DIGEST.to_string(),
            subject.to_string(),
        )]));
        descriptor.digest = child.clone();
        descriptor.artifact_type = Some(media_type(
            "application/vnd.dev.cosign.artifact.sig.v1+json",
        ));

        let candidate = extract_docker_referrer(&descriptor).expect("should return Some");
        assert_eq!(candidate.subject, subject);
        assert_eq!(candidate.child_digest, child);
        assert_eq!(candidate.info.digest, child.to_string());
        assert_eq!(
            candidate.info.artifact_type.as_deref(),
            Some("application/vnd.dev.cosign.artifact.sig.v1+json")
        );
        assert_eq!(
            candidate.info.annotations.get(DOCKER_REFERENCE_DIGEST),
            Some(&subject.to_string())
        );
    }

    // --- analyze_manifest ---

    #[test]
    fn analyze_manifest_returns_empty_for_manifest_with_no_children() {
        let manifest = Manifest::default();
        let analysis = analyze_manifest(&manifest);
        assert!(analysis.parent_links.is_empty());
        assert!(analysis.referrer_candidates.is_empty());
    }

    #[test]
    fn analyze_manifest_returns_parent_links_for_non_referrer_children() {
        let child_digest = digest("1111");
        let platform = OciPlatform {
            architecture: "amd64".to_string(),
            os: "linux".to_string(),
            variant: None,
            os_version: None,
            os_features: None,
            features: None,
        };
        let child = Descriptor {
            media_type: media_type("application/vnd.oci.image.manifest.v1+json"),
            digest: child_digest.clone(),
            size: 0,
            annotations: HashMap::new(),
            artifact_type: None,
            platform: Some(platform),
        };
        let manifest = Manifest {
            manifests: vec![child],
            ..Manifest::default()
        };

        let analysis = analyze_manifest(&manifest);
        assert_eq!(analysis.parent_links.len(), 1);
        assert!(analysis.referrer_candidates.is_empty());
        let (d, plat) = &analysis.parent_links[0];
        assert_eq!(d, &child_digest);
        let p = plat.as_ref().expect("platform should be present");
        assert_eq!(p.os, "linux");
        assert_eq!(p.architecture, "amd64");
    }

    #[test]
    fn analyze_manifest_returns_referrer_candidates_for_docker_referrer_children() {
        let subject = digest("beef");
        let child_digest = digest("cafe");
        let child = Descriptor {
            media_type: media_type("application/vnd.oci.image.manifest.v1+json"),
            digest: child_digest.clone(),
            size: 0,
            annotations: HashMap::from([(
                DOCKER_REFERENCE_DIGEST.to_string(),
                subject.to_string(),
            )]),
            artifact_type: None,
            platform: None,
        };
        let manifest = Manifest {
            manifests: vec![child],
            ..Manifest::default()
        };

        let analysis = analyze_manifest(&manifest);
        assert!(analysis.parent_links.is_empty());
        assert_eq!(analysis.referrer_candidates.len(), 1);
        assert_eq!(analysis.referrer_candidates[0].subject, subject);
        assert_eq!(analysis.referrer_candidates[0].child_digest, child_digest);
    }

    #[test]
    fn analyze_manifest_partitions_mixed_children_correctly() {
        let subject = digest("beef");
        let referrer_digest = digest("cafe");
        let index_child_digest = digest("1234");

        let referrer_child = Descriptor {
            media_type: media_type("application/vnd.oci.image.manifest.v1+json"),
            digest: referrer_digest.clone(),
            size: 0,
            annotations: HashMap::from([(
                DOCKER_REFERENCE_DIGEST.to_string(),
                subject.to_string(),
            )]),
            artifact_type: None,
            platform: None,
        };
        let index_child = Descriptor {
            media_type: media_type("application/vnd.oci.image.manifest.v1+json"),
            digest: index_child_digest.clone(),
            size: 0,
            annotations: HashMap::new(),
            artifact_type: None,
            platform: None,
        };
        let manifest = Manifest {
            manifests: vec![referrer_child, index_child],
            ..Manifest::default()
        };

        let analysis = analyze_manifest(&manifest);
        assert_eq!(analysis.parent_links.len(), 1);
        assert_eq!(analysis.referrer_candidates.len(), 1);
        assert_eq!(analysis.parent_links[0].0, index_child_digest);
        assert_eq!(analysis.referrer_candidates[0].subject, subject);
        assert_eq!(
            analysis.referrer_candidates[0].child_digest,
            referrer_digest
        );
    }

    // --- build_digest_to_tags_map_from_pairs ---

    #[test]
    fn build_digest_to_tags_map_empty_input_produces_empty_map() {
        let result = build_digest_to_tags_map_from_pairs(vec![]);
        assert!(result.is_empty());
    }

    #[test]
    fn build_digest_to_tags_map_single_tag_maps_to_its_digest() {
        let d = digest("1111");
        let result =
            build_digest_to_tags_map_from_pairs(vec![(Tag::new("latest").unwrap(), d.clone())]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[&d], vec![Tag::new("latest").unwrap()]);
    }

    #[test]
    fn build_digest_to_tags_map_multiple_tags_for_same_digest_are_grouped() {
        let d = digest("2222");
        let pairs = vec![
            (Tag::new("v1.0").unwrap(), d.clone()),
            (Tag::new("latest").unwrap(), d.clone()),
        ];
        let result = build_digest_to_tags_map_from_pairs(pairs);
        assert_eq!(result.len(), 1);
        let mut tags = result[&d].clone();
        tags.sort_unstable();
        assert_eq!(
            tags,
            vec![Tag::new("latest").unwrap(), Tag::new("v1.0").unwrap()]
        );
    }

    #[test]
    fn build_digest_to_tags_map_tags_for_different_digests_are_separate() {
        let d1 = digest("aaaa");
        let d2 = digest("bbbb");
        let pairs = vec![
            (Tag::new("alpha").unwrap(), d1.clone()),
            (Tag::new("beta").unwrap(), d2.clone()),
        ];
        let result = build_digest_to_tags_map_from_pairs(pairs);
        assert_eq!(result.len(), 2);
        assert_eq!(result[&d1], vec![Tag::new("alpha").unwrap()]);
        assert_eq!(result[&d2], vec![Tag::new("beta").unwrap()]);
    }

    // --- parent_refs_for ---

    #[test]
    fn parent_refs_for_returns_empty_when_digest_not_in_parent_map() {
        let child_to_parents: HashMap<Digest, Vec<(Digest, Option<ExtPlatform>)>> = HashMap::new();
        let digest_to_tags: HashMap<Digest, Vec<Tag>> = HashMap::new();
        let result = parent_refs_for(&digest("cccc"), &child_to_parents, &digest_to_tags);
        assert!(result.is_empty());
    }

    #[test]
    fn parent_refs_for_single_parent_no_tags_no_platform() {
        let child = digest("cccc");
        let parent = digest("dddd");
        let child_to_parents = HashMap::from([(child.clone(), vec![(parent.clone(), None)])]);
        let digest_to_tags: HashMap<Digest, Vec<Tag>> = HashMap::new();

        let result = parent_refs_for(&child, &child_to_parents, &digest_to_tags);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].digest, parent.to_string());
        assert!(result[0].tags.is_empty());
        assert!(result[0].platform.is_none());
    }

    #[test]
    fn parent_refs_for_single_parent_with_tags() {
        let child = digest("eeee");
        let parent = digest("ffff");
        let child_to_parents = HashMap::from([(child.clone(), vec![(parent.clone(), None)])]);
        let digest_to_tags = HashMap::from([(parent.clone(), vec![Tag::new("v2").unwrap()])]);

        let result = parent_refs_for(&child, &child_to_parents, &digest_to_tags);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].tags, vec![Tag::new("v2").unwrap()]);
    }

    #[test]
    fn parent_refs_for_multiple_parents_emitted_in_order() {
        let child = digest("1234");
        let parent_a = digest("aaaa");
        let parent_b = digest("bbbb");
        let platform = ExtPlatform {
            os: "linux".to_string(),
            architecture: "arm64".to_string(),
            variant: Some("v8".to_string()),
        };
        let child_to_parents = HashMap::from([(
            child.clone(),
            vec![
                (parent_a.clone(), None),
                (parent_b.clone(), Some(platform.clone())),
            ],
        )]);
        let digest_to_tags: HashMap<Digest, Vec<Tag>> = HashMap::new();

        let result = parent_refs_for(&child, &child_to_parents, &digest_to_tags);
        assert_eq!(result.len(), 2);

        let ref_a = result
            .iter()
            .find(|r| r.digest == parent_a.to_string())
            .unwrap();
        assert!(ref_a.platform.is_none());

        let ref_b = result
            .iter()
            .find(|r| r.digest == parent_b.to_string())
            .unwrap();
        let p = ref_b.platform.as_ref().unwrap();
        assert_eq!(p.os, "linux");
        assert_eq!(p.architecture, "arm64");
        assert_eq!(p.variant.as_deref(), Some("v8"));
    }

    /// A namespace holding only an in-progress upload has no `_manifests`
    /// child, yet the ext listings must surface it (with its upload count) so
    /// the UI can show and cancel its pending uploads. A namespace with both
    /// manifests and uploads must appear exactly once.
    #[tokio::test]
    async fn namespaces_info_includes_upload_only_namespace() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();

            let upload_only = Namespace::new("test-repo/upload-only").unwrap();
            registry
                .blob_store
                .create_upload(&upload_only, UploadSessionId::generate().as_ref())
                .await
                .unwrap();

            let mixed = Namespace::new("test-repo/mixed").unwrap();
            create_test_blob(registry, &mixed, b"mixed content").await;
            registry
                .blob_store
                .create_upload(&mixed, UploadSessionId::generate().as_ref())
                .await
                .unwrap();

            let response = registry.get_namespaces_info("test-repo").await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
            let namespaces = body["namespaces"].as_array().unwrap();

            let entries: Vec<(&str, u64, u64)> = namespaces
                .iter()
                .map(|ns| {
                    (
                        ns["name"].as_str().unwrap(),
                        ns["manifest_count"].as_u64().unwrap(),
                        ns["upload_count"].as_u64().unwrap(),
                    )
                })
                .collect();
            assert!(
                entries.contains(&("test-repo/upload-only", 0, 1)),
                "an upload-only namespace must be listed with its upload count; got: {entries:?}"
            );
            assert_eq!(
                entries
                    .iter()
                    .filter(|(name, _, _)| *name == "test-repo/mixed")
                    .count(),
                1,
                "a namespace with manifests and uploads must be listed once; got: {entries:?}"
            );

            let response = registry.get_repositories_info().await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
            let count = body["repositories"][0]["namespace_count"].as_u64().unwrap();
            assert_eq!(
                count, 2,
                "the repository namespace count must include the upload-only namespace"
            );
        })
        .await;
    }

    /// With the blob and metadata stores on separate backends, upload sessions
    /// exist only on the blob store; the ext listing must discover an
    /// upload-only namespace there, not on the metadata store's tree.
    #[tokio::test]
    async fn namespaces_info_finds_upload_only_namespace_across_split_backends() {
        let test_case = FSRegistryTestCase::with_split_backends();
        let registry = test_case.registry();

        let namespace = Namespace::new("test-repo/upload-only").unwrap();
        registry
            .blob_store
            .create_upload(&namespace, UploadSessionId::generate().as_ref())
            .await
            .unwrap();

        let response = registry.get_namespaces_info("test-repo").await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        let namespaces = body["namespaces"].as_array().unwrap();

        assert_eq!(
            namespaces.len(),
            1,
            "the upload-only namespace must be discovered on the blob store; got: {namespaces:?}"
        );
        assert_eq!(namespaces[0]["name"], "test-repo/upload-only");
        assert_eq!(namespaces[0]["manifest_count"], 0);
        assert_eq!(namespaces[0]["upload_count"], 1);
    }
}
