//! The `/v2/_angos` admin surface: repository and namespace info for the web UI,
//! plus the durable job list/retry/delete endpoints.

use std::{collections::HashMap, future::Future};

use chrono::{DateTime, Utc};
use futures_util::stream::{self, StreamExt, TryStreamExt};
use http::{HeaderMap, Response, StatusCode};
use serde::Serialize;
use tokio::try_join;
use tracing::{instrument, warn};

use angos_oci::request::GetReferrersRequest;
use angos_oci::{
    Content, DOCKER_REFERENCE_DIGEST, Descriptor, Digest, IN_TOTO_PREDICATE_TYPE, Manifest,
    MediaType, Namespace, Platform, Reference, Tag, UploadSessionId, namespace_belongs_to,
};

use crate::{
    configuration::RegexPattern,
    http_response::{ResponseBody, build_response, json_response},
    jobs::store as job_store,
    jobs::{JobState, Queue},
    registry::{
        Error, Registry,
        keys::NamespaceKeys,
        manifest::read_manifest,
        metadata_store::{AccessEntry, LinkKind},
    },
};

#[derive(Debug)]
pub struct ListPullsRequest {
    pub namespace: Namespace,
    pub reference: Reference,
}

#[derive(Debug)]
pub struct ListJobsRequest {
    pub queue: Queue,
    pub n: Option<u16>,
    pub after: Option<String>,
}

#[derive(Debug)]
pub struct RetryJobRequest {
    pub queue: Queue,
    pub storage_key: String,
}

#[derive(Debug)]
pub struct DeleteJobRequest {
    pub queue: Queue,
    pub state: JobState,
    pub storage_key: String,
}

/// Page size for the durable job-queue listings when the client sends no `?n=`.
const DEFAULT_JOBS_PAGE: u16 = 100;

/// Most pull-history entries one listing returns; the directory is unbounded,
/// so the newest page is all the UI gets.
const PULL_HISTORY_PAGE: u16 = 100;

/// Bounds the per-namespace stat fan-out so a repository with many namespaces
/// does not open one request per namespace at once.
const NAMESPACE_STAT_CONCURRENCY: usize = 32;

/// Fan-out for the per-item reads behind the info endpoints.
const ADMIN_READ_CONCURRENCY: usize = 16;

#[derive(Serialize, Debug)]
pub struct RepositoryInfo {
    name: String,
    namespace_count: usize,
    pull_through_cache: bool,
    upstream_urls: Vec<String>,
    immutable_tags: bool,
}

#[derive(Serialize, Debug)]
pub struct RepositoriesBody {
    repositories: Vec<RepositoryInfo>,
}

#[derive(Serialize, Debug)]
pub struct NamespaceInfo {
    name: String,
    tag_count: usize,
    manifest_count: usize,
    upload_count: usize,
}

#[derive(Serialize, Debug)]
pub struct NamespacesBody {
    repository: String,
    namespaces: Vec<NamespaceInfo>,
    pull_through_cache: bool,
    upstream_urls: Vec<String>,
    immutable_tags: bool,
    immutable_tags_exclusions: Vec<RegexPattern>,
}

#[derive(Serialize, Debug, Clone)]
pub struct ParentRef {
    digest: String,
    tags: Vec<Tag>,
    #[serde(skip_serializing_if = "Option::is_none")]
    platform: Option<Platform>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReferrerInfo {
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
pub struct ManifestEntry {
    digest: String,
    tags: Vec<Tag>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    parents: Vec<ParentRef>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    referrers: Vec<ReferrerInfo>,
    /// Where the OCI referrers listing continues, absent once exhausted; the UI
    /// feeds it back to `/v2/{namespace}/referrers/{digest}?last=`.
    #[serde(skip_serializing_if = "Option::is_none")]
    referrers_next: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pushed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_pulled_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Debug)]
pub struct RevisionsBody {
    name: String,
    manifests: Vec<ManifestEntry>,
}

#[derive(Serialize, Debug)]
pub struct UploadEntry {
    session_id: UploadSessionId,
    size: u64,
    started_at: DateTime<Utc>,
}

#[derive(Serialize, Debug)]
pub struct UploadsBody {
    name: String,
    uploads: Vec<UploadEntry>,
}

/// One target's recorded pulls, newest first. `window_secs` is the configured
/// retention, so the UI can state how far back the list can reach.
#[derive(Serialize, Debug)]
pub struct PullsBody {
    target: String,
    window_secs: u64,
    entries: Vec<AccessEntry>,
}

/// A pending or in-flight durable job. `not_before` is decoded from the
/// storage key's time prefix so the UI can label backed-off retries.
#[derive(Serialize, Debug)]
pub struct JobEntry {
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
pub struct JobsBody {
    jobs: Vec<JobEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next: Option<String>,
}

/// A dead-letter job, carrying the failure reason and instant.
#[derive(Serialize, Debug)]
pub struct FailedJobEntry {
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
pub struct FailedJobsBody {
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

/// A child descriptor that points back at a `subject` via the Docker reference
/// digest annotation.
struct DockerReferrerCandidate {
    subject: Digest,
    child_digest: Digest,
    /// Annotations are not yet enriched with the in-toto predicate type; the
    /// caller does that after reading the child manifest body.
    info: ReferrerInfo,
}

/// The Docker-style referrer `descriptor` carries, or `None` when it has no
/// reference digest annotation or the annotation does not parse as a digest.
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

/// The in-toto predicate type annotation from the first layer carrying one.
fn extract_in_toto_predicate(child_manifest: &Manifest) -> Option<String> {
    let Content::Image { layers, .. } = &child_manifest.content else {
        return None;
    };
    layers
        .iter()
        .find_map(|layer| layer.annotations.get(IN_TOTO_PREDICATE_TYPE).cloned())
}

struct ManifestAnalysis {
    /// Index children that are not referrers, each paired with its platform.
    parent_links: Vec<(Digest, Option<Platform>)>,
    referrer_candidates: Vec<DockerReferrerCandidate>,
}

/// Partitions an index's child descriptors into parent-links and Docker-style
/// referrer candidates.
fn analyze_manifest(manifest: &Manifest) -> ManifestAnalysis {
    let mut parent_links = Vec::new();
    let mut referrer_candidates = Vec::new();
    if let Content::Index { manifests } = &manifest.content {
        for child in manifests {
            if let Some(referrer) = extract_docker_referrer(child) {
                referrer_candidates.push(referrer);
            } else {
                parent_links.push((child.digest.clone(), child.platform.clone()));
            }
        }
    }
    ManifestAnalysis {
        parent_links,
        referrer_candidates,
    }
}

/// Reads one job record per storage key, keeping the keyset (time) order; a
/// record gone or unreadable mid-scan is skipped rather than failing the page.
async fn read_job_page<T, Fut>(
    storage_keys: Vec<String>,
    read: impl Fn(String) -> Fut,
) -> Result<Vec<T>, Error>
where
    Fut: Future<Output = Result<T, job_store::Error>>,
{
    let read = &read;
    stream::iter(storage_keys)
        .map(|storage_key| async move {
            match read(storage_key.clone()).await {
                Ok(entry) => Ok(Some(entry)),
                Err(job_store::Error::NotFound) => Ok(None),
                // Failing the page here would hide every other job on it.
                Err(job_store::Error::Corrupt(e)) => {
                    warn!("admin: skipping unreadable job record '{storage_key}': {e}");
                    Ok(None)
                }
                Err(e) => Err(Error::from(e)),
            }
        })
        .buffered(ADMIN_READ_CONCURRENCY)
        .try_filter_map(|entry| async move { Ok(entry) })
        .try_collect()
        .await
}

/// The `ParentRef` list for `digest`, empty when it has no recorded parents.
fn parent_refs_for(
    digest: &Digest,
    child_to_parents: &HashMap<Digest, Vec<(Digest, Option<Platform>)>>,
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
    pub async fn get_repositories_info(&self) -> Result<Response<ResponseBody>, Error> {
        // One walk bucketed in memory: listing per repository would re-scan the
        // whole store once per configured repository.
        let all_namespaces = self.collect_namespaces(None).await?;

        let mut repositories = Vec::with_capacity(self.resolver.len());
        for name in self.resolver.keys() {
            let namespace_count = all_namespaces
                .iter()
                .filter(|ns| namespace_belongs_to(ns, name))
                .count();
            let config = self.get_repository_config(name);
            repositories.push(RepositoryInfo {
                name: name.to_string(),
                namespace_count,
                pull_through_cache: config.pull_through_cache,
                upstream_urls: config.upstream_urls,
                immutable_tags: config.immutable_tags,
            });
        }

        repositories.sort_by(|a, b| a.name.cmp(&b.name));

        json_response(StatusCode::OK, &RepositoriesBody { repositories })
    }

    #[instrument(skip(self))]
    pub async fn get_namespaces_info(
        &self,
        repository: &Namespace,
    ) -> Result<Response<ResponseBody>, Error> {
        let repository = repository.as_ref();
        let namespace_names = self.list_repository_namespaces(repository).await?;

        // A directory whose name is not a valid namespace is a storage artifact
        // scrub removes; dropping it keeps one bad name from failing the listing.
        let mut namespaces: Vec<NamespaceInfo> = stream::iter(
            namespace_names
                .into_iter()
                .filter_map(|name| Namespace::new(&name).ok()),
        )
        .map(|name| async move {
            // The three counts read disjoint prefixes, so they go out together
            // rather than paying one round trip after another per namespace.
            let (tag_count, manifest_count, upload_count) = try_join!(
                self.metadata_store.count_tags(&name),
                self.metadata_store.count_manifests(&name),
                self.count_uploads(&name),
            )?;
            Ok::<_, Error>(NamespaceInfo {
                name: name.to_string(),
                tag_count,
                manifest_count,
                upload_count,
            })
        })
        .buffer_unordered(NAMESPACE_STAT_CONCURRENCY)
        .try_collect()
        .await?;

        namespaces.sort_by(|a, b| a.name.cmp(&b.name));

        let config = self.get_repository_config(repository);

        json_response(
            StatusCode::OK,
            &NamespacesBody {
                repository: repository.to_string(),
                namespaces,
                pull_through_cache: config.pull_through_cache,
                upstream_urls: config.upstream_urls,
                immutable_tags: config.immutable_tags,
                immutable_tags_exclusions: config.immutable_tags_exclusions,
            },
        )
    }

    #[instrument(skip(self))]
    pub async fn get_revisions_info(
        &self,
        namespace: &Namespace,
    ) -> Result<Response<ResponseBody>, Error> {
        // Materialized once: every step below needs the full revision set.
        let all_revisions: Vec<Digest> = self
            .metadata_store
            .stream_revisions(namespace)
            .try_collect()
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

        json_response(
            StatusCode::OK,
            &RevisionsBody {
                name: namespace.to_string(),
                manifests,
            },
        )
    }

    /// The newest recorded pulls of one tag or revision, newest first. The
    /// entry directory is append-only, so an entry deleted or corrupted
    /// mid-listing is skipped rather than failing the request.
    #[instrument(skip(self))]
    pub async fn get_pull_history(
        &self,
        request: ListPullsRequest,
    ) -> Result<Response<ResponseBody>, Error> {
        let ListPullsRequest {
            namespace,
            reference,
        } = request;
        let dir = match &reference {
            Reference::Tag(tag) => namespace.tag_atime_entry_dir(tag),
            Reference::Digest(digest) => namespace.revision_atime_entry_dir(digest),
        };
        let page = self
            .metadata_store
            .object_store()
            .list(&dir, PULL_HISTORY_PAGE, None)
            .await?;

        // `buffered` keeps the listing's newest-first order.
        let entries: Vec<AccessEntry> = stream::iter(page.items)
            .map(|name| {
                let key = format!("{dir}/{name}");
                async move {
                    let raw = self.metadata_store.object_store().get(&key).await.ok()?;
                    serde_json::from_slice::<AccessEntry>(&raw).ok()
                }
            })
            .buffered(ADMIN_READ_CONCURRENCY)
            .filter_map(|entry| async move { entry })
            .collect()
            .await;

        json_response(
            StatusCode::OK,
            &PullsBody {
                target: reference.to_string(),
                window_secs: self.metadata_store.atime_audit_window_secs(),
                entries,
            },
        )
    }

    #[instrument(skip(self))]
    pub async fn get_uploads_info(
        &self,
        namespace: &Namespace,
    ) -> Result<Response<ResponseBody>, Error> {
        let mut session_ids: Vec<UploadSessionId> = self
            .blob_store
            .stream_uploads(namespace)
            .try_collect()
            .await?;
        session_ids.sort();

        // `buffered` keeps the sorted order; an upload whose summary read fails
        // (reaped mid-listing) is skipped.
        let all_uploads: Vec<UploadEntry> = stream::iter(session_ids)
            .map(|session_id| async move {
                let summary = self
                    .blob_store
                    .upload_summary(namespace, &session_id)
                    .await
                    .ok()?;
                Some(UploadEntry {
                    session_id,
                    size: summary.size,
                    started_at: summary.started_at,
                })
            })
            .buffered(ADMIN_READ_CONCURRENCY)
            .filter_map(|entry| async move { entry })
            .collect()
            .await;

        json_response(
            StatusCode::OK,
            &UploadsBody {
                name: namespace.to_string(),
                uploads: all_uploads,
            },
        )
    }

    /// One keyset page of pending or in-flight durable jobs on `queue`, where
    /// `after` is the plain storage key from a previous page's `next`. A row
    /// deleted mid-scan is silently skipped.
    #[instrument(skip(self))]
    pub async fn get_jobs_info(
        &self,
        request: ListJobsRequest,
    ) -> Result<Response<ResponseBody>, Error> {
        let ListJobsRequest { queue, n, after } = request;
        let n = n.unwrap_or(DEFAULT_JOBS_PAGE);
        let page = self
            .job_queue
            .list_pending_page(queue, n, after.as_deref())
            .await?;

        let jobs = read_job_page(page.items, |storage_key| async move {
            let envelope = self.job_queue.read_pending(queue, &storage_key).await?;
            let not_before =
                job_store::parse_not_before(&storage_key).unwrap_or(envelope.created_at);
            Ok(JobEntry {
                storage_key,
                id: envelope.id,
                kind: envelope.kind,
                lock_key: envelope.lock_key.to_string(),
                attempts: envelope.attempts,
                max_attempts: envelope.max_attempts.unwrap_or_default(),
                created_at: envelope.created_at,
                not_before,
            })
        })
        .await?;

        json_response(
            StatusCode::OK,
            &JobsBody {
                jobs,
                next: page.next_token,
            },
        )
    }

    /// One keyset page of dead-letter jobs on `queue`; see
    /// [`Self::get_jobs_info`] for the cursor and skip semantics.
    #[instrument(skip(self))]
    pub async fn get_failed_jobs_info(
        &self,
        request: ListJobsRequest,
    ) -> Result<Response<ResponseBody>, Error> {
        let ListJobsRequest { queue, n, after } = request;
        let n = n.unwrap_or(DEFAULT_JOBS_PAGE);
        let page = self
            .job_queue
            .list_failed_page(queue, n, after.as_deref())
            .await?;

        let failed = read_job_page(page.items, |storage_key| async move {
            let record = self.job_queue.read_failed(queue, &storage_key).await?;
            Ok(FailedJobEntry {
                storage_key,
                id: record.envelope.id,
                kind: record.envelope.kind,
                lock_key: record.envelope.lock_key.to_string(),
                attempts: record.envelope.attempts,
                max_attempts: record.envelope.max_attempts.unwrap_or_default(),
                created_at: record.envelope.created_at,
                failed_at: record.failed_at,
                last_error: record.last_error,
            })
        })
        .await?;

        json_response(
            StatusCode::OK,
            &FailedJobsBody {
                failed,
                next: page.next_token,
            },
        )
    }

    /// Requeue a dead-letter job on `queue` with its attempts reset to zero; a
    /// stale key surfaces as [`Error::NotFound`].
    #[instrument(skip(self))]
    pub async fn retry_failed_job(
        &self,
        request: RetryJobRequest,
    ) -> Result<Response<ResponseBody>, Error> {
        self.job_queue
            .retry_failed(request.queue, &request.storage_key)
            .await?;

        Ok(build_response(
            StatusCode::NO_CONTENT,
            HeaderMap::new(),
            ResponseBody::empty(),
        )?)
    }

    /// Delete a job on `queue` in the given partition; a stale key surfaces as
    /// [`Error::NotFound`].
    #[instrument(skip(self))]
    pub async fn delete_job(
        &self,
        request: DeleteJobRequest,
    ) -> Result<Response<ResponseBody>, Error> {
        self.job_queue
            .delete_job(request.queue, request.state, &request.storage_key)
            .await?;

        Ok(build_response(
            StatusCode::NO_CONTENT,
            HeaderMap::new(),
            ResponseBody::empty(),
        )?)
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
        HashMap<Digest, Vec<(Digest, Option<Platform>)>>,
        HashMap<Digest, Vec<ReferrerInfo>>,
    ) {
        // `buffered` keeps the revision order so the merged map values stay
        // deterministic.
        let analyses: Vec<_> = stream::iter(all_revisions.iter().cloned())
            .map(|digest| async move {
                // A body that will not read drops its row rather than failing
                // the whole listing.
                let manifest = read_manifest(&self.blob_store, &digest)
                    .await
                    .ok()
                    .flatten()?;
                let analysis = analyze_manifest(&manifest);
                let mut referrers = Vec::with_capacity(analysis.referrer_candidates.len());
                for referrer in analysis.referrer_candidates {
                    let info = self
                        .enrich_referrer_with_predicate(referrer.info, &referrer.child_digest)
                        .await;
                    referrers.push((referrer.subject, info));
                }
                Some((digest, analysis.parent_links, referrers))
            })
            .buffered(ADMIN_READ_CONCURRENCY)
            .collect()
            .await;

        let mut child_to_parents: HashMap<Digest, Vec<(Digest, Option<Platform>)>> = HashMap::new();
        let mut docker_referrers: HashMap<Digest, Vec<ReferrerInfo>> = HashMap::new();
        for (digest, parent_links, referrers) in analyses.into_iter().flatten() {
            for (child_digest, platform) in parent_links {
                child_to_parents
                    .entry(child_digest)
                    .or_default()
                    .push((digest.clone(), platform));
            }
            for (subject, info) in referrers {
                docker_referrers.entry(subject).or_default().push(info);
            }
        }

        (child_to_parents, docker_referrers)
    }

    /// Enriches `info` with the child manifest's in-toto predicate type
    /// annotation, when it carries one.
    async fn enrich_referrer_with_predicate(
        &self,
        mut info: ReferrerInfo,
        child_digest: &Digest,
    ) -> ReferrerInfo {
        if let Ok(Some(child_manifest)) = read_manifest(&self.blob_store, child_digest).await
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
        child_to_parents: HashMap<Digest, Vec<(Digest, Option<Platform>)>>,
        mut docker_referrers: HashMap<Digest, Vec<ReferrerInfo>>,
    ) -> Vec<ManifestEntry> {
        // Read once for the whole listing, not once per manifest carrying the
        // tag, and bounded by the same fan-out as the reads below.
        let tag_pulls = self.newest_tag_pulls(namespace, digest_to_tags).await;
        let tag_pulls = &tag_pulls;

        // `buffered` below keeps the revision order.
        let seeds: Vec<_> = all_revisions
            .into_iter()
            .map(|digest| {
                let tags = digest_to_tags.get(&digest).cloned().unwrap_or_default();
                let parents = parent_refs_for(&digest, &child_to_parents, digest_to_tags);
                let referrers = docker_referrers.remove(&digest).unwrap_or_default();
                (digest, tags, parents, referrers)
            })
            .collect();

        stream::iter(seeds)
            .map(|(digest, tags, parents, mut referrers)| async move {
                // One page of what this registry holds alone: querying the
                // upstream would do so once per manifest. The fallback-tag
                // lookup stays because the cursor handed back here is followed
                // through the OCI referrers endpoint, which merges that index,
                // so dropping it would cut the cursor over a different
                // candidate set.
                let listing = GetReferrersRequest {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                    artifact_type: None,
                    last: None,
                };
                let mut referrers_next = None;
                if let Ok(page) = self.list_referrers(None, &listing).await {
                    referrers.extend(page.items.into_iter().map(ReferrerInfo::from));
                    referrers_next = page.next_token;
                }

                let pushed_at = self
                    .metadata_store
                    .read_link(namespace, &LinkKind::Digest(digest.clone()))
                    .await
                    .ok()
                    .and_then(|m| m.created_at);
                // A revision's last pull lives in its access entries.
                let last_pulled_at = self
                    .metadata_store
                    .read_revision_access_time(namespace, &digest)
                    .await
                    .ok()
                    .flatten();
                // A pull naming a tag stamps that tag alone, never the revision
                // it resolves to, so a manifest only ever fetched by tag has no
                // revision atime at all. Folding its tags in is what makes the
                // reported time the manifest's last pull rather than its last
                // pull by digest.
                //
                // A tag that later moves to another manifest carries its pull
                // history to the new target, which then reports a pull that
                // happened against the old one. Acceptable for an advisory
                // timestamp, and the alternative is stamping the revision on
                // every tag pull, doubling writes on the hottest path.
                let last_pulled_at = tags
                    .iter()
                    .filter_map(|tag| tag_pulls.get(tag).copied())
                    .chain(last_pulled_at)
                    .max();

                ManifestEntry {
                    digest: digest.to_string(),
                    tags,
                    parents,
                    referrers,
                    referrers_next,
                    pushed_at,
                    last_pulled_at,
                }
            })
            .buffered(ADMIN_READ_CONCURRENCY)
            .collect()
            .await
    }

    /// The newest recorded pull of every tag in `digest_to_tags`, keyed by tag.
    /// A tag with no recorded pull is absent rather than present and null.
    async fn newest_tag_pulls(
        &self,
        namespace: &Namespace,
        digest_to_tags: &HashMap<Digest, Vec<Tag>>,
    ) -> HashMap<Tag, DateTime<Utc>> {
        let tags: Vec<Tag> = digest_to_tags.values().flatten().cloned().collect();
        stream::iter(tags)
            .map(|tag| async move {
                let at = self
                    .metadata_store
                    .read_tag_access_time(namespace, &tag)
                    .await
                    .ok()
                    .flatten()?;
                Some((tag, at))
            })
            .buffered(ADMIN_READ_CONCURRENCY)
            .filter_map(|pull| async move { pull })
            .collect()
            .await
    }

    async fn count_uploads(&self, namespace: &Namespace) -> Result<usize, Error> {
        self.blob_store
            .stream_uploads(namespace)
            .try_fold(0, |count, _| async move { Ok(count + 1) })
            .await
    }

    async fn build_digest_to_tags_map(
        &self,
        namespace: &Namespace,
    ) -> Result<HashMap<Digest, Vec<Tag>>, Error> {
        let mut all_tags: Vec<Tag> = self
            .metadata_store
            .stream_tags(namespace)
            .try_collect()
            .await?;
        all_tags.sort();

        // `buffered` keeps the sorted tag order so each digest's tag list stays
        // deterministic; a tag whose link read fails is skipped.
        let tag_links: Vec<(Tag, Digest)> = stream::iter(all_tags)
            .map(|tag| async move {
                let link = LinkKind::Tag(tag.clone());
                let metadata = self.metadata_store.read_link(namespace, &link).await.ok()?;
                Some((tag, metadata.target))
            })
            .buffered(ADMIN_READ_CONCURRENCY)
            .filter_map(|pair| async move { pair })
            .collect()
            .await;

        Ok(tag_links
            .into_iter()
            .fold(HashMap::new(), |mut map, (tag, digest)| {
                map.entry(digest).or_default().push(tag);
                map
            }))
    }

    async fn list_repository_namespaces(&self, repository: &str) -> Result<Vec<Namespace>, Error> {
        if !self.resolver.contains_key(repository) {
            return Err(Error::NameUnknown);
        }

        self.collect_namespaces(Some(repository)).await
    }

    /// Every namespace across both stores, sorted and deduplicated; `scope`
    /// restricts the listing to one repository's key range. The blob store's
    /// `_uploads` listing is merged to surface a namespace holding only
    /// in-progress uploads.
    ///
    /// Names come straight off the `v2/cat` index with no per-namespace
    /// content probe: an admin listing showing a name whose content was just
    /// emptied, until scrub reaps the key, is worth far more than one extra
    /// round trip per namespace.
    async fn collect_namespaces(&self, scope: Option<&str>) -> Result<Vec<Namespace>, Error> {
        let (mut namespaces, upload_namespaces) = try_join!(
            self.metadata_store.list_indexed_namespaces(scope),
            self.blob_store.collect_upload_namespaces(scope),
        )?;
        namespaces.extend(upload_namespaces);

        namespaces.sort_unstable();
        namespaces.dedup();
        Ok(namespaces)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tokio::time::sleep;

    use angos_storage::{
        Error as StorageError, ObjectStore,
        test_util::{HookedStore, StoreHook, StoreOp},
    };

    use angos_oci::{
        DOCKER_REFERENCE_DIGEST, Descriptor, Digest, IN_TOTO_PREDICATE_TYPE, Manifest, Namespace,
        Platform, Reference, Tag, UploadSessionId,
    };

    use chrono::{DateTime, Duration as ChronoDuration, Utc};

    use crate::registry::admin::{
        analyze_manifest, extract_docker_referrer, extract_in_toto_predicate, parent_refs_for,
    };
    use serde_json::Value;

    use crate::registry::admin::ListPullsRequest;
    use crate::registry::keys::NamespaceKeys;
    use crate::registry::metadata_store::{
        AccessEntry, MetadataStore,
        access_time::{atime_client_suffix, atime_entry_name},
        tag_ord,
    };
    use crate::registry::{
        Registry,
        metadata_store::{LinkKind, LinkOperation},
        test_utils::{
            FSRegistryTestCase, RegistryTestCase, create_test_blob, create_test_registry,
            for_each_backend, media_type, metadata_store_over, response_json,
        },
    };

    /// Holds every intercepted read for a beat and records whether a tag-side
    /// and a revision-side read were ever in flight together. Counting reads
    /// in aggregate would not do: one count's own reads already overlap, so
    /// only a cross-count overlap distinguishes the two orderings.
    struct OverlapProbe {
        tags_in_flight: Arc<AtomicUsize>,
        revisions_in_flight: Arc<AtomicUsize>,
        overlapped: Arc<AtomicBool>,
    }

    impl OverlapProbe {
        /// The counter for the side `prefix` belongs to, if any. Tags and
        /// revisions live under disjoint key prefixes.
        fn side(&self, prefix: &str) -> Option<&Arc<AtomicUsize>> {
            if prefix.contains("!tag") {
                Some(&self.tags_in_flight)
            } else if prefix.contains("!rev") || prefix.contains("_manifests/revisions") {
                Some(&self.revisions_in_flight)
            } else {
                None
            }
        }
    }

    #[async_trait::async_trait]
    impl StoreHook for OverlapProbe {
        async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
            let (StoreOp::List { prefix } | StoreOp::ListChildren { prefix }) = op else {
                return Ok(());
            };
            let Some(side) = self.side(prefix) else {
                return Ok(());
            };
            let other = if Arc::ptr_eq(side, &self.tags_in_flight) {
                &self.revisions_in_flight
            } else {
                &self.tags_in_flight
            };

            side.fetch_add(1, Ordering::SeqCst);
            if other.load(Ordering::SeqCst) > 0 {
                self.overlapped.store(true, Ordering::SeqCst);
            }
            sleep(Duration::from_millis(20)).await;
            if other.load(Ordering::SeqCst) > 0 {
                self.overlapped.store(true, Ordering::SeqCst);
            }
            side.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// A namespace's tag and manifest counts are issued together, not one after
    /// the other: on a remote store each is a round trip, and the listing pays
    /// them for every namespace it returns.
    #[tokio::test]
    async fn namespace_counts_are_issued_concurrently() {
        let case = FSRegistryTestCase::new();
        let namespace = Namespace::new("test-repo/counted").unwrap();
        create_test_blob(case.registry(), &namespace, b"counted").await;

        let overlapped = Arc::new(AtomicBool::new(false));
        let hooked: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(
            case.metadata_store().object_store().clone(),
            OverlapProbe {
                tags_in_flight: Arc::new(AtomicUsize::new(0)),
                revisions_in_flight: Arc::new(AtomicUsize::new(0)),
                overlapped: overlapped.clone(),
            },
        ));
        let registry = create_test_registry(case.blob_store(), metadata_store_over(hooked));

        registry
            .get_namespaces_info(&Namespace::new("test-repo").unwrap())
            .await
            .unwrap();

        assert!(
            overlapped.load(Ordering::SeqCst),
            "the tag and revision counts of one namespace must be in flight together"
        );
    }

    fn digest(hex_suffix: &str) -> Digest {
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
        Manifest::image(None, layers)
    }

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
        let platform = Platform {
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
            ..Manifest::index(vec![child])
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
            ..Manifest::index(vec![referrer_child, index_child])
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

    #[test]
    fn parent_refs_for_returns_empty_when_digest_not_in_parent_map() {
        let child_to_parents: HashMap<Digest, Vec<(Digest, Option<Platform>)>> = HashMap::new();
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
        let platform = Platform {
            os: "linux".to_string(),
            architecture: "arm64".to_string(),
            variant: Some("v8".to_string()),
            ..Platform::default()
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
    /// child, yet must still be listed with its upload count, exactly once.
    #[tokio::test]
    async fn namespaces_info_includes_upload_only_namespace() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();

            let upload_only = Namespace::new("test-repo/upload-only").unwrap();
            registry
                .blob_store
                .create_upload(&upload_only, &UploadSessionId::generate(), None)
                .await
                .unwrap();

            let mixed = Namespace::new("test-repo/mixed").unwrap();
            create_test_blob(registry, &mixed, b"mixed content").await;
            registry
                .blob_store
                .create_upload(&mixed, &UploadSessionId::generate(), None)
                .await
                .unwrap();

            let response = registry
                .get_namespaces_info(&Namespace::new("test-repo").unwrap())
                .await
                .unwrap();
            let body = response_json(response).await;
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
            let body = response_json(response).await;
            let count = body["repositories"][0]["namespace_count"].as_u64().unwrap();
            assert_eq!(
                count, 2,
                "the repository namespace count must include the upload-only namespace"
            );
        })
        .await;
    }

    /// Tags are counted from the tag directory, not derived from revisions: the
    /// seeded namespace carries two tags and no revision.
    #[tokio::test]
    async fn namespaces_info_counts_tags_not_manifests() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();

            let namespace = Namespace::new("test-repo/multi-tag").unwrap();
            let (digest, _) = create_test_blob(registry, &namespace, b"multi tag content").await;
            registry
                .metadata_store
                .update_links(
                    &namespace,
                    &[LinkOperation::create(
                        LinkKind::Tag(Tag::new("v1.0").unwrap()),
                        digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let response = registry
                .get_namespaces_info(&Namespace::new("test-repo").unwrap())
                .await
                .unwrap();
            let body = response_json(response).await;
            let entry = body["namespaces"]
                .as_array()
                .unwrap()
                .iter()
                .find(|ns| ns["name"] == "test-repo/multi-tag")
                .expect("the seeded namespace must be listed");

            assert_eq!(entry["tag_count"], 2, "both tags must be counted: {entry}");
            assert_eq!(
                entry["manifest_count"], 0,
                "the counts come from different sources: {entry}"
            );
        })
        .await;
    }

    /// On split backends upload sessions exist only on the blob store, so the
    /// listing must discover an upload-only namespace there.
    #[tokio::test]
    async fn namespaces_info_finds_upload_only_namespace_across_split_backends() {
        let test_case = FSRegistryTestCase::with_split_backends();
        let registry = test_case.registry();

        let namespace = Namespace::new("test-repo/upload-only").unwrap();
        registry
            .blob_store
            .create_upload(&namespace, &UploadSessionId::generate(), None)
            .await
            .unwrap();

        let response = registry
            .get_namespaces_info(&Namespace::new("test-repo").unwrap())
            .await
            .unwrap();
        let body = response_json(response).await;
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

    #[tokio::test]
    async fn namespaces_info_is_scoped_to_the_requested_repository() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();

            let kept = Namespace::new("test-repo/kept").unwrap();
            create_test_blob(registry, &kept, b"kept content").await;

            let other = Namespace::new("other-repo/hidden").unwrap();
            create_test_blob(registry, &other, b"hidden content").await;

            let response = registry
                .get_namespaces_info(&Namespace::new("test-repo").unwrap())
                .await
                .unwrap();
            let body = response_json(response).await;
            let names: Vec<&str> = body["namespaces"]
                .as_array()
                .unwrap()
                .iter()
                .map(|ns| ns["name"].as_str().unwrap())
                .collect();

            assert_eq!(
                names,
                ["test-repo/kept"],
                "only the requested repository's namespaces must be listed; got: {names:?}"
            );
        })
        .await;
    }

    /// A directory whose name is not a valid namespace must not take the whole
    /// listing down with it.
    #[tokio::test]
    async fn namespaces_info_skips_an_invalid_namespace_directory() {
        let test_case = FSRegistryTestCase::new();
        let registry = test_case.registry();

        let valid = Namespace::new("test-repo/valid").unwrap();
        create_test_blob(registry, &valid, b"valid content").await;

        // Uppercase is outside the namespace grammar, so this directory can
        // only have arrived from outside the write paths.
        test_case
            .metadata_store()
            .object_store()
            .put(
                "v2/repositories/test-repo/BAD/_manifests/tags/v1/current/link",
                Bytes::from_static(b"{}"),
            )
            .await
            .unwrap();

        let response = registry
            .get_namespaces_info(&Namespace::new("test-repo").unwrap())
            .await
            .expect("one invalid directory must not fail the listing");
        let body = response_json(response).await;
        let names: Vec<&str> = body["namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .map(|ns| ns["name"].as_str().unwrap())
            .collect();

        assert_eq!(names, ["test-repo/valid"]);
    }

    /// Plant one access entry at `at`, the shape a stamped pull writes.
    async fn put_pull_entry(
        metadata_store: &MetadataStore,
        dir: &str,
        client: &str,
        at: DateTime<Utc>,
    ) {
        let name = atime_entry_name(tag_ord(Some(at)), &atime_client_suffix(client));
        let body = serde_json::to_vec(&AccessEntry {
            client: client.to_string(),
            at,
        })
        .unwrap();
        metadata_store
            .object_store()
            .put(&format!("{dir}/{name}"), Bytes::from(body))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pull_history_lists_a_tag_newest_first_with_the_configured_window() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("test-repo/pulls").unwrap();
            let tag = Tag::new("v1").unwrap();
            let dir = namespace.tag_atime_entry_dir(&tag);

            let now = Utc::now();
            put_pull_entry(&metadata_store, &dir, "alice", now).await;
            put_pull_entry(&metadata_store, &dir, "bob", now - ChronoDuration::hours(2)).await;
            // An unparseable body must be skipped, not fail the listing.
            metadata_store
                .object_store()
                .put(
                    &format!("{dir}/{}", atime_entry_name(tag_ord(Some(now)), "ffffffff")),
                    Bytes::from_static(b"not json"),
                )
                .await
                .unwrap();

            let response = registry
                .get_pull_history(ListPullsRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Tag(tag.clone()),
                })
                .await
                .unwrap();
            let body = response_json(response).await;

            assert_eq!(body["target"], "v1");
            assert_eq!(body["window_secs"], 3600);
            let clients: Vec<&str> = body["entries"]
                .as_array()
                .unwrap()
                .iter()
                .map(|entry| entry["client"].as_str().unwrap())
                .collect();
            assert_eq!(clients, ["alice", "bob"], "entries must be newest first");
        })
        .await;
    }

    /// Seed one revision record and point `tags` at it, the shape a push
    /// leaves behind.
    async fn seed_tagged_revision(
        metadata_store: &MetadataStore,
        namespace: &Namespace,
        target: &Digest,
        tags: &[&str],
    ) {
        let mut ops = vec![LinkOperation::create(
            LinkKind::Digest(target.clone()),
            target.clone(),
        )];
        for tag in tags {
            ops.push(LinkOperation::create(
                LinkKind::Tag(Tag::new(tag).unwrap()),
                target.clone(),
            ));
        }
        metadata_store.update_links(namespace, &ops).await.unwrap();
    }

    /// An access entry is named by a millisecond ordinal, so a fixture instant
    /// is truncated to what a read of it can return.
    fn pulled_ago(ago: ChronoDuration) -> DateTime<Utc> {
        let at = Utc::now() - ago;
        DateTime::from_timestamp_millis(at.timestamp_millis()).unwrap()
    }

    async fn last_pulled_of(registry: &Registry, namespace: &Namespace, target: &Digest) -> Value {
        let response = registry
            .get_revisions_info(&namespace.clone())
            .await
            .unwrap();
        let body = response_json(response).await;
        body["manifests"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["digest"] == target.to_string())
            .unwrap_or_else(|| panic!("the seeded revision must be listed: {body}"))["last_pulled_at"]
            .clone()
    }

    /// A kubelet re-resolving `:main` sends only `HEAD .../manifests/main`,
    /// which stamps the tag and never the revision it resolves to. The listing
    /// must still report that pull.
    #[tokio::test]
    async fn last_pulled_at_reports_a_pull_that_only_named_the_tag() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("test-repo/tag-pulled").unwrap();
            let target = digest("da61");
            seed_tagged_revision(&metadata_store, &namespace, &target, &["main"]).await;

            assert_eq!(
                last_pulled_of(registry, &namespace, &target).await,
                Value::Null,
                "nothing has been pulled yet"
            );

            let pulled_at = pulled_ago(ChronoDuration::minutes(5));
            put_pull_entry(
                &metadata_store,
                &namespace.tag_atime_entry_dir(&Tag::new("main").unwrap()),
                "kubelet",
                pulled_at,
            )
            .await;

            let reported = last_pulled_of(registry, &namespace, &target).await;
            assert_eq!(
                reported
                    .as_str()
                    .map(|at| at.parse::<DateTime<Utc>>().unwrap()),
                Some(pulled_at),
                "a tag-only pull must surface as the manifest's last pull; got {reported}"
            );
        })
        .await;
    }

    /// The manifest's last pull is the newest across everything that names it,
    /// not whichever tag happens to be read first.
    #[tokio::test]
    async fn last_pulled_at_takes_the_newest_of_several_tags() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("test-repo/many-tags").unwrap();
            let target = digest("da62");
            seed_tagged_revision(&metadata_store, &namespace, &target, &["old", "new"]).await;

            let older = pulled_ago(ChronoDuration::hours(6));
            let newest = pulled_ago(ChronoDuration::minutes(1));
            put_pull_entry(
                &metadata_store,
                &namespace.tag_atime_entry_dir(&Tag::new("old").unwrap()),
                "alice",
                older,
            )
            .await;
            put_pull_entry(
                &metadata_store,
                &namespace.tag_atime_entry_dir(&Tag::new("new").unwrap()),
                "bob",
                newest,
            )
            .await;

            let reported = last_pulled_of(registry, &namespace, &target).await;
            assert_eq!(
                reported
                    .as_str()
                    .map(|at| at.parse::<DateTime<Utc>>().unwrap()),
                Some(newest),
                "the freshest tag pull must win; got {reported}"
            );
        })
        .await;
    }

    /// Folding tags in must not disturb a manifest that has none: its revision
    /// atime is still the only thing that can report a pull.
    #[tokio::test]
    async fn last_pulled_at_of_an_untagged_manifest_stays_revision_only() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("test-repo/untagged").unwrap();
            let target = digest("da63");
            seed_tagged_revision(&metadata_store, &namespace, &target, &[]).await;

            assert_eq!(
                last_pulled_of(registry, &namespace, &target).await,
                Value::Null,
                "an untagged, unpulled manifest reports no pull"
            );

            let pulled_at = pulled_ago(ChronoDuration::minutes(2));
            put_pull_entry(
                &metadata_store,
                &namespace.revision_atime_entry_dir(&target),
                "carol",
                pulled_at,
            )
            .await;

            let reported = last_pulled_of(registry, &namespace, &target).await;
            assert_eq!(
                reported
                    .as_str()
                    .map(|at| at.parse::<DateTime<Utc>>().unwrap()),
                Some(pulled_at),
                "a by-digest pull must still be reported; got {reported}"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn pull_history_lists_a_revision_through_the_stamping_path() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("test-repo/pulls-rev").unwrap();
            let target = digest("beef1");
            let link = LinkKind::Digest(target.clone());
            metadata_store
                .update_links(
                    &namespace,
                    &[LinkOperation::create(link.clone(), target.clone())],
                )
                .await
                .unwrap();
            metadata_store
                .read_link_recording_access(&namespace, &link, "carol")
                .await
                .unwrap();

            let response = registry
                .get_pull_history(ListPullsRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Digest(target.clone()),
                })
                .await
                .unwrap();
            let body = response_json(response).await;

            assert_eq!(body["target"], target.to_string());
            let entries = body["entries"].as_array().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0]["client"], "carol");
        })
        .await;
    }

    /// A target nobody pulled answers with an empty list, not a 404.
    #[tokio::test]
    async fn pull_history_of_an_unpulled_target_is_empty() {
        for_each_backend(async |test_case| {
            let response = test_case
                .registry()
                .get_pull_history(ListPullsRequest {
                    namespace: Namespace::new("test-repo/quiet").unwrap(),
                    reference: Reference::Tag(Tag::new("never").unwrap()),
                })
                .await
                .unwrap();
            let body = response_json(response).await;

            assert!(body["entries"].as_array().unwrap().is_empty());
        })
        .await;
    }
}
