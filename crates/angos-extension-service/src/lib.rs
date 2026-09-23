//! Angos's own registry API, the `_angos/` extension endpoints the web UI and
//! operators use: the repository/namespace/manifest listings, upload and pull
//! history, the layer filesystem index, and the durable-job administration.
//!
//! None of this is in the OCI or Docker spec, so it lives behind its own trait,
//! keeping [`angos_oci_service`](../angos_oci_service/index.html) exactly the
//! Distribution spec. The types here are the extension's wire interface; the
//! registry crate fills them in.
//!
//! `Queue` and `JobState` are declared here as the job-administration
//! interface's own vocabulary; these convert into the job engine's copies at
//! the boundary.

use std::{collections::HashMap, num::NonZeroU16};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncRead;

use angos_oci::http_range::{RequestRange, ResponseRange};
use angos_oci::{
    Descriptor, Digest, MediaType, Namespace, Platform, Reference, Tag, UploadSessionId,
};

pub mod endpoint;
pub use endpoint::{Endpoint, parse};

#[cfg(feature = "hyper")]
mod render;

/// A `204 No Content`: a job the registry retried or removed, in full. No body.
pub struct NoContent;

// ---- Repository / namespace listings --------------------------------------

#[derive(Serialize, Debug)]
pub struct RepositoryInfo {
    pub name: String,
    pub namespace_count: usize,
    pub pull_through_cache: bool,
    pub upstream_urls: Vec<String>,
    pub immutable_tags: bool,
}

#[derive(Serialize, Debug)]
pub struct RepositoriesBody {
    pub repositories: Vec<RepositoryInfo>,
}

#[derive(Serialize, Debug)]
pub struct NamespaceInfo {
    pub name: String,
    pub tag_count: usize,
    pub manifest_count: usize,
    pub upload_count: usize,
}

#[derive(Serialize, Debug)]
pub struct NamespacesBody {
    pub repository: String,
    pub namespaces: Vec<NamespaceInfo>,
    pub pull_through_cache: bool,
    pub upstream_urls: Vec<String>,
    pub immutable_tags: bool,
    /// The immutable-tag exclusion patterns in their source form.
    pub immutable_tags_exclusions: Vec<String>,
}

// ---- Manifest (revision) listing ------------------------------------------

#[derive(Serialize, Debug, Clone)]
pub struct ParentRef {
    pub digest: String,
    pub tags: Vec<Tag>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<Platform>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReferrerInfo {
    pub digest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_type: Option<MediaType>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub annotations: HashMap<String, String>,
}

impl From<&Descriptor> for ReferrerInfo {
    fn from(descriptor: &Descriptor) -> Self {
        ReferrerInfo {
            digest: descriptor.digest.to_string(),
            artifact_type: descriptor.artifact_type.clone(),
            annotations: descriptor.annotations.clone(),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct ManifestEntry {
    pub digest: String,
    pub tags: Vec<Tag>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub parents: Vec<ParentRef>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub referrers: Vec<ReferrerInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub referrers_next: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pushed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_pulled_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Debug)]
pub struct RevisionsBody {
    pub name: String,
    pub manifests: Vec<ManifestEntry>,
}

// ---- Uploads / pull history -----------------------------------------------

#[derive(Serialize, Debug)]
pub struct UploadEntry {
    #[serde(rename = "uuid")]
    pub session_id: UploadSessionId,
    pub size: u64,
    pub started_at: DateTime<Utc>,
}

#[derive(Serialize, Debug)]
pub struct UploadsBody {
    pub name: String,
    pub uploads: Vec<UploadEntry>,
}

/// One recorded pull: who pulled, from where, and when.
#[derive(Serialize, Deserialize, Debug)]
pub struct AccessEntry {
    pub client: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    pub at: DateTime<Utc>,
}

#[derive(Serialize, Debug)]
pub struct PullsBody {
    pub target: String,
    /// Pulls the history keeps at most.
    pub max_pulls: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_age_secs: Option<u64>,
    pub entries: Vec<AccessEntry>,
    /// The offset of the next page, when there is one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<u32>,
}

#[derive(Debug)]
pub struct ListPullsRequest {
    pub namespace: Namespace,
    pub reference: Reference,
    /// Pulls to skip, newest first.
    pub offset: u32,
    pub n: Option<NonZeroU16>,
}

// ---- Layer filesystem index -----------------------------------------------

/// A tar entry's kind in a layer listing.
#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EntryKind {
    File,
    Dir,
    Symlink,
    Hardlink,
    Whiteout,
    Opaque,
    Other,
}

/// One tar entry with where its data starts in the uncompressed stream.
#[derive(Serialize, Debug, Clone)]
pub struct LayerEntry {
    pub path: String,
    pub kind: EntryKind,
    pub size: u64,
    pub mode: u32,
    pub uid: u64,
    pub gid: u64,
    pub mtime: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
    pub offset: u64,
    /// Set on files only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<FileContent>,
    /// The Linux capabilities its `security.capability` attribute permits.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
}

/// A file's digests, hex-encoded, its media type, and the credentials its
/// first bytes give away.
#[derive(Serialize, Debug, Clone)]
pub struct FileContent {
    pub sha256: String,
    pub sha512: String,
    pub mime_type: String,
    /// In line order.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<Secret>,
}

/// A credential, and the line it is on from 1.
#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Secret {
    pub kind: SecretKind,
    pub line: usize,
}

#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SecretKind {
    PrivateKey,
    AwsCredentials,
    RegistryAuth,
    NpmToken,
    GitCredentials,
    Netrc,
    GithubToken,
    GitlabToken,
    SlackToken,
    StripeKey,
    AwsAccessKey,
    Kubeconfig,
}

/// A layer's entries in tar order.
#[derive(Serialize, Debug)]
pub struct LayerListing {
    /// An older version wrote it: a job is walking the layer again.
    pub refreshing: bool,
    pub compressed: bool,
    pub uncompressed_size: u64,
    pub entries: Vec<LayerEntry>,
}

#[derive(Debug)]
pub struct LayerEntriesRequest {
    pub namespace: Namespace,
    pub digest: Digest,
    /// Whether the client takes a gzipped listing.
    pub gzip: bool,
}

/// The listing of a layer, or a signal that it is still being indexed. The two
/// arms are exclusive: an indexing layer has no listing yet, and a ready one is
/// never also indexing.
pub enum LayerEntries {
    Indexing,
    /// The [`LayerListing`] as JSON, gzipped when `gzip` is set.
    Ready {
        body: Vec<u8>,
        gzip: bool,
    },
}

#[derive(Debug)]
pub struct LayerFileRequest {
    pub namespace: Namespace,
    pub digest: Digest,
    pub path: String,
    /// Whether to answer with a download disposition rather than inline.
    pub download: bool,
    /// A `Range` asking for part of the file only.
    pub range: Option<RequestRange>,
}

#[derive(Debug)]
pub struct LayerFileDetailsRequest {
    pub namespace: Namespace,
    pub digest: Digest,
    pub path: String,
}

/// What a file of a layer holds that its bytes spell only once decoded: an ELF
/// binary's header and libraries, or a PEM file's certificates.
#[derive(Serialize, Debug, Default)]
pub struct LayerFileDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elf: Option<ElfDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub certificates: Option<Vec<PemBlock>>,
}

/// What `file` would say of an ELF binary, and the libraries it needs.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ElfDetails {
    #[serde(rename = "type")]
    pub kind: String,
    pub machine: String,
    pub bits: u8,
    pub endian: String,
    pub entry: String,
    /// The dynamic loader it names, absent for a static binary or a shared object.
    pub interpreter: Option<String>,
    pub dynamic: bool,
    pub needed: Vec<String>,
    /// The name a shared library answers to.
    pub soname: Option<String>,
    pub build_id: Option<String>,
    /// How much of the relocations turn read-only once loaded: `full`,
    /// `partial` or `none`.
    pub relro: String,
    pub executable_stack: bool,
}

/// One block of a PEM file: a certificate decoded, anything else by its label.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct PemBlock {
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub certificate: Option<Certificate>,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct Certificate {
    pub subject: String,
    pub issuer: String,
    pub not_before: DateTime<Utc>,
    pub not_after: DateTime<Utc>,
    /// The DNS names and IPv4 addresses it is for.
    pub names: Vec<String>,
}

/// One file streamed out of a layer: the reader delivers exactly `size` bytes,
/// so a large file never buffers in full. `content_type` is the media type the
/// response carries, `download` asks for an attachment disposition, and `range`
/// is the part of the file served when one was asked for.
pub struct LayerFile<R> {
    pub path: String,
    pub content_type: String,
    pub size: u64,
    pub download: bool,
    pub range: Option<ResponseRange>,
    pub reader: R,
}

// ---- Durable-job administration -------------------------------------------

/// Page size for the durable job-queue listings when the client sends no `?n=`.
pub const DEFAULT_JOBS_PAGE: u16 = 100;

/// The queue a job belongs to, the job-administration interface's own copy of
/// the engine's queue set.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Queue {
    Cache,
    Replication,
    Scan,
    Index,
}

/// Whether a job is pending or dead-lettered.
#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Pending,
    Failed,
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

#[derive(Serialize, Debug)]
pub struct JobEntry {
    pub storage_key: String,
    pub id: String,
    pub kind: String,
    pub lock_key: String,
    pub attempts: u32,
    pub max_attempts: u32,
    pub created_at: DateTime<Utc>,
    pub not_before: DateTime<Utc>,
}

#[derive(Serialize, Debug)]
pub struct JobsBody {
    pub jobs: Vec<JobEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct FailedJobEntry {
    pub storage_key: String,
    pub id: String,
    pub kind: String,
    pub lock_key: String,
    pub attempts: u32,
    pub max_attempts: u32,
    pub created_at: DateTime<Utc>,
    pub failed_at: DateTime<Utc>,
    pub last_error: String,
}

#[derive(Serialize, Debug)]
pub struct FailedJobsBody {
    pub failed: Vec<FailedJobEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

// ---- The service ----------------------------------------------------------

/// Whether a caller may see a namespace in a listing.
///
/// Listing authorization is the transport's, not the service's: the transport
/// implements this over its authorizer and the service consults it per entry.
/// Blanket-implemented for any `Fn(&Namespace) -> bool`, so a caller may pass a
/// closure where a `&dyn NamespaceVisibility` is expected.
pub trait NamespaceVisibility: Send + Sync {
    fn allows(&self, namespace: &Namespace) -> bool;
}

impl<F: Fn(&Namespace) -> bool + Send + Sync> NamespaceVisibility for F {
    fn allows(&self, namespace: &Namespace) -> bool {
        self(namespace)
    }
}

/// Angos's `_angos/` registry API.
///
/// Most endpoints are authorized at the route alone, so the service takes no
/// actor. The two listings that span repositories are the exception: a route
/// check cannot speak for entries the caller may not see, so they take a
/// [`NamespaceVisibility`] and drop those. `Body` is the reader a layer-file
/// response streams from.
#[async_trait]
pub trait AngosExtensionService: Send + Sync {
    type Body: AsyncRead + Send + 'static;
    type Error;

    /// `GET /v2/_angos/repositories/list`. Serves the repositories `visibility`
    /// admits, each counting only the namespaces it admits.
    async fn list_repositories(
        &self,
        visibility: &dyn NamespaceVisibility,
    ) -> Result<RepositoriesBody, Self::Error>;

    /// `GET /v2/_angos/namespaces/list?repository=`. Serves the namespaces
    /// `visibility` admits.
    async fn list_namespaces(
        &self,
        repository: Namespace,
        visibility: &dyn NamespaceVisibility,
    ) -> Result<NamespacesBody, Self::Error>;

    /// `GET /v2/<name>/_angos/revisions/list`.
    async fn list_revisions(&self, namespace: Namespace) -> Result<RevisionsBody, Self::Error>;

    /// `GET /v2/<name>/_angos/uploads/list`.
    async fn list_uploads(&self, namespace: Namespace) -> Result<UploadsBody, Self::Error>;

    /// `GET /v2/<name>/_angos/pulls/list?tag=|digest=`.
    async fn list_pulls(&self, request: ListPullsRequest) -> Result<PullsBody, Self::Error>;

    /// `GET /v2/{namespace}/_angos/layers/{digest}/entries`.
    async fn list_layer_entries(
        &self,
        request: LayerEntriesRequest,
    ) -> Result<LayerEntries, Self::Error>;

    /// `GET /v2/{namespace}/_angos/layers/{digest}/file`.
    async fn get_layer_file(
        &self,
        request: LayerFileRequest,
    ) -> Result<LayerFile<Self::Body>, Self::Error>;

    /// `GET /v2/{namespace}/_angos/layers/{digest}/details`.
    async fn get_layer_file_details(
        &self,
        request: LayerFileDetailsRequest,
    ) -> Result<LayerFileDetails, Self::Error>;

    /// `GET /v2/_angos/jobs/list`.
    async fn list_jobs(&self, request: ListJobsRequest) -> Result<JobsBody, Self::Error>;

    /// `GET /v2/_angos/jobs/failed`.
    async fn list_failed_jobs(
        &self,
        request: ListJobsRequest,
    ) -> Result<FailedJobsBody, Self::Error>;

    /// `POST /v2/_angos/jobs/retry`.
    async fn retry_job(&self, request: RetryJobRequest) -> Result<NoContent, Self::Error>;

    /// `DELETE /v2/_angos/jobs`.
    async fn delete_job(&self, request: DeleteJobRequest) -> Result<NoContent, Self::Error>;
}
