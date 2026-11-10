//! The wire contract of the `_angos/` extension endpoints the web UI and
//! operators use. None of it is in the OCI or Docker spec, so it stays out of
//! `angos_oci_service`; the registry crate supplies the behaviour.

use std::collections::HashMap;
use std::num::NonZeroU16;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use angos_oci::http_range::{RequestRange, ResponseRange};
use angos_oci::{
    Descriptor, Digest, MediaType, Namespace, Platform, Reference, Tag, UploadSessionId,
};

pub mod endpoint;
pub use endpoint::{Endpoint, parse};

#[cfg(feature = "hyper")]
mod render;

#[derive(Deserialize, Debug, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

/// A page of a listing, `?offset=&n=`: `n` rows from `offset`, `n` defaulting
/// to the server's page size.
#[derive(Deserialize, Debug, Default, Clone, Copy, PartialEq, Eq)]
#[serde(default)]
pub struct PageRequest {
    pub offset: u32,
    pub n: Option<NonZeroU16>,
}

#[derive(Deserialize, Debug, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RevisionSort {
    #[default]
    Tag,
    Digest,
}

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
    /// Rows across every page.
    pub total: usize,
    /// The offset of the next page, when there is one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<u32>,
}

/// The counts are absent when `namespace_counts` is off.
#[derive(Serialize, Debug)]
pub struct NamespaceInfo {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_count: Option<usize>,
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
    /// Rows across every page.
    pub total: usize,
    /// The offset of the next page, when there is one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct ListNamespacesRequest {
    pub repository: Namespace,
    /// Lists only the namespaces nested below this one, which must be the
    /// repository or one of its namespaces.
    pub under: Option<Namespace>,
    pub order: SortOrder,
    pub page: PageRequest,
}

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
    /// The page's top-level revisions, each followed by the revisions it holds.
    pub manifests: Vec<ManifestEntry>,
    /// Top-level revisions across every page.
    pub total: usize,
    /// The offset of the next page, when there is one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<u32>,
}

/// What a revision listing serves: a page of the top-level revisions, or one
/// revision wherever it sits. Either way each comes with the revisions it
/// holds, its platform manifests and referrers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevisionSelection {
    Page {
        sort: RevisionSort,
        order: SortOrder,
        page: PageRequest,
    },
    Digest(Digest),
}

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
    /// Rows across every page.
    pub total: usize,
    /// The offset of the next page, when there is one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<u32>,
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

#[derive(Debug, Clone)]
pub struct ListPullsRequest {
    pub namespace: Namespace,
    pub reference: Reference,
    /// The page of pulls, newest first.
    pub page: PageRequest,
}

/// A tar entry's kind in a layer listing.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EntryKind {
    File,
    Dir,
    Symlink,
    Hardlink,
    /// `.wh.<name>`: the lower layers' `<name>` is gone; `path` names it.
    Whiteout,
    /// `.wh..wh..opq`: the lower layers' content of `path` is gone.
    Opaque,
    Other,
}

/// One tar entry, with where its data starts in the uncompressed stream; the
/// indexer stores it as served.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LayerEntry {
    pub path: String,
    pub kind: EntryKind,
    pub size: u64,
    pub mode: u32,
    pub uid: u64,
    pub gid: u64,
    pub mtime: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
    pub offset: u64,
    /// Set on files only, and missing from listings indexed before it existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<FileContent>,
    /// The Linux capabilities its `security.capability` attribute permits.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
}

/// A file's digests, hex-encoded, its media type, and the credentials its
/// first bytes give away.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FileContent {
    pub sha256: String,
    pub sha512: String,
    pub mime_type: String,
    /// In line order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<Secret>,
}

/// A credential, and the line it is on from 1.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Secret {
    pub kind: SecretKind,
    pub line: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum SecretKind {
    /// A PEM, OpenSSH or PGP private key block.
    PrivateKey,
    /// An `aws_secret_access_key` line of an AWS credentials file.
    AwsCredentials,
    /// An `auth` entry of a Docker `config.json`.
    RegistryAuth,
    /// An `.npmrc` registry token or password.
    NpmToken,
    /// A URL carrying a user and a password, as `.git-credentials` keeps them.
    GitCredentials,
    /// A `.netrc` machine with its password.
    Netrc,
    /// A GitHub personal access, OAuth or app token.
    GithubToken,
    /// A GitLab personal access token.
    GitlabToken,
    /// A Slack bot or user token.
    SlackToken,
    /// A Stripe live secret or restricted key.
    StripeKey,
    /// An AWS access key ID, long-lived or temporary.
    AwsAccessKey,
    /// A kubeconfig's client key or bearer token.
    Kubeconfig,
}

/// A layer's entries, sorted by path.
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

#[derive(Debug, Clone)]
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

/// The interface's own copy of the job engine's queue set.
#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Queue {
    #[default]
    Cache,
    Replication,
    Scan,
    Index,
}

#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Pending,
    Failed,
}

/// A job's storage key: never empty and never holding a `/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobKey(String);

impl JobKey {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for JobKey {
    type Error = String;

    fn try_from(key: String) -> Result<Self, Self::Error> {
        if key.is_empty() || key.contains('/') {
            return Err(format!("invalid job key: '{key}'"));
        }
        Ok(Self(key))
    }
}

#[derive(Debug, Clone)]
pub struct ListJobsRequest {
    pub queue: Queue,
    pub n: Option<NonZeroU16>,
    pub after: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RetryJobRequest {
    pub queue: Queue,
    pub storage_key: JobKey,
}

#[derive(Debug, Clone)]
pub struct DeleteJobRequest {
    pub queue: Queue,
    pub state: JobState,
    pub storage_key: JobKey,
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
