//! Namespace resolution and URL builders for upstream registry requests.

use crate::{
    oci::{Digest, Reference},
    registry_client::RegistryClient,
};

/// Sentinel `local_name` meaning "do not rewrite the namespace": passed by the
/// replication push path, where the downstream uses the SAME namespace as local
/// (unlike the pull-mirror path, which strips a local repo-name prefix). It is
/// the identity transform for [`get_upstream_namespace`].
pub const NO_LOCAL_PREFIX: &str = "";

/// Resolves the upstream namespace from a local repository reference by
/// stripping the configured pull-mirror prefix.
///
/// `local_name` is the local repository name whose prefix the pull-mirror path
/// strips off `upstream_name` (e.g. local `local/repo` -> upstream `repo`). When
/// no prefix matches, `upstream_name` is returned unchanged. Pass
/// [`NO_LOCAL_PREFIX`] to disable the rewrite entirely: the identity transform
/// used by replication, which keeps the downstream namespace identical to local.
#[must_use]
pub fn get_upstream_namespace(local_name: &str, upstream_name: &str) -> String {
    upstream_name
        .strip_prefix(local_name)
        .unwrap_or(upstream_name)
        .trim_start_matches('/')
        .to_string()
}

impl RegistryClient {
    pub fn get_manifest_path(
        &self,
        local_name: &str,
        upstream_name: &str,
        reference: &Reference,
    ) -> String {
        let namespace = get_upstream_namespace(local_name, upstream_name);
        format!("{}/v2/{namespace}/manifests/{reference}", self.url)
    }

    pub fn get_blob_path(&self, local_name: &str, upstream_name: &str, digest: &Digest) -> String {
        let namespace = get_upstream_namespace(local_name, upstream_name);
        format!("{}/v2/{namespace}/blobs/{digest}", self.url)
    }

    /// URL to start a resumable blob upload session (OCI `POST /v2/<ns>/blobs/uploads/`).
    ///
    /// The session-continuation URLs (PATCH/PUT targets) are NOT synthesized here:
    /// they come back from the server in the `Location` response header.
    pub fn get_uploads_start_path(&self, local_name: &str, upstream_name: &str) -> String {
        let namespace = get_upstream_namespace(local_name, upstream_name);
        format!("{}/v2/{namespace}/blobs/uploads/", self.url)
    }

    /// URL to list a repository's tags (OCI `GET /v2/<ns>/tags/list`).
    ///
    /// Pagination query parameters (`?n=&last=`) are appended by the caller (or
    /// taken from the `Link` rel="next" header), not synthesized here.
    pub fn get_tags_list_path(&self, local_name: &str, upstream_name: &str) -> String {
        let namespace = get_upstream_namespace(local_name, upstream_name);
        format!("{}/v2/{namespace}/tags/list", self.url)
    }
}
