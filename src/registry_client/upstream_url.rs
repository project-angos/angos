//! Namespace resolution and URL builders for upstream registry requests.

use crate::{
    oci::{Digest, Reference},
    registry_client::RegistryClient,
};

/// Sentinel `local_name` that makes [`get_upstream_namespace`] the identity
/// transform, used by replication where the downstream namespace matches local.
pub const NO_LOCAL_PREFIX: &str = "";

/// Resolves the upstream namespace by stripping the `local_name` pull-mirror
/// prefix from `upstream_name` (unchanged when no prefix matches).
///
/// Pass [`NO_LOCAL_PREFIX`] to disable the rewrite for replication.
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
    /// Session-continuation URLs are server-assigned via `Location`, never built here.
    pub fn get_uploads_start_path(&self, local_name: &str, upstream_name: &str) -> String {
        let namespace = get_upstream_namespace(local_name, upstream_name);
        format!("{}/v2/{namespace}/blobs/uploads/", self.url)
    }

    /// URL to list a repository's tags (OCI `GET /v2/<ns>/tags/list`), without
    /// pagination parameters.
    pub fn get_tags_list_path(&self, local_name: &str, upstream_name: &str) -> String {
        let namespace = get_upstream_namespace(local_name, upstream_name);
        format!("{}/v2/{namespace}/tags/list", self.url)
    }
}
