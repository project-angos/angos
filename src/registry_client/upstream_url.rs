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
    /// Resolve the request namespace: strip the pull-through `local_name` prefix,
    /// then apply the optional downstream mirror (replication fan-out). With no
    /// mirror configured this is just [`get_upstream_namespace`].
    fn request_namespace(&self, local_name: &str, upstream_name: &str) -> String {
        let namespace = get_upstream_namespace(local_name, upstream_name);
        match &self.mirror {
            None => namespace,
            Some(mirror) => {
                let bare = get_upstream_namespace(&mirror.source, &namespace);
                if bare.is_empty() {
                    mirror.target.clone()
                } else {
                    format!("{}/{bare}", mirror.target)
                }
            }
        }
    }

    pub fn get_manifest_path(
        &self,
        local_name: &str,
        upstream_name: &str,
        reference: &Reference,
    ) -> String {
        let namespace = self.request_namespace(local_name, upstream_name);
        format!("{}/v2/{namespace}/manifests/{reference}", self.url)
    }

    pub fn get_blob_path(&self, local_name: &str, upstream_name: &str, digest: &Digest) -> String {
        let namespace = self.request_namespace(local_name, upstream_name);
        format!("{}/v2/{namespace}/blobs/{digest}", self.url)
    }

    /// URL to start a resumable blob upload session (OCI `POST /v2/<ns>/blobs/uploads/`).
    ///
    /// Session-continuation URLs are server-assigned via `Location`, never built here.
    pub fn get_uploads_start_path(&self, local_name: &str, upstream_name: &str) -> String {
        let namespace = self.request_namespace(local_name, upstream_name);
        format!("{}/v2/{namespace}/blobs/uploads/", self.url)
    }

    /// URL to list a repository's tags (OCI `GET /v2/<ns>/tags/list`), without
    /// pagination parameters.
    pub fn get_tags_list_path(&self, local_name: &str, upstream_name: &str) -> String {
        let namespace = self.request_namespace(local_name, upstream_name);
        format!("{}/v2/{namespace}/tags/list", self.url)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{
        cache,
        oci::{Digest, Reference, Tag},
        registry::manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
        registry_client::{NO_LOCAL_PREFIX, RegistryClient},
    };

    fn client(url: &str) -> RegistryClient {
        let cache = cache::Config::Memory.to_backend().expect("memory cache");
        RegistryClient::builder(url.to_string(), reqwest::Client::new(), cache)
            .max_manifest_size_bytes(DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .build()
    }

    fn tag(name: &str) -> Reference {
        Reference::Tag(Tag::new(name).unwrap())
    }

    #[test]
    fn no_mirror_keeps_the_namespace_verbatim() {
        let c = client("http://downstream:8000");
        assert_eq!(
            c.get_manifest_path(NO_LOCAL_PREFIX, "push-through/kube-apiserver", &tag("v1")),
            "http://downstream:8000/v2/push-through/kube-apiserver/manifests/v1"
        );
    }

    #[test]
    fn mirror_replaces_the_source_prefix_with_the_target_across_path_kinds() {
        let c = client("http://downstream:8000")
            .with_namespace_mirror("push-through".to_string(), "push-through-1".to_string());
        let digest = Digest::from_str(
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();

        assert_eq!(
            c.get_manifest_path(NO_LOCAL_PREFIX, "push-through/kube-apiserver", &tag("v1")),
            "http://downstream:8000/v2/push-through-1/kube-apiserver/manifests/v1"
        );
        assert_eq!(
            c.get_blob_path(NO_LOCAL_PREFIX, "push-through/kube-apiserver", &digest),
            format!("http://downstream:8000/v2/push-through-1/kube-apiserver/blobs/{digest}")
        );
        assert_eq!(
            c.get_uploads_start_path(NO_LOCAL_PREFIX, "push-through/kube-apiserver"),
            "http://downstream:8000/v2/push-through-1/kube-apiserver/blobs/uploads/"
        );
        assert_eq!(
            c.get_tags_list_path(NO_LOCAL_PREFIX, "push-through/kube-apiserver"),
            "http://downstream:8000/v2/push-through-1/kube-apiserver/tags/list"
        );
    }

    #[test]
    fn mirror_preserves_deeper_sub_namespaces() {
        let c = client("http://downstream:8000")
            .with_namespace_mirror("push-through".to_string(), "push-through-2".to_string());
        assert_eq!(
            c.get_manifest_path(NO_LOCAL_PREFIX, "push-through/team/app", &tag("v1")),
            "http://downstream:8000/v2/push-through-2/team/app/manifests/v1"
        );
    }

    #[test]
    fn pull_through_strips_the_repo_name_then_prepends_the_url_prefix() {
        // The pull path passes the repository name as `local_name` (stripped),
        // and an upstream URL path is set as a prepend-only mirror, so
        // `kubernetes.io/kube-apiserver` is fetched as `k8s-images/kube-apiserver`.
        let c = client("https://upstream")
            .with_namespace_mirror(String::new(), "k8s-images".to_string());
        assert_eq!(
            c.get_manifest_path("kubernetes.io", "kubernetes.io/kube-apiserver", &tag("v1")),
            "https://upstream/v2/k8s-images/kube-apiserver/manifests/v1"
        );
    }
}
