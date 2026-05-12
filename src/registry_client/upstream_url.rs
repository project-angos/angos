//! Namespace resolution and URL builders for upstream registry requests.

use crate::{
    oci::{Digest, Reference},
    registry_client::RegistryClient,
};

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
}
