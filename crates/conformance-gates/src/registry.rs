use std::env;

use reqwest::header::{ACCEPT, CONTENT_TYPE, LOCATION};
use reqwest::{Client, Response, StatusCode};
use serde_json::Value;

use crate::error::{GateResult, ensure};
use crate::store::sha256_hex;

/// Every manifest media type the audit accepts, matching what a real client
/// (and the bash harness before it) negotiates.
const ACCEPT_MANIFESTS: &str = "application/vnd.oci.image.index.v1+json,\
application/vnd.oci.image.manifest.v1+json,\
application/vnd.docker.distribution.manifest.v2+json,\
application/vnd.docker.distribution.manifest.list.v2+json";
const OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
const CATALOG_PAGE: u16 = 1000;

/// An image pushed through the registry API; digests are hex without the
/// `sha256:` prefix, the form the seeded store keys use.
#[allow(clippy::struct_field_names)]
pub struct PushedImage {
    pub layer_digest: String,
    pub config_digest: String,
    pub manifest_digest: String,
}

/// OCI distribution API client for the registry under test.
pub struct RegistryClient {
    http: Client,
    base: String,
}

impl RegistryClient {
    /// Construct against `REGISTRY_URL` (default `http://127.0.0.1:8000`).
    pub fn from_env() -> Self {
        let base = env::var("REGISTRY_URL").unwrap_or_else(|_| "http://127.0.0.1:8000".to_string());
        Self {
            http: Client::new(),
            base,
        }
    }

    /// Start a blob upload session and return its status path, query
    /// stripped. Used for the decoy in-flight upload that gates must prove
    /// scrub and prune leave alone.
    pub async fn start_upload(&self, namespace: &str) -> GateResult<String> {
        let location = self.upload_location(namespace).await?;
        let path = location.split('?').next().unwrap_or(&location);
        Ok(path.to_string())
    }

    /// Upload `content` as a blob via a monolithic POST + PUT, returning its
    /// hex digest.
    pub async fn upload_blob(&self, namespace: &str, content: &[u8]) -> GateResult<String> {
        let location = self.upload_location(namespace).await?;
        let digest = sha256_hex(content);
        let separator = if location.contains('?') { '&' } else { '?' };
        let url = format!(
            "{}{location}{separator}digest=sha256:{digest}",
            self.base_for(&location)
        );
        let response = self
            .http
            .put(url)
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(content.to_vec())
            .send()
            .await?;
        ensure(response.status().is_success(), || {
            format!("blob upload to {namespace} returned {}", response.status())
        })?;
        Ok(digest)
    }

    /// Push a one-layer OCI image (`{}` config) and tag it, returning every
    /// digest a gate later probes.
    pub async fn push_image(
        &self,
        namespace: &str,
        tag: &str,
        layer: &[u8],
    ) -> GateResult<PushedImage> {
        let config: &[u8] = b"{}";
        let layer_digest = self.upload_blob(namespace, layer).await?;
        let config_digest = self.upload_blob(namespace, config).await?;
        let manifest = format!(
            r#"{{"schemaVersion":2,"mediaType":"{OCI_MANIFEST}","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:{config_digest}","size":{}}},"layers":[{{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":"sha256:{layer_digest}","size":{}}}]}}"#,
            config.len(),
            layer.len()
        );
        let response = self
            .http
            .put(format!("{}/v2/{namespace}/manifests/{tag}", self.base))
            .header(CONTENT_TYPE, OCI_MANIFEST)
            .body(manifest.clone())
            .send()
            .await?;
        ensure(response.status().is_success(), || {
            format!(
                "manifest push {namespace}:{tag} returned {}",
                response.status()
            )
        })?;
        Ok(PushedImage {
            layer_digest,
            config_digest,
            manifest_digest: sha256_hex(manifest.as_bytes()),
        })
    }

    /// Status code of a GET on an upload session's status path (204 while the
    /// session is alive).
    pub async fn upload_status(&self, path: &str) -> GateResult<StatusCode> {
        let response = self.http.get(format!("{}{path}", self.base)).send().await?;
        Ok(response.status())
    }

    /// All repository names, first catalog page (the gates' stores stay well
    /// under one page).
    pub async fn catalog(&self) -> GateResult<Vec<String>> {
        let url = format!("{}/v2/_catalog?n={CATALOG_PAGE}", self.base);
        let body: Value = self.get_json(&url, "catalog").await?;
        Ok(string_array(&body["repositories"]))
    }

    /// All tags of a repository; a repository without tags yields empty.
    pub async fn tags(&self, repo: &str) -> GateResult<Vec<String>> {
        let url = format!("{}/v2/{repo}/tags/list?n={CATALOG_PAGE}", self.base);
        let body: Value = self.get_json(&url, "tags list").await?;
        Ok(string_array(&body["tags"]))
    }

    /// GET a manifest by tag or digest: its raw bytes plus the
    /// `Docker-Content-Digest` header the server declared for it.
    pub async fn manifest(&self, repo: &str, reference: &str) -> GateResult<(Vec<u8>, String)> {
        let response = self
            .http
            .get(format!("{}/v2/{repo}/manifests/{reference}", self.base))
            .header(ACCEPT, ACCEPT_MANIFESTS)
            .send()
            .await?;
        ensure(response.status().is_success(), || {
            format!(
                "manifest {repo}:{reference} not served (status {})",
                response.status()
            )
        })?;
        let served = header_value(&response, "docker-content-digest");
        let body = response.bytes().await?;
        Ok((body.to_vec(), served))
    }

    /// HEAD a manifest, returning its status and `Content-Type` (empty when
    /// absent). go-containerregistry (kaniko, crane) rejects a HEAD response
    /// without a Content-Type, so a served manifest must always carry one.
    pub async fn head_manifest(
        &self,
        repo: &str,
        reference: &str,
    ) -> GateResult<(StatusCode, String)> {
        let response = self
            .http
            .head(format!("{}/v2/{repo}/manifests/{reference}", self.base))
            .header(ACCEPT, ACCEPT_MANIFESTS)
            .send()
            .await?;
        let status = response.status();
        let content_type = header_value(&response, CONTENT_TYPE.as_str());
        Ok((status, content_type))
    }

    /// GET a blob by digest, following redirects, returning status and bytes.
    pub async fn blob(&self, repo: &str, digest: &str) -> GateResult<(StatusCode, Vec<u8>)> {
        let response = self
            .http
            .get(format!("{}/v2/{repo}/blobs/{digest}", self.base))
            .send()
            .await?;
        let status = response.status();
        let body = response.bytes().await?;
        Ok((status, body.to_vec()))
    }

    /// Re-hash every manifest and blob in the catalog against its declared
    /// digest; at least `floor` manifests must be audited or the store lost
    /// content.
    pub async fn audit_digests(&self, floor: usize) -> GateResult<()> {
        let mut audited = 0;
        for repo in self.catalog().await? {
            for tag in self.tags(&repo).await? {
                let (body, served) = self.manifest(&repo, &tag).await?;
                let digest = format!("sha256:{}", sha256_hex(&body));
                ensure(served == digest, || {
                    format!("manifest {repo}:{tag} digest header {served} != body {digest}")
                })?;
                for blob_digest in manifest_blob_digests(&body)? {
                    let (status, bytes) = self.blob(&repo, &blob_digest).await?;
                    ensure(status == StatusCode::OK, || {
                        format!("blob {repo}@{blob_digest} returned {status}")
                    })?;
                    let actual = format!("sha256:{}", sha256_hex(&bytes));
                    ensure(actual == blob_digest, || {
                        format!("blob {repo}@{blob_digest} content does not match its digest")
                    })?;
                }
                audited += 1;
            }
        }
        println!("audited {audited} manifests with full content verification");
        ensure(audited >= floor, || {
            format!("only {audited} manifests audited (floor {floor})")
        })
    }

    async fn upload_location(&self, namespace: &str) -> GateResult<String> {
        let response = self
            .http
            .post(format!("{}/v2/{namespace}/blobs/uploads/", self.base))
            .send()
            .await?;
        ensure(response.status().is_success(), || {
            format!(
                "upload start for {namespace} returned {}",
                response.status()
            )
        })?;
        let location = header_value(&response, LOCATION.as_str());
        ensure(!location.is_empty(), || {
            format!("upload start for {namespace} returned no Location header")
        })?;
        Ok(location)
    }

    async fn get_json(&self, url: &str, what: &str) -> GateResult<Value> {
        let response = self.http.get(url).send().await?;
        ensure(response.status().is_success(), || {
            format!("{what} GET returned {}", response.status())
        })?;
        Ok(response.json().await?)
    }

    /// An absolute `Location` is used verbatim; a path-relative one is
    /// resolved against the registry base.
    fn base_for(&self, location: &str) -> &str {
        if location.starts_with("http") {
            ""
        } else {
            &self.base
        }
    }
}

/// The config and layer digests a manifest declares; an index manifest
/// (no config/layers) contributes none, exactly like the bash audit.
fn manifest_blob_digests(body: &[u8]) -> GateResult<Vec<String>> {
    let parsed: Value = serde_json::from_slice(body)?;
    let mut digests = Vec::new();
    if let Some(digest) = parsed["config"]["digest"].as_str() {
        digests.push(digest.to_string());
    }
    if let Some(layers) = parsed["layers"].as_array() {
        for layer in layers {
            if let Some(digest) = layer["digest"].as_str() {
                digests.push(digest.to_string());
            }
        }
    }
    Ok(digests)
}

fn string_array(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn header_value(response: &Response, name: &str) -> String {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string()
}
