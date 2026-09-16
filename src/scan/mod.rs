//! Vulnerability scanning. A push of an image manifest into a repository with
//! `scan = true` enqueues one [`SCAN_IMAGE_KIND`] job; [`ScanJobHandler`] asks
//! the external scanner service for a SARIF report and pushes it back as a
//! referrer of the image through the registry's own write path, so the report
//! is linked, announced and replicated like any client push.

use std::{collections::HashMap, io::Cursor, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use futures_util::TryStreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use angos_oci::request::{PutManifestRequest, StartUploadRequest, StartUploadTarget};
use angos_oci::{Content, Descriptor, Digest, Manifest, MediaType, Namespace, Reference};

use crate::{
    event_webhook::event::EventActor,
    jobs::{
        Queue,
        store::{Error, JobEnvelope, JobHandler},
    },
    registry::{
        Error as RegistryError, Registry,
        blob_store::BlobStore,
        manifest::read_manifest,
        metadata_store::{LinkKind, MetadataStore},
    },
};
use angos_secret::Secret;

pub const SCAN_IMAGE_KIND: &str = "scan.image";
pub const SARIF_MEDIA_TYPE: &str = "application/sarif+json";
const EMPTY_CONFIG_MEDIA_TYPE: &str = "application/vnd.oci.empty.v1+json";
const EMPTY_CONFIG_BODY: &[u8] = b"{}";
/// Internal-process name stamped on the events a report push emits.
const SCAN_ACTOR: &str = "scan";

fn default_timeout_secs() -> u64 {
    600
}

/// The `[global.scan]` section: the scanner service every `scan = true`
/// repository's pushes are sent to.
#[derive(Clone, Debug, Deserialize)]
pub struct ScanConfig {
    /// Base URL of the scanner service; the handler posts to its `/scan`.
    pub url: String,
    /// Bearer token the scanner service expects, when it checks one.
    pub token: Option<Secret<String>>,
    /// Bound on one scan request, pull and analysis included.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

/// JSON payload of a [`SCAN_IMAGE_KIND`] job, and the body posted to the
/// scanner service, which pulls the image itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanImagePayload {
    pub namespace: Namespace,
    pub digest: Digest,
    /// Scan again even when a report already hangs off the image.
    #[serde(default)]
    pub force: bool,
}

/// A scan job keyed on `scan.{namespace}:{digest}`, so pending scans of one
/// image coalesce however many pushes name it.
pub fn build_envelope(payload: &ScanImagePayload) -> Result<JobEnvelope, Error> {
    JobEnvelope::new(
        Queue::Scan,
        SCAN_IMAGE_KIND,
        format!("{}.{}:{}", Queue::Scan, payload.namespace, payload.digest),
        payload,
    )
}

/// A report, an attestation and an index are pushed like any manifest; only a
/// plain image manifest is a scan subject.
pub fn is_scan_subject(manifest: &Manifest) -> bool {
    manifest.subject.is_none()
        && manifest.artifact_type.is_none()
        && matches!(manifest.content, Content::Image { .. })
}

/// Whether a SARIF referrer already hangs off `digest`, which makes a re-run
/// of a scan a no-op rather than a second report.
pub async fn already_reported(
    metadata_store: &MetadataStore,
    namespace: &Namespace,
    digest: &Digest,
) -> Result<bool, RegistryError> {
    let referrers: Vec<Digest> = metadata_store
        .stream_referrer_digests(namespace, digest)
        .try_collect()
        .await?;
    for referrer in referrers {
        let link = LinkKind::Referrer {
            subject: digest.clone(),
            referrer,
        };
        let reported = match metadata_store.read_link(namespace, &link).await {
            Ok(metadata) => metadata
                .descriptor
                .and_then(|descriptor| descriptor.artifact_type)
                .is_some_and(|artifact_type| artifact_type.as_ref() == SARIF_MEDIA_TYPE),
            Err(RegistryError::NotFound) => false,
            Err(e) => return Err(e),
        };
        if reported {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Severity counts of a SARIF report, written as annotations of the report
/// manifest so the web UI can show them without reading the report.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ScanSummary {
    pub scanner: Option<String>,
    /// Indexed by `Severity as usize`.
    pub counts: [usize; 5],
}

impl ScanSummary {
    /// Reads `report`, a SARIF document; anything else summarises as empty
    /// rather than failing the job, since the report is still worth keeping.
    pub fn of(report: &[u8]) -> Self {
        let Ok(document) = serde_json::from_slice::<serde_json::Value>(report) else {
            return Self::default();
        };
        let Some(run) = document["runs"].get(0) else {
            return Self::default();
        };
        let driver = &run["tool"]["driver"];
        let scanner = driver["name"]
            .as_str()
            .map(|name| match driver["version"].as_str() {
                Some(version) => format!("{name} {version}"),
                None => name.to_string(),
            });
        let empty = Vec::new();
        let rules = driver["rules"].as_array().unwrap_or(&empty);
        let mut summary = Self {
            scanner,
            ..Self::default()
        };
        for result in run["results"].as_array().unwrap_or(&empty) {
            let rule = result["ruleIndex"]
                .as_u64()
                .and_then(|index| rules.get(usize::try_from(index).ok()?))
                .or_else(|| {
                    let id = result["ruleId"].as_str()?;
                    rules.iter().find(|rule| rule["id"].as_str() == Some(id))
                });
            let message = result["message"]["text"].as_str().unwrap_or_default();
            summary.counts[severity(rule, message) as usize] += 1;
        }
        summary
    }

    /// The `io.angos.scan.*` annotations carrying this summary.
    pub fn annotations(&self) -> Vec<(String, String)> {
        Severity::ALL
            .iter()
            .map(|severity| {
                (
                    format!("io.angos.scan.{}", severity.as_str()),
                    self.counts[*severity as usize].to_string(),
                )
            })
            .chain(
                self.scanner
                    .iter()
                    .map(|scanner| ("io.angos.scan.scanner".to_string(), scanner.clone())),
            )
            .collect()
    }
}

#[derive(Clone, Copy)]
enum Severity {
    Critical,
    High,
    Medium,
    Low,
    Unknown,
}

impl Severity {
    const ALL: [Severity; 5] = [
        Severity::Critical,
        Severity::High,
        Severity::Medium,
        Severity::Low,
        Severity::Unknown,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Severity::Critical => "critical",
            Severity::High => "high",
            Severity::Medium => "medium",
            Severity::Low => "low",
            Severity::Unknown => "unknown",
        }
    }
}

fn severity_word(word: &str) -> Option<Severity> {
    match word.to_ascii_lowercase().as_str() {
        "critical" => Some(Severity::Critical),
        "high" => Some(Severity::High),
        "medium" | "moderate" => Some(Severity::Medium),
        "low" | "negligible" => Some(Severity::Low),
        _ => None,
    }
}

/// The scanner's own severity word when it states one (Trivy tags its rule
/// with it, Grype writes it into the message), else the CVSS score bucketed
/// the way GitHub code scanning does.
fn severity(rule: Option<&serde_json::Value>, message: &str) -> Severity {
    let tagged = rule
        .and_then(|rule| rule["properties"]["tags"].as_array())
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .find_map(severity_word);
    if let Some(severity) = tagged {
        return severity;
    }
    let stated = message
        .split_whitespace()
        .collect::<Vec<_>>()
        .windows(2)
        .find_map(|pair| match pair {
            ["Severity:" | "A" | "An", word] => severity_word(word),
            _ => None,
        });
    if let Some(severity) = stated {
        return severity;
    }
    let score = rule
        .and_then(|rule| rule["properties"]["security-severity"].as_str())
        .and_then(|score| score.parse::<f64>().ok());
    match score {
        Some(score) if score >= 9.0 => Severity::Critical,
        Some(score) if score >= 7.0 => Severity::High,
        Some(score) if score >= 4.0 => Severity::Medium,
        Some(score) if score > 0.0 => Severity::Low,
        _ => Severity::Unknown,
    }
}

pub struct ScanJobHandler {
    registry: Arc<Registry>,
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    client: Client,
    url: String,
    token: Option<Secret<String>>,
}

fn job_error(error: &RegistryError) -> Error {
    Error::Execution(error.to_string())
}

impl ScanJobHandler {
    pub fn new(
        registry: Arc<Registry>,
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
        config: &ScanConfig,
    ) -> Result<Self, Error> {
        let client = Client::builder()
            .use_rustls_tls()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| Error::Initialization(format!("scanner client: {e}")))?;
        Ok(Self {
            registry,
            blob_store,
            metadata_store,
            client,
            url: config.url.trim_end_matches('/').to_string(),
            token: config.token.clone(),
        })
    }

    async fn scan(&self, namespace: &Namespace, digest: &Digest, force: bool) -> Result<(), Error> {
        // Gone before the job ran, or not an image after all: nothing to do,
        // and a retry would find the same.
        let Some(manifest) = read_manifest(&self.blob_store, digest)
            .await
            .map_err(|e| job_error(&e))?
        else {
            debug!("Scan of {namespace}@{digest} skipped: the manifest is gone");
            return Ok(());
        };
        if !is_scan_subject(&manifest) {
            return Ok(());
        }
        if !force
            && already_reported(&self.metadata_store, namespace, digest)
                .await
                .map_err(|e| job_error(&e))?
        {
            debug!("Scan of {namespace}@{digest} skipped: already reported");
            return Ok(());
        }
        let size = self
            .blob_store
            .size(digest)
            .await
            .map_err(|e| job_error(&e))?;
        let subject = Descriptor {
            media_type: manifest.media_type.unwrap_or_else(MediaType::oci_manifest),
            digest: digest.clone(),
            size,
            annotations: HashMap::default(),
            artifact_type: None,
            platform: None,
        };

        let report = self.request_report(namespace, digest).await?;
        self.attach_report(namespace, subject, &report).await?;
        info!("Attached a scan report to {namespace}@{digest}");
        Ok(())
    }

    async fn request_report(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<Vec<u8>, Error> {
        let mut request = self
            .client
            .post(format!("{}/scan", self.url))
            .json(&serde_json::json!({ "namespace": namespace, "digest": digest }));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token.expose());
        }
        let response = request
            .send()
            .await
            .map_err(|e| Error::Execution(format!("scanner service: {e}")))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|e| Error::Execution(format!("scanner service body: {e}")))?;
        if !status.is_success() {
            let detail = String::from_utf8_lossy(&body);
            return Err(Error::Execution(format!(
                "scanner service answered {status}: {}",
                detail.trim()
            )));
        }
        if body.is_empty() {
            return Err(Error::Execution(
                "scanner service returned an empty report".to_string(),
            ));
        }
        Ok(body.to_vec())
    }

    /// Pushes the empty config, the SARIF layer, then the artifact manifest
    /// naming the image as its subject, all through the registry's client
    /// write path.
    async fn attach_report(
        &self,
        namespace: &Namespace,
        subject: Descriptor,
        report: &[u8],
    ) -> Result<(), Error> {
        let config_digest = Digest::sha256_of_bytes(EMPTY_CONFIG_BODY);
        self.push_blob(namespace, &config_digest, EMPTY_CONFIG_BODY.to_vec())
            .await?;
        let layer_digest = Digest::sha256_of_bytes(report);
        self.push_blob(namespace, &layer_digest, report.to_vec())
            .await?;

        let sarif =
            MediaType::new(SARIF_MEDIA_TYPE).map_err(|e| Error::Execution(e.to_string()))?;
        let manifest = Manifest {
            schema_version: 2,
            media_type: Some(MediaType::oci_manifest()),
            subject: Some(subject),
            annotations: ScanSummary::of(report)
                .annotations()
                .into_iter()
                .chain([(
                    "org.opencontainers.image.created".to_string(),
                    Utc::now().to_rfc3339(),
                )])
                .collect(),
            artifact_type: Some(sarif.clone()),
            content: Content::Image {
                config: Some(Box::new(Descriptor {
                    media_type: MediaType::new(EMPTY_CONFIG_MEDIA_TYPE)
                        .map_err(|e| Error::Execution(e.to_string()))?,
                    digest: config_digest,
                    size: EMPTY_CONFIG_BODY.len() as u64,
                    annotations: HashMap::default(),
                    artifact_type: None,
                    platform: None,
                })),
                layers: vec![Descriptor {
                    media_type: sarif,
                    digest: layer_digest,
                    size: report.len() as u64,
                    annotations: HashMap::default(),
                    artifact_type: None,
                    platform: None,
                }],
            },
        };
        let body = serde_json::to_vec(&manifest)
            .map_err(|e| Error::Execution(format!("report manifest: {e}")))?;
        let manifest_digest = Digest::sha256_of_bytes(&body);
        self.registry
            .accept_put_manifest(
                Some(EventActor::internal(SCAN_ACTOR)),
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Digest(manifest_digest),
                    content_type: Some(MediaType::oci_manifest()),
                    tags: Vec::new(),
                    source_ts: None,
                },
                Cursor::new(body),
            )
            .await
            .map_err(|e| job_error(&e))?;
        Ok(())
    }

    async fn push_blob(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        bytes: Vec<u8>,
    ) -> Result<(), Error> {
        let length = bytes.len() as u64;
        self.registry
            .start_upload(
                Some(EventActor::internal(SCAN_ACTOR)),
                StartUploadRequest {
                    namespace: namespace.clone(),
                    digest_algorithm: None,
                    target: Some(StartUploadTarget {
                        digest: digest.clone(),
                        content_length: Some(length),
                    }),
                },
                Cursor::new(bytes),
            )
            .await
            .map_err(|e| job_error(&e))?;
        Ok(())
    }
}

#[async_trait]
impl JobHandler for ScanJobHandler {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<(), Error> {
        if envelope.kind != SCAN_IMAGE_KIND {
            return Err(Error::Execution(format!(
                "unsupported job kind '{}'; expected '{SCAN_IMAGE_KIND}'",
                envelope.kind,
            )));
        }
        let payload: ScanImagePayload = serde_json::from_value(envelope.payload.clone())
            .map_err(|e| Error::Execution(format!("failed to deserialize job payload: {e}")))?;
        self.scan(&payload.namespace, &payload.digest, payload.force)
            .await
    }
}

#[cfg(test)]
mod tests;
