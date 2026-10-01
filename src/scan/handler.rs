//! The scan job handler: asks the scanner service for an image's report and
//! attaches it as a SARIF referrer.

use std::{collections::HashMap, io::Cursor, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use reqwest::Client;
use tracing::{debug, info};

use angos_oci::request::{PutManifestRequest, StartUploadRequest, StartUploadTarget};
use angos_oci::{Content, Descriptor, Digest, Manifest, MediaType, Namespace, Reference};
use angos_secret::Secret;

use crate::{
    event_webhook::event::EventActor,
    jobs::store::{Error, JobEnvelope, JobHandler},
    registry::{
        Registry, blob_store::BlobStore, manifest::read_manifest, metadata_store::MetadataStore,
    },
    scan::{
        CREATED_ANNOTATION, EMPTY_CONFIG_BODY, SARIF_MEDIA_TYPE, SCAN_ACTOR, SCAN_IMAGE_KIND,
        ScanConfig, ScanImagePayload, ScanSummary, reported_since, scan_reports,
    },
};

pub struct ScanJobHandler {
    registry: Arc<Registry>,
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    client: Client,
    url: String,
    token: Option<Secret<String>>,
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

    async fn scan(&self, payload: &ScanImagePayload) -> Result<(), Error> {
        let ScanImagePayload {
            namespace,
            digest,
            force,
            reported_before,
        } = payload;
        // Gone before the job ran, or not an image after all: nothing to do,
        // and a retry would find the same.
        let Some(manifest) = read_manifest(&self.blob_store, digest).await? else {
            debug!("Scan of {namespace}@{digest} skipped: the manifest is gone");
            return Ok(());
        };
        if !manifest.is_plain_image() {
            return Ok(());
        }
        let reports = scan_reports(&self.metadata_store, namespace, digest).await?;
        if !force && reported_since(&reports, *reported_before) {
            debug!("Scan of {namespace}@{digest} skipped: already reported");
            return Ok(());
        }
        let size = self.blob_store.size(digest).await?;
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
                .chain([(CREATED_ANNOTATION.to_string(), Utc::now().to_rfc3339())])
                .collect(),
            artifact_type: Some(sarif.clone()),
            content: Content::Image {
                config: Some(Box::new(Descriptor {
                    media_type: MediaType::oci_empty(),
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
            .handle_put_manifest(
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
            .await?;
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
            .handle_start_upload(
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
            .await?;
        Ok(())
    }
}

#[async_trait]
impl JobHandler for ScanJobHandler {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<(), Error> {
        let payload: ScanImagePayload = envelope.payload(&[SCAN_IMAGE_KIND])?;
        self.scan(&payload).await
    }
}
