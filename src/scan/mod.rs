//! Vulnerability scanning. A push of an image manifest a repository's scan
//! policy applies to enqueues one [`SCAN_IMAGE_KIND`] job; [`ScanJobHandler`] asks
//! the external scanner service for a SARIF report and pushes it back as a
//! referrer of the image through the registry's own write path, so the report
//! is linked, announced and replicated like any client push, and
//! `angos reconcile scan` scans again the images the scan policy finds due.

use std::cmp::Reverse;

use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};

use angos_oci::{Descriptor, Digest, Namespace};

use crate::{
    jobs::{
        Queue,
        store::{Error, JobEnvelope},
    },
    policy::PolicyConfig,
    registry::{
        Error as RegistryError,
        metadata_store::{LinkKind, MetadataStore},
    },
};
use angos_secret::Secret;

pub const SCAN_IMAGE_KIND: &str = "scan.image";
pub const SARIF_MEDIA_TYPE: &str = "application/sarif+json";
const EMPTY_CONFIG_BODY: &[u8] = b"{}";
/// Internal-process name stamped on the events a report push emits.
const SCAN_ACTOR: &str = "scan";
/// Prefix of the annotations a report angos wrote carries.
const SCAN_ANNOTATION_PREFIX: &str = "io.angos.scan.";
const CREATED_ANNOTATION: &str = "org.opencontainers.image.created";

fn default_timeout_secs() -> u64 {
    600
}

/// The `[global.scan]` section: the scanner service every scanning
/// repository's pushes are sent to, and the scan policy every repository
/// without a `scan` table of its own follows.
#[derive(Clone, Debug, Deserialize)]
pub struct ScanConfig {
    /// Base URL of the scanner service; the handler posts to its `/scan`.
    pub url: String,
    /// Bearer token the scanner service expects, when it checks one.
    pub token: Option<Secret<String>>,
    /// Bound on one scan request, pull and analysis included.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    /// `default` and `rules`, beside the service settings.
    #[serde(flatten)]
    pub policy: PolicyConfig<ScanAction>,
}

/// What a `scan` table's `default` gives an image no rule matches.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScanAction {
    Scan,
    Skip,
}

impl From<ScanAction> for bool {
    fn from(action: ScanAction) -> bool {
        matches!(action, ScanAction::Scan)
    }
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
    /// When `reconcile scan` judged the image: a report created after this
    /// makes the scan redundant, so a push between the run and the job's
    /// execution scans once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported_before: Option<DateTime<Utc>>,
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

/// Fan-out for the referrer record reads listing an image's reports.
const REFERRER_READ_CONCURRENCY: usize = 16;

/// A report angos attached to an image: the SARIF referrer and the time its
/// `created` annotation states.
#[derive(Debug)]
pub struct ScanReport {
    pub digest: Digest,
    pub created: Option<DateTime<Utc>>,
}

/// The reports angos attached to `digest`, newest first. A SARIF referrer
/// without an `io.angos.scan.*` annotation was written by someone else and
/// is not one; every report but the first is superseded, and retention
/// judges it like any untagged manifest.
pub async fn scan_reports(
    metadata_store: &MetadataStore,
    namespace: &Namespace,
    digest: &Digest,
) -> Result<Vec<ScanReport>, RegistryError> {
    let mut reports: Vec<ScanReport> = metadata_store
        .stream_referrer_digests(namespace, digest)
        .map_ok(|referrer| async move {
            let link = LinkKind::Referrer {
                subject: digest.clone(),
                referrer: referrer.clone(),
            };
            let descriptor = match metadata_store.read_link(namespace, &link).await {
                Ok(metadata) => metadata.descriptor,
                Err(RegistryError::NotFound) => None,
                Err(e) => return Err(e),
            };
            Ok(descriptor
                .filter(is_angos_report)
                .map(|descriptor| ScanReport {
                    digest: referrer,
                    created: descriptor
                        .annotations
                        .get(CREATED_ANNOTATION)
                        .and_then(|created| DateTime::parse_from_rfc3339(created).ok())
                        .map(|created| created.with_timezone(&Utc)),
                }))
        })
        .try_buffered(REFERRER_READ_CONCURRENCY)
        .try_filter_map(|report| async move { Ok(report) })
        .try_collect()
        .await?;
    reports.sort_by_key(|report| Reverse(report.created));
    Ok(reports)
}

/// Whether a referrer's descriptor is a report angos wrote.
pub fn is_angos_report(descriptor: &Descriptor) -> bool {
    descriptor
        .artifact_type
        .as_ref()
        .is_some_and(|artifact_type| artifact_type.as_ref() == SARIF_MEDIA_TYPE)
        && descriptor
            .annotations
            .keys()
            .any(|key| key.starts_with(SCAN_ANNOTATION_PREFIX))
}

/// Whether a report makes a scan redundant: any report when no cutoff was
/// set, else one created after the cutoff, since the run already judged
/// the ones before it stale.
fn reported_since(reports: &[ScanReport], cutoff: Option<DateTime<Utc>>) -> bool {
    match cutoff {
        None => !reports.is_empty(),
        Some(cutoff) => reports
            .iter()
            .any(|report| report.created.is_some_and(|created| created > cutoff)),
    }
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

pub mod handler;
#[cfg(test)]
mod tests;
