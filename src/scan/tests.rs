use std::sync::Arc;

use chrono::{TimeDelta, Utc};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

use angos_oci::{Digest, Manifest, Namespace, Tag};

use crate::{
    jobs::{
        Queue,
        store::{ClaimMode, JobHandler, JobStore},
    },
    policy::{CelRule, ImagePolicy, PolicyConfig},
    registry::{
        Registry, RegistryConfig, Repository,
        manifest::read_manifest,
        test_utils::{
            FsTestStack, fs_test_stack, repository_with_replication, seed_manifest,
            single_repo_resolver,
        },
    },
    scan::{
        SARIF_MEDIA_TYPE, ScanAction, ScanConfig, ScanImagePayload, ScanJobHandler, ScanSummary,
        build_envelope, is_scan_subject, scan_reports,
    },
};
use angos_secret::Secret;

const SARIF: &[u8] = br#"{"version":"2.1.0","runs":[]}"#;

#[test]
fn scan_jobs_coalesce_on_the_image_digest() {
    let namespace = Namespace::new("apps/web").unwrap();
    let digest = Digest::sha256_of_bytes(b"image");
    let envelope = build_envelope(&ScanImagePayload {
        namespace: namespace.clone(),
        digest: digest.clone(),
        force: false,
        reported_before: None,
    })
    .unwrap();
    assert_eq!(envelope.queue, Queue::Scan);
    assert_eq!(
        envelope.lock_key.as_str(),
        format!("scan.{namespace}:{digest}")
    );
}

#[test]
fn only_a_plain_image_manifest_is_a_scan_subject() {
    let image = r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0},"layers":[]}"#;
    let report = r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","artifactType":"application/sarif+json","config":{"mediaType":"application/vnd.oci.empty.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":2},"layers":[],"subject":{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0}}"#;
    let index = r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}"#;
    // A buildx provenance attestation: an image manifest with no subject,
    // whose one layer is an in-toto statement.
    let attestation = r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":167},"layers":[{"mediaType":"application/vnd.in-toto+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":34184,"annotations":{"in-toto.io/predicate-type":"https://slsa.dev/provenance/v0.2"}}]}"#;
    assert!(is_scan_subject(
        &Manifest::from_slice(image.as_bytes()).unwrap()
    ));
    assert!(!is_scan_subject(
        &Manifest::from_slice(report.as_bytes()).unwrap()
    ));
    assert!(!is_scan_subject(
        &Manifest::from_slice(index.as_bytes()).unwrap()
    ));
    assert!(!is_scan_subject(
        &Manifest::from_slice(attestation.as_bytes()).unwrap()
    ));
}

/// The handler asks the scanner service for the report, pushes it as a SARIF
/// referrer of the image through the registry, and a re-run finds the report
/// already there rather than asking again.
#[tokio::test]
async fn a_scan_job_attaches_one_report_and_reruns_as_a_no_op() {
    let stack = fs_test_stack();
    let namespace = Namespace::new("apps/web").unwrap();
    let (image, _config, _layer) =
        seed_manifest(&stack.store, &stack.metadata_store, &namespace).await;

    let scanner = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/scan"))
        .and(header("authorization", "Bearer s3cret"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(SARIF))
        .expect(1)
        .mount(&scanner)
        .await;

    let handler = handler_for(
        &stack,
        repository_with_replication("apps", Vec::new()),
        &scanner,
        Some(Secret::new("s3cret".to_string())),
    );

    let envelope = build_envelope(&ScanImagePayload {
        namespace: namespace.clone(),
        digest: image.clone(),
        force: false,
        reported_before: None,
    })
    .unwrap();
    handler.execute(&envelope).await.unwrap();

    let reports = scan_reports(&stack.metadata_store, &namespace, &image)
        .await
        .unwrap();
    assert_eq!(
        reports.len(),
        1,
        "one SARIF referrer must hang off the image"
    );
    let report = read_manifest(&stack.blob_store, &reports[0].digest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.subject.map(|s| s.digest), Some(image.clone()));
    assert_eq!(
        report.artifact_type.map(|t| t.as_ref().to_string()),
        Some(SARIF_MEDIA_TYPE.to_string())
    );

    handler.execute(&envelope).await.unwrap();
    assert_eq!(
        scan_reports(&stack.metadata_store, &namespace, &image)
            .await
            .unwrap()
            .len(),
        1,
        "a re-run must not attach a second report"
    );
    // `expect(1)` on the mock fails the test at drop if the re-run scanned again.
}

/// A scanner failure is a retryable job failure, not a silent skip.
#[tokio::test]
async fn a_failing_scanner_fails_the_job() {
    let stack = fs_test_stack();
    let namespace = Namespace::new("apps/web").unwrap();
    let (image, _, _) = seed_manifest(&stack.store, &stack.metadata_store, &namespace).await;
    let scanner = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(502).set_body_string("grype exited with 1"))
        .mount(&scanner)
        .await;
    let handler = handler_for(
        &stack,
        repository_with_replication("apps", Vec::new()),
        &scanner,
        None,
    );
    let err = handler
        .execute(
            &build_envelope(&ScanImagePayload {
                namespace: namespace.clone(),
                digest: image.clone(),
                force: false,
                reported_before: None,
            })
            .unwrap(),
        )
        .await
        .expect_err("a 502 from the scanner must fail the job");
    assert!(err.to_string().contains("grype exited with 1"), "{err}");
    assert!(
        scan_reports(&stack.metadata_store, &namespace, &image)
            .await
            .unwrap()
            .is_empty()
    );
}

/// A cache miss on a scanning pull-through repository enqueues one scan
/// job for the image it stores; the hit that follows enqueues nothing more.
#[tokio::test]
async fn a_cache_miss_enqueues_a_scan_job_in_a_scanning_pull_through_repository() {
    use angos_oci::{MediaRange, MediaType, Reference, Tag};

    use crate::{
        registry::repository::{Config as RepositoryConfig, Repository},
        test_fixtures::client::test_client_config,
    };

    let stack = fs_test_stack();
    let namespace = Namespace::new("mirror/web").unwrap();
    let job_store = Arc::new(JobStore::new(
        stack.store.clone(),
        "scan-test",
        ClaimMode::Atomic,
    ));
    let upstream = MockServer::start().await;
    let image = r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000001","size":1},"layers":[]}"#;
    // The upstream sees the path below the repository prefix; the HEAD answers
    // the freshness check a second pull of the mutable tag makes.
    let digest = Digest::sha256_of_bytes(image.as_bytes());
    for verb in ["GET", "HEAD"] {
        Mock::given(method(verb))
            .and(path("/v2/web/manifests/latest"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(image)
                    .insert_header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                    .insert_header("Docker-Content-Digest", digest.to_string()),
            )
            .mount(&upstream)
            .await;
    }
    let repository = Repository::new(
        "mirror",
        &RepositoryConfig {
            upstream: vec![test_client_config(upstream.uri())],
            scan: Some(scan_everything()),
            ..Default::default()
        },
        &angos_cache::Config::Memory.to_backend().unwrap(),
        crate::registry::manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
    )
    .await
    .unwrap();
    let resolver = single_repo_resolver("mirror", repository);
    let registry = Registry::new(
        stack.blob_store.clone(),
        stack.metadata_store.clone(),
        resolver.clone(),
        RegistryConfig::new(job_store.clone()),
    );
    let repository = resolver.resolve(&namespace).unwrap();
    let accepted = [MediaRange::from(MediaType::oci_manifest())];
    let latest = Tag::new("latest").unwrap();
    let pull = || {
        registry.get_manifest_direct(
            Some(repository),
            &accepted,
            &namespace,
            Reference::Tag(latest.clone()),
            false,
            "test-client",
        )
    };

    pull().await.unwrap();
    assert_eq!(
        job_store.count_pending(Queue::Scan, 0).await.unwrap(),
        1,
        "the fill stores an image, which is scanned"
    );
    pull().await.unwrap();
    assert_eq!(
        job_store.count_pending(Queue::Scan, 0).await.unwrap(),
        1,
        "a refresh to the same digest stores nothing and scans nothing"
    );
}

/// A push into a scanning repository enqueues one scan job for an image
/// manifest and none for a report, while a repository without the flag
/// enqueues nothing.
#[tokio::test]
async fn a_push_enqueues_a_scan_job_only_for_an_image_in_a_scanning_repository() {
    use std::io::Cursor;

    use angos_oci::{MediaType, Reference, request::PutManifestRequest};

    let stack = fs_test_stack();
    let namespace = Namespace::new("apps/web").unwrap();
    let (_, config, layer) = seed_manifest(&stack.store, &stack.metadata_store, &namespace).await;
    let job_store = Arc::new(JobStore::new(
        stack.store.clone(),
        "scan-test",
        ClaimMode::Atomic,
    ));
    let mut repository = repository_with_replication("apps", Vec::new());
    repository.scan = Some(ImagePolicy::new(&scan_everything()));
    // The seeded digests carry links but no bytes; the push under test is
    // about enqueueing, not reference validation.
    let registry = Registry::new(
        stack.blob_store.clone(),
        stack.metadata_store.clone(),
        single_repo_resolver("apps", repository),
        RegistryConfig {
            validate_manifest_references: false,
            ..RegistryConfig::new(job_store.clone())
        },
    );
    let push = |body: String, namespace: &Namespace| {
        let request = PutManifestRequest {
            namespace: namespace.clone(),
            reference: Reference::Digest(Digest::sha256_of_bytes(body.as_bytes())),
            content_type: Some(MediaType::oci_manifest()),
            tags: Vec::new(),
            source_ts: None,
        };
        registry.handle_put_manifest(None, request, Cursor::new(body.into_bytes()))
    };
    let descriptor = |media: &str, digest: &Digest| {
        format!(r#"{{"mediaType":"{media}","digest":"{digest}","size":1}}"#)
    };
    let image = format!(
        r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{},"layers":[{}],"annotations":{{"v":"2"}}}}"#,
        descriptor("application/vnd.oci.image.config.v1+json", &config),
        descriptor("application/vnd.oci.image.layer.v1.tar", &layer),
    );
    push(image.clone(), &namespace).await.unwrap();
    assert_eq!(job_store.count_pending(Queue::Scan, 0).await.unwrap(), 1);

    let report = format!(
        r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","artifactType":"application/sarif+json","config":{},"layers":[{}],"subject":{}}}"#,
        descriptor("application/vnd.oci.image.config.v1+json", &config),
        descriptor("application/vnd.oci.image.layer.v1.tar", &layer),
        descriptor(
            "application/vnd.oci.image.manifest.v1+json",
            &Digest::sha256_of_bytes(image.as_bytes())
        ),
    );
    push(report, &namespace).await.unwrap();
    assert_eq!(
        job_store.count_pending(Queue::Scan, 0).await.unwrap(),
        1,
        "a report is not a scan subject"
    );

    let other = Namespace::new("other/web").unwrap();
    let (_, config, layer) = seed_manifest(&stack.store, &stack.metadata_store, &other).await;
    let foreign = format!(
        r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{},"layers":[{}],"annotations":{{"v":"3"}}}}"#,
        descriptor("application/vnd.oci.image.config.v1+json", &config),
        descriptor("application/vnd.oci.image.layer.v1.tar", &layer),
    );
    push(foreign, &other).await.unwrap();
    assert_eq!(
        job_store.count_pending(Queue::Scan, 0).await.unwrap(),
        1,
        "a repository without a scan policy enqueues nothing"
    );
}

/// `--force` scans an image that already carries a report and attaches a
/// second one.
#[tokio::test]
async fn a_forced_scan_job_scans_a_reported_image_again() {
    let stack = fs_test_stack();
    let namespace = Namespace::new("apps/web").unwrap();
    let (image, _, _) = seed_manifest(&stack.store, &stack.metadata_store, &namespace).await;
    let scanner = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(SARIF))
        .expect(2)
        .mount(&scanner)
        .await;
    let handler = handler_for(
        &stack,
        repository_with_replication("apps", Vec::new()),
        &scanner,
        None,
    );
    handler
        .execute(
            &build_envelope(&ScanImagePayload {
                namespace: namespace.clone(),
                digest: image.clone(),
                force: false,
                reported_before: None,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    handler
        .execute(
            &build_envelope(&ScanImagePayload {
                namespace: namespace.clone(),
                digest: image.clone(),
                force: true,
                reported_before: None,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    // The forced report carries a later `created` annotation, so it is a
    // distinct manifest and a second referrer.
    assert_eq!(
        scan_reports(&stack.metadata_store, &namespace, &image)
            .await
            .unwrap()
            .len(),
        2
    );
}

/// Trivy tags each rule with its severity and states it in the message;
/// counts follow the tags.
#[test]
fn summary_counts_trivy_severities_from_rule_tags() {
    let report = br#"{"runs":[{"tool":{"driver":{"name":"Trivy","version":"0.74.0","rules":[
        {"id":"CVE-1","properties":{"security-severity":"2.0","tags":["vulnerability","LOW"]}},
        {"id":"CVE-2","properties":{"security-severity":"9.8","tags":["vulnerability","CRITICAL"]}}
    ]}},"results":[
        {"ruleId":"CVE-1","ruleIndex":0,"message":{"text":"Package: apt\nSeverity: LOW"}},
        {"ruleId":"CVE-2","ruleIndex":1,"message":{"text":"Package: ssl\nSeverity: CRITICAL"}},
        {"ruleId":"CVE-2","ruleIndex":1,"message":{"text":"Package: ssl\nSeverity: CRITICAL"}}
    ]}]}"#;
    let summary = ScanSummary::of(report);
    assert_eq!(summary.scanner.as_deref(), Some("Trivy 0.74.0"));
    assert_eq!(summary.counts, [2, 0, 0, 1, 0]);
}

/// Grype states the severity in the message and carries only a score on the
/// rule; a result naming no known rule falls back to the score, then unknown.
#[test]
fn summary_reads_grype_messages_then_scores() {
    let report = br#"{"runs":[{"tool":{"driver":{"name":"grype","version":"0.79.1","rules":[
        {"id":"CVE-9-busybox","properties":{"security-severity":"6.5"}},
        {"id":"CVE-8-zlib","properties":{"security-severity":"7.5"}},
        {"id":"CVE-7-none","properties":{}}
    ]}},"results":[
        {"ruleId":"CVE-9-busybox","message":{"text":"A medium vulnerability in apk package: busybox"}},
        {"ruleId":"CVE-8-zlib","message":{"text":"found in image"}},
        {"ruleId":"CVE-7-none","message":{"text":"found in image"}}
    ]}]}"#;
    let summary = ScanSummary::of(report);
    assert_eq!(summary.counts, [0, 1, 1, 0, 1]);
    assert_eq!(
        summary
            .annotations()
            .iter()
            .find(|(k, _)| k == "io.angos.scan.high")
            .map(|(_, v)| v.as_str()),
        Some("1")
    );
}

#[test]
fn summary_of_a_non_sarif_body_is_empty() {
    assert_eq!(ScanSummary::of(b"not json"), ScanSummary::default());
}

/// A handler over `scanner`, presenting `token`, for the repository
/// `repository` resolves `apps/...` to.
fn handler_for(
    stack: &FsTestStack,
    repository: Repository,
    scanner: &MockServer,
    token: Option<Secret<String>>,
) -> ScanJobHandler {
    let job_store = Arc::new(JobStore::new(
        stack.store.clone(),
        "scan-test",
        ClaimMode::Atomic,
    ));
    let registry = Registry::new(
        stack.blob_store.clone(),
        stack.metadata_store.clone(),
        single_repo_resolver("apps", repository),
        RegistryConfig::new(job_store),
    );
    ScanJobHandler::new(
        registry,
        stack.blob_store.clone(),
        stack.metadata_store.clone(),
        &ScanConfig {
            url: scanner.uri(),
            token,
            timeout_secs: 5,
            policy: PolicyConfig {
                default: None,
                rules: Vec::new(),
            },
        },
    )
    .unwrap()
}

fn payload(namespace: &Namespace, digest: &Digest) -> ScanImagePayload {
    ScanImagePayload {
        namespace: namespace.clone(),
        digest: digest.clone(),
        force: false,
        reported_before: None,
    }
}

/// A run's `reported_before` makes the job a no-op only when a report
/// newer than it exists; a report older than the cutoff is the stale one the
/// run saw, and is scanned over.
#[tokio::test]
async fn reported_before_skips_only_when_a_newer_report_exists() {
    let stack = fs_test_stack();
    let namespace = Namespace::new("apps/web").unwrap();
    let (image, _, _) = seed_manifest(&stack.store, &stack.metadata_store, &namespace).await;
    let scanner = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(SARIF))
        .expect(2)
        .mount(&scanner)
        .await;
    let handler = handler_for(
        &stack,
        repository_with_replication("apps", Vec::new()),
        &scanner,
        None,
    );

    handler
        .execute(&build_envelope(&payload(&namespace, &image)).unwrap())
        .await
        .unwrap();
    let attached_at = Utc::now();

    // Judged before the report landed: the push it came from already scanned.
    handler
        .execute(
            &build_envelope(&ScanImagePayload {
                reported_before: Some(attached_at - TimeDelta::hours(1)),
                ..payload(&namespace, &image)
            })
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        scan_reports(&stack.metadata_store, &namespace, &image)
            .await
            .unwrap()
            .len(),
        1
    );

    // Judged after it: the report is the stale one, so it is scanned again.
    handler
        .execute(
            &build_envelope(&ScanImagePayload {
                reported_before: Some(Utc::now()),
                ..payload(&namespace, &image)
            })
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        scan_reports(&stack.metadata_store, &namespace, &image)
            .await
            .unwrap()
            .len(),
        2
    );
}

/// A scan policy that scans every image.
fn scan_everything() -> PolicyConfig<ScanAction> {
    PolicyConfig {
        default: Some(ScanAction::Scan),
        rules: Vec::new(),
    }
}

/// `default` decides an image no rule matches and a matching rule the
/// opposite; at push an image is untagged or under its pushed tags, never
/// scanned, so an age rule scans it.
#[test]
fn a_policy_default_decides_and_a_rule_flips_it() {
    let policy = |default: Option<ScanAction>, rules: &[&str]| {
        ImagePolicy::new(&PolicyConfig {
            default,
            rules: rules.iter().map(|r| CelRule::compile(r).unwrap()).collect(),
        })
    };
    let latest = [Tag::new("latest").unwrap()];
    let v1 = [Tag::new("v1").unwrap()];
    assert!(policy(Some(ScanAction::Scan), &[]).applies_at_push(&[]));
    assert!(!policy(Some(ScanAction::Skip), &[]).applies_at_push(&[]));
    assert!(
        !policy(None, &[]).applies_at_push(&latest),
        "no default is skip"
    );

    let scan_latest = policy(None, &["image.tag == 'latest'"]);
    assert!(scan_latest.applies_at_push(&latest));
    assert!(!scan_latest.applies_at_push(&v1));
    assert!(
        !policy(Some(ScanAction::Scan), &["image.tag == 'latest'"]).applies_at_push(&latest),
        "with a scanning default a matching rule skips"
    );
    assert!(
        policy(None, &["image.scanned_at < now() - days(30)"]).applies_at_push(&v1),
        "a fresh image was never scanned"
    );
    assert!(
        policy(Some(ScanAction::Skip), &["image.tag.size() > 3"]).applies_at_push(&[]),
        "a rule that cannot be evaluated applies the policy"
    );

    let config: ScanConfig = toml::from_str(
        r#"
        url = "http://scanner:8766"
        default = "scan"
        rules = ["image.tag == 'nightly'"]
        "#,
    )
    .unwrap();
    assert_eq!(config.policy.default, Some(ScanAction::Scan));
    assert_eq!(config.policy.rules.len(), 1);
    let service_only: ScanConfig = toml::from_str(r#"url = "http://scanner:8766""#).unwrap();
    assert!(!service_only.policy.is_set());
}
