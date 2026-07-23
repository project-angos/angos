//! The `angos migrate` maintenance command: converts pre-JSON link metadata
//! into the current JSON [`LinkMetadata`] format and backfills a served
//! manifest link's `media_type`.
//!
//! Registries seeded from a raw upstream `distribution` on-disk layout hold
//! link files that are a bare digest string rather than JSON. The serving
//! paths parse link files as JSON only, so such a link no longer resolves,
//! cannot be rewritten (a read-modify-write reads it first), and cannot be
//! deleted through the API until it is migrated. This command walks every link
//! object once and rewrites each bare-digest file as JSON, leaving already-JSON
//! links and unrecognisable files untouched.
//!
//! It also recovers the `media_type` of every tag and revision link that lacks
//! one (a bare-digest link, or one an earlier migrate rewrote without it) from
//! the manifest body, so a manifest HEAD/GET always carries the `Content-Type`
//! the OCI spec requires. It is idempotent, so a partially completed run can
//! simply be re-run.

use std::str;

use argh::FromArgs;
use bytes::Bytes;
use futures_util::TryStreamExt;
use tracing::{debug, info, warn};

use angos_storage::ObjectStore;

use crate::{
    command::bootstrap,
    configuration::Configuration,
    oci::{Digest, MediaType},
    registry::{
        self, blob_store::BlobStore, metadata_store::LinkMetadata, path_builder,
        recover_media_type,
    },
};

mod error;

pub use error::Error;

/// A link file's on-disk form, decided by trying to parse its raw bytes.
enum LinkForm {
    /// Already the current JSON `LinkMetadata`; may still need a `media_type` backfill.
    Current(Box<LinkMetadata>),
    /// A pre-JSON `distribution` link: a bare digest string to rebuild from.
    Legacy(Digest),
    /// Neither JSON nor a bare digest: left untouched and reported.
    Unrecognized,
}

/// Tally of what a run saw, logged as its summary.
#[derive(Default)]
struct Report {
    scanned: u64,
    current: u64,
    migrated: u64,
    backfilled: u64,
    unrecognized: u64,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "migrate",
    description = "Convert pre-JSON bare-digest link metadata to the current JSON format"
)]
pub struct Options {
    #[argh(switch, short = 'd')]
    /// display only, no actual changes applied
    pub dry_run: bool,
}

/// Classify a link file's raw bytes. JSON that deserialises to `LinkMetadata`
/// is current; otherwise a bare digest string is a legacy `distribution` link,
/// and anything else is unrecognised and must not be rewritten.
fn classify(raw: &[u8]) -> LinkForm {
    if let Ok(metadata) = serde_json::from_slice::<LinkMetadata>(raw) {
        return LinkForm::Current(Box::new(metadata));
    }
    match str::from_utf8(raw)
        .ok()
        .and_then(|text| Digest::try_from(text.trim()).ok())
    {
        Some(target) => LinkForm::Legacy(target),
        None => LinkForm::Unrecognized,
    }
}

/// Whether a link key is a tag or revision manifest link, the links served with
/// a `Content-Type`. Referrer and index back-links also live under `_manifests/`
/// but are never served as manifests, so they carry no `media_type`.
fn serves_manifest(key: &str) -> bool {
    key.contains("/_manifests/tags/") || key.contains("/_manifests/revisions/")
}

/// The media type for a served-manifest link's target, recovered from its stored
/// body. `None` for a non-manifest link (layer, config, referrer, index) or an
/// unreadable body, in which case the serving path recovers it on each read.
async fn link_media_type(
    blob_store: &BlobStore,
    key: &str,
    target: &Digest,
) -> Option<MediaType> {
    if !serves_manifest(key) {
        return None;
    }
    match blob_store.read(target).await {
        Ok(body) => Some(recover_media_type(&body)),
        Err(error) => {
            warn!("Cannot read manifest {target} for link {key} to recover its media type: {error}");
            None
        }
    }
}

async fn write_link(
    object_store: &dyn ObjectStore,
    key: &str,
    metadata: &LinkMetadata,
) -> Result<(), Error> {
    let body = Bytes::from(serde_json::to_vec(metadata).map_err(registry::Error::from)?);
    object_store
        .put(key, body)
        .await
        .map_err(registry::Error::from)?;
    Ok(())
}

/// Walk every link object and rewrite each bare-digest file as JSON. Supersedes
/// the removed runtime fallback that parsed bare-digest links on every read.
pub async fn run(options: &Options, config: &Configuration) -> Result<(), Error> {
    let bootstrap::MaintenanceContext {
        blob_store,
        metadata_store,
        ..
    } = bootstrap::maintenance_context(config).await?;

    if options.dry_run {
        info!("Dry-run mode: scanning links without rewriting them");
    }

    let object_store = metadata_store.store().object_store().as_ref();
    let report = migrate_links(object_store, &blob_store, options.dry_run).await?;

    log_summary(&report, options.dry_run);
    Ok(())
}

/// Walk every link object under the repositories root, rewriting each
/// bare-digest file as JSON and backfilling a missing manifest media type.
/// Streams the keys so it never holds more than one listing page in memory.
async fn migrate_links(
    object_store: &dyn ObjectStore,
    blob_store: &BlobStore,
    dry_run: bool,
) -> Result<Report, Error> {
    let root = path_builder::repository_dir();
    let mut report = Report::default();
    let mut keys = object_store.list_all(root).map_err(registry::Error::from);
    while let Some(key) = keys.try_next().await? {
        // `list_all` yields keys relative to `root`; rebuild the full key before
        // touching the object.
        if key.ends_with("/link") {
            let full_key = format!("{root}/{key}");
            migrate_one(object_store, blob_store, &full_key, dry_run, &mut report).await?;
        }
    }
    Ok(report)
}

/// Read one link file, rewriting a bare-digest legacy link as JSON and
/// backfilling a served-manifest link's missing media type from the body.
async fn migrate_one(
    object_store: &dyn ObjectStore,
    blob_store: &BlobStore,
    key: &str,
    dry_run: bool,
    report: &mut Report,
) -> Result<(), Error> {
    report.scanned += 1;
    let raw = object_store.get(key).await.map_err(registry::Error::from)?;
    match classify(&raw) {
        LinkForm::Current(metadata) => {
            backfill_current(object_store, blob_store, key, *metadata, dry_run, report).await?;
        }
        LinkForm::Unrecognized => {
            report.unrecognized += 1;
            warn!("Link {key} is neither JSON nor a bare digest; leaving it untouched");
        }
        LinkForm::Legacy(target) => {
            report.migrated += 1;
            if dry_run {
                info!("Would migrate legacy link {key} -> {target}");
            } else {
                let media_type = link_media_type(blob_store, key, &target).await;
                let metadata = LinkMetadata::without_timestamp(target.clone()).with_media_type(media_type);
                write_link(object_store, key, &metadata).await?;
                debug!("Migrated legacy link {key} -> {target}");
            }
        }
    }
    Ok(())
}

/// Backfill a served-manifest link that is already JSON but carries no media
/// type (rewritten by an earlier migrate that stored none), so its HEAD/GET no
/// longer depends on the serving-path recovery. Everything else counts as
/// current and is left untouched.
async fn backfill_current(
    object_store: &dyn ObjectStore,
    blob_store: &BlobStore,
    key: &str,
    metadata: LinkMetadata,
    dry_run: bool,
    report: &mut Report,
) -> Result<(), Error> {
    if metadata.media_type.is_some() || !serves_manifest(key) {
        report.current += 1;
        return Ok(());
    }
    if dry_run {
        report.backfilled += 1;
        info!("Would backfill media type for link {key}");
        return Ok(());
    }
    match link_media_type(blob_store, key, &metadata.target).await {
        Some(media_type) => {
            write_link(object_store, key, &metadata.with_media_type(Some(media_type))).await?;
            report.backfilled += 1;
            debug!("Backfilled media type for link {key}");
        }
        None => report.current += 1,
    }
    Ok(())
}

fn log_summary(report: &Report, dry_run: bool) {
    let verb = if dry_run { "would migrate" } else { "migrated" };
    let backfill_verb = if dry_run { "would backfill" } else { "backfilled" };
    info!(
        "Link migration complete: scanned {}, {verb} {}, {backfill_verb} media type on {}, already current {}, unrecognized {}",
        report.scanned, report.migrated, report.backfilled, report.current, report.unrecognized
    );
    if report.unrecognized > 0 {
        warn!(
            "{} link file(s) were neither JSON nor a bare digest and were left untouched; \
             inspect them manually",
            report.unrecognized
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        oci::{Namespace, Tag},
        registry::{
            metadata_store::LinkKind,
            test_utils::{FSRegistryTestCase, RegistryTestCase, put_link_raw},
        },
    };

    const HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";

    fn digest() -> Digest {
        Digest::sha256(HASH).unwrap()
    }

    /// Stores a minimal manifest body in the blob store, returning its digest.
    async fn seed_manifest_blob(blob_store: &BlobStore) -> Digest {
        let body = format!(r#"{{"schemaVersion":2,"mediaType":"{OCI_MANIFEST}"}}"#).into_bytes();
        let target = Digest::sha256_of_bytes(&body);
        blob_store.put_blob(&target, Bytes::from(body)).await.unwrap();
        target
    }

    #[test]
    fn classify_recognizes_current_json_link() {
        let metadata = LinkMetadata::without_timestamp(digest());
        let json = serde_json::to_vec(&metadata).unwrap();
        assert!(matches!(classify(&json), LinkForm::Current(_)));
    }

    #[test]
    fn serves_manifest_matches_tag_and_revision_links_only() {
        assert!(serves_manifest(
            "v2/repositories/ns/_manifests/tags/v1/current/link"
        ));
        assert!(serves_manifest(&format!(
            "v2/repositories/ns/_manifests/revisions/sha256/{HASH}/link"
        )));
        assert!(!serves_manifest(&format!(
            "v2/repositories/ns/_layers/sha256/{HASH}/link"
        )));
        assert!(!serves_manifest(&format!(
            "v2/repositories/ns/_manifests/referrers/sha256/{HASH}/sha256/{HASH}/link"
        )));
    }

    #[test]
    fn classify_recognizes_bare_digest_as_legacy() {
        let raw = format!("sha256:{HASH}");
        match classify(raw.as_bytes()) {
            LinkForm::Legacy(target) => assert_eq!(target, digest()),
            _ => panic!("bare digest should classify as legacy"),
        }
    }

    #[test]
    fn classify_tolerates_trailing_whitespace_on_bare_digest() {
        let raw = format!("sha256:{HASH}\n");
        assert!(matches!(classify(raw.as_bytes()), LinkForm::Legacy(_)));
    }

    #[test]
    fn classify_rejects_garbage_as_unrecognized() {
        assert!(matches!(classify(b"not a digest"), LinkForm::Unrecognized));
        assert!(matches!(classify(&[0xff, 0xfe]), LinkForm::Unrecognized));
        assert!(matches!(classify(b""), LinkForm::Unrecognized));
    }

    #[tokio::test]
    async fn migrate_rewrites_bare_digest_link_as_readable_json() {
        let test_case = FSRegistryTestCase::new();
        let metadata_store = test_case.metadata_store();
        let blob_store = test_case.blob_store();
        let namespace = Namespace::new("migrate-repo").unwrap();
        let link = LinkKind::Tag(Tag::new("latest").unwrap());
        let target = seed_manifest_blob(&blob_store).await;

        // Seed a pre-JSON bare-digest link, the format the serving path no
        // longer reads.
        put_link_raw(
            metadata_store.store(),
            &namespace,
            &link,
            target.to_string().as_bytes(),
        )
        .await;
        assert!(
            metadata_store.read_link(&namespace, &link).await.is_err(),
            "bare-digest link should not parse before migration"
        );

        let object_store = metadata_store.store().object_store().as_ref();
        let report = migrate_links(object_store, &blob_store, false).await.unwrap();
        assert_eq!(report.migrated, 1);
        assert_eq!(report.scanned, 1);

        let migrated = metadata_store.read_link(&namespace, &link).await.unwrap();
        assert_eq!(migrated.target, target);
        assert!(
            migrated.created_at.is_none(),
            "a migrated legacy link must never win last-writer-wins"
        );
        assert_eq!(
            migrated.media_type,
            Some(MediaType::new(OCI_MANIFEST).unwrap()),
            "a migrated tag link must recover its media type from the body"
        );
    }

    #[tokio::test]
    async fn migrate_backfills_media_type_on_a_media_typeless_json_link() {
        let test_case = FSRegistryTestCase::new();
        let metadata_store = test_case.metadata_store();
        let blob_store = test_case.blob_store();
        let namespace = Namespace::new("migrate-repo").unwrap();
        let link = LinkKind::Tag(Tag::new("latest").unwrap());
        let target = seed_manifest_blob(&blob_store).await;

        // A JSON tag link that an earlier migrate rewrote without a media type.
        let metadata = LinkMetadata::without_timestamp(target.clone());
        put_link_raw(
            metadata_store.store(),
            &namespace,
            &link,
            &serde_json::to_vec(&metadata).unwrap(),
        )
        .await;

        let object_store = metadata_store.store().object_store().as_ref();
        let report = migrate_links(object_store, &blob_store, false).await.unwrap();
        assert_eq!(report.migrated, 0);
        assert_eq!(report.backfilled, 1);

        let backfilled = metadata_store.read_link(&namespace, &link).await.unwrap();
        assert_eq!(
            backfilled.media_type,
            Some(MediaType::new(OCI_MANIFEST).unwrap())
        );
    }

    #[tokio::test]
    async fn migrate_leaves_non_manifest_links_without_media_type() {
        let test_case = FSRegistryTestCase::new();
        let metadata_store = test_case.metadata_store();
        let blob_store = test_case.blob_store();
        let namespace = Namespace::new("migrate-repo").unwrap();
        let link = LinkKind::Layer(digest());

        put_link_raw(
            metadata_store.store(),
            &namespace,
            &link,
            digest().to_string().as_bytes(),
        )
        .await;

        let object_store = metadata_store.store().object_store().as_ref();
        let report = migrate_links(object_store, &blob_store, false).await.unwrap();
        assert_eq!(report.migrated, 1);

        let migrated = metadata_store.read_link(&namespace, &link).await.unwrap();
        assert!(
            migrated.media_type.is_none(),
            "a layer link is not a served manifest and needs no media type"
        );
    }

    #[tokio::test]
    async fn migrate_is_idempotent_and_leaves_current_links_untouched() {
        let test_case = FSRegistryTestCase::new();
        let metadata_store = test_case.metadata_store();
        let blob_store = test_case.blob_store();
        let namespace = Namespace::new("migrate-repo").unwrap();
        let link = LinkKind::Tag(Tag::new("latest").unwrap());
        let target = seed_manifest_blob(&blob_store).await;

        put_link_raw(
            metadata_store.store(),
            &namespace,
            &link,
            target.to_string().as_bytes(),
        )
        .await;

        let object_store = metadata_store.store().object_store().as_ref();
        migrate_links(object_store, &blob_store, false).await.unwrap();

        // A second pass finds the link already current and rewrites nothing.
        let report = migrate_links(object_store, &blob_store, false).await.unwrap();
        assert_eq!(report.migrated, 0);
        assert_eq!(report.backfilled, 0);
        assert_eq!(report.current, 1);
    }

    #[tokio::test]
    async fn dry_run_reports_without_rewriting() {
        let test_case = FSRegistryTestCase::new();
        let metadata_store = test_case.metadata_store();
        let blob_store = test_case.blob_store();
        let namespace = Namespace::new("migrate-repo").unwrap();
        let link = LinkKind::Tag(Tag::new("latest").unwrap());

        put_link_raw(
            metadata_store.store(),
            &namespace,
            &link,
            digest().to_string().as_bytes(),
        )
        .await;

        let object_store = metadata_store.store().object_store().as_ref();
        let report = migrate_links(object_store, &blob_store, true).await.unwrap();
        assert_eq!(report.migrated, 1);
        assert!(
            metadata_store.read_link(&namespace, &link).await.is_err(),
            "dry run must not rewrite the link"
        );
    }
}
