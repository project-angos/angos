//! The `angos migrate` maintenance command: converts pre-JSON link metadata
//! into the current JSON [`LinkMetadata`] format.
//!
//! Registries seeded from a raw upstream `distribution` on-disk layout hold
//! link files that are a bare digest string rather than JSON. The serving
//! paths parse link files as JSON only, so such a link no longer resolves,
//! cannot be rewritten (a read-modify-write reads it first), and cannot be
//! deleted through the API until it is migrated. This command walks every link
//! object once and rewrites each bare-digest file as JSON, leaving already-JSON
//! links and unrecognisable files untouched. It is idempotent, so a partially
//! completed run can simply be re-run.

use std::str;

use argh::FromArgs;
use bytes::Bytes;
use tracing::{debug, info, warn};

use angos_storage::ObjectStore;

use crate::{
    command::bootstrap,
    configuration::Configuration,
    oci::Digest,
    registry::{self, metadata_store::LinkMetadata, path_builder},
};

mod error;

pub use error::Error;

/// Number of link keys fetched per flat-listing page.
const PAGE_SIZE: u16 = 100;

/// A link file's on-disk form, decided by trying to parse its raw bytes.
enum LinkForm {
    /// Already the current JSON `LinkMetadata`: nothing to do.
    Current,
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
    if serde_json::from_slice::<LinkMetadata>(raw).is_ok() {
        return LinkForm::Current;
    }
    match str::from_utf8(raw)
        .ok()
        .and_then(|text| Digest::try_from(text.trim()).ok())
    {
        Some(target) => LinkForm::Legacy(target),
        None => LinkForm::Unrecognized,
    }
}

/// Walk every link object and rewrite each bare-digest file as JSON. Supersedes
/// the removed runtime fallback that parsed bare-digest links on every read.
pub async fn run(options: &Options, config: &Configuration) -> Result<(), Error> {
    let bootstrap::MaintenanceContext { metadata_store, .. } =
        bootstrap::maintenance_context(config).await?;

    if options.dry_run {
        info!("Dry-run mode: scanning links without rewriting them");
    }

    let object_store = metadata_store.store().object_store().as_ref();
    let report = migrate_links(object_store, options.dry_run).await?;

    log_summary(&report, options.dry_run);
    Ok(())
}

/// Walk every link object under the repositories root, rewriting each
/// bare-digest file as JSON. Paginates so it never holds more than one page of
/// keys in memory.
async fn migrate_links(object_store: &dyn ObjectStore, dry_run: bool) -> Result<Report, Error> {
    let root = path_builder::repository_dir();
    let mut report = Report::default();
    let mut token = None;
    loop {
        let page = object_store
            .list(root, PAGE_SIZE, token)
            .await
            .map_err(registry::Error::from)?;
        for key in page.items {
            // `list` yields keys relative to `root`; rebuild the full key before
            // touching the object.
            if key.ends_with("/link") {
                let full_key = format!("{root}/{key}");
                migrate_one(object_store, &full_key, dry_run, &mut report).await?;
            }
        }
        match page.next_token {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    Ok(report)
}

/// Read one link file and rewrite it when it is a bare-digest legacy link.
async fn migrate_one(
    object_store: &dyn ObjectStore,
    key: &str,
    dry_run: bool,
    report: &mut Report,
) -> Result<(), Error> {
    report.scanned += 1;
    let raw = object_store.get(key).await.map_err(registry::Error::from)?;
    match classify(&raw) {
        LinkForm::Current => report.current += 1,
        LinkForm::Unrecognized => {
            report.unrecognized += 1;
            warn!("Link {key} is neither JSON nor a bare digest; leaving it untouched");
        }
        LinkForm::Legacy(target) => {
            report.migrated += 1;
            if dry_run {
                info!("Would migrate legacy link {key} -> {target}");
            } else {
                let metadata = LinkMetadata::without_timestamp(target.clone());
                let body =
                    Bytes::from(serde_json::to_vec(&metadata).map_err(registry::Error::from)?);
                object_store
                    .put(key, body)
                    .await
                    .map_err(registry::Error::from)?;
                debug!("Migrated legacy link {key} -> {target}");
            }
        }
    }
    Ok(())
}

fn log_summary(report: &Report, dry_run: bool) {
    let verb = if dry_run { "would migrate" } else { "migrated" };
    info!(
        "Link migration complete: scanned {}, {verb} {}, already current {}, unrecognized {}",
        report.scanned, report.migrated, report.current, report.unrecognized
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

    fn digest() -> Digest {
        Digest::sha256(HASH).unwrap()
    }

    #[test]
    fn classify_recognizes_current_json_link() {
        let metadata = LinkMetadata::without_timestamp(digest());
        let json = serde_json::to_vec(&metadata).unwrap();
        assert!(matches!(classify(&json), LinkForm::Current));
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
        let namespace = Namespace::new("migrate-repo").unwrap();
        let link = LinkKind::Tag(Tag::new("latest").unwrap());

        // Seed a pre-JSON bare-digest link, the format the serving path no
        // longer reads.
        put_link_raw(
            metadata_store.store(),
            &namespace,
            &link,
            digest().to_string().as_bytes(),
        )
        .await;
        assert!(
            metadata_store.read_link(&namespace, &link).await.is_err(),
            "bare-digest link should not parse before migration"
        );

        let object_store = metadata_store.store().object_store().as_ref();
        let report = migrate_links(object_store, false).await.unwrap();
        assert_eq!(report.migrated, 1);
        assert_eq!(report.scanned, 1);

        let migrated = metadata_store.read_link(&namespace, &link).await.unwrap();
        assert_eq!(migrated.target, digest());
        assert!(
            migrated.created_at.is_none(),
            "a migrated legacy link must never win last-writer-wins"
        );
    }

    #[tokio::test]
    async fn migrate_is_idempotent_and_leaves_current_links_untouched() {
        let test_case = FSRegistryTestCase::new();
        let metadata_store = test_case.metadata_store();
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
        migrate_links(object_store, false).await.unwrap();

        // A second pass finds the link already current and rewrites nothing.
        let report = migrate_links(object_store, false).await.unwrap();
        assert_eq!(report.migrated, 0);
        assert_eq!(report.current, 1);
    }

    #[tokio::test]
    async fn dry_run_reports_without_rewriting() {
        let test_case = FSRegistryTestCase::new();
        let metadata_store = test_case.metadata_store();
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
        let report = migrate_links(object_store, true).await.unwrap();
        assert_eq!(report.migrated, 1);
        assert!(
            metadata_store.read_link(&namespace, &link).await.is_err(),
            "dry run must not rewrite the link"
        );
    }
}
