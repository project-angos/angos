//! The blob-reclamation marker protocol: the one place a writer and the
//! collector must agree, with no lock.
//!
//! A collector about to delete blob data publishes a [`GcRun`] naming its
//! digest range, re-reads that marker before the irreversible delete, and
//! expires it afterwards; a writer that has just written reference keys lists
//! `v2/gc/` once and backs off on an unexpired run covering one of its
//! digests. Either the writer's reference completed before the collector's
//! liveness listing (the key is younger than the grace period, so the blob
//! reads live), or it completed after, in which case the marker was still
//! visible to the writer's check.
//!
//! That last part is why a finished run expires its marker instead of removing
//! it: a writer whose reference landed after the run's last liveness listing
//! would otherwise find nothing and write onto reclaimed bytes. The expiry
//! also stops a crashed collector from wedging writers, since a live one
//! fences itself by refreshing before each delete.

use std::time::Duration as StdDuration;

use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use uuid::Uuid;

use angos_backoff::Backoff;
use angos_oci::Digest;
use angos_storage::Error as StorageError;

use crate::registry::{Error, keys::GC_ROOT, metadata_store::MetadataStore};

/// Attempts and jittered backoff for a writer waiting out a collector run; a
/// run only covers one batch, so the wait is short.
const WRITER_BACKOFF_ATTEMPTS: u32 = 5;
const WRITER_BACKOFF: Backoff =
    Backoff::exponential(StdDuration::from_millis(50), StdDuration::from_millis(800)).with_jitter();

/// One collector run's published claim over an inclusive digest range.
/// `Digest` ordering matches the lexical order of its `algo:hash` string, so
/// the range covers exactly the stored keys between its bounds.
#[derive(Debug, Serialize, Deserialize)]
pub struct GcRun {
    pub start: Digest,
    pub end: Digest,
    pub expires_at: DateTime<Utc>,
    pub instance: String,
}

/// One collector run's range marker: the only key a writer and the collector
/// both consult.
fn gc_run_path(run: &str) -> String {
    format!("{GC_ROOT}/{run}")
}

/// A held claim: the marker key plus the token that proves ownership on
/// refresh.
pub struct GcClaim {
    key: String,
    instance: String,
    start: Digest,
    end: Digest,
}

impl MetadataStore {
    /// Writer-side wait: brief backoff while an unexpired collector run
    /// covers any of `digests`. `false` means still covered after the budget,
    /// so the caller must treat the blobs as being reclaimed.
    pub async fn gc_clear(&self, digests: &[&Digest]) -> Result<bool, Error> {
        for attempt in 0..WRITER_BACKOFF_ATTEMPTS {
            if !self.gc_blocked(digests).await? {
                return Ok(true);
            }
            sleep(WRITER_BACKOFF.delay(attempt)).await;
        }
        Ok(false)
    }

    /// Whether an unexpired collector run covers any of `digests`; one
    /// listing, nothing per blob.
    pub async fn gc_blocked(&self, digests: &[&Digest]) -> Result<bool, Error> {
        let mut token = None;
        loop {
            let page = self.object_store().list(GC_ROOT, 100, token).await?;
            for run in &page.items {
                let key = gc_run_path(run);
                let raw = match self.object_store().get(&key).await {
                    Ok(raw) => raw,
                    // Released between the listing and the read.
                    Err(StorageError::NotFound) => continue,
                    Err(e) => return Err(e.into()),
                };
                // An unreadable marker blocks: failing open here would let a
                // corrupt marker green-light a delete race.
                let Ok(run) = serde_json::from_slice::<GcRun>(&raw) else {
                    return Ok(true);
                };
                if run.expires_at < Utc::now() {
                    // A released marker lingers by design and scrub leaves it
                    // alone, so the writer that reads it expired reaps it.
                    let _ = self.object_store().delete(&key).await;
                    continue;
                }
                if digests
                    .iter()
                    .any(|digest| run.start <= **digest && **digest <= run.end)
                {
                    return Ok(true);
                }
            }
            token = page.next_token;
            if token.is_none() {
                return Ok(false);
            }
        }
    }

    /// Collector side: publish a run marker covering `start..=end`. The
    /// expiry is generous because safety rests on [`Self::gc_refresh`], not
    /// on the timer.
    pub async fn gc_claim(&self, start: &Digest, end: &Digest) -> Result<GcClaim, Error> {
        let claim = GcClaim {
            key: gc_run_path(&Uuid::new_v4().to_string()),
            instance: Uuid::new_v4().to_string(),
            start: start.clone(),
            end: end.clone(),
        };
        // A fresh UUID cannot legitimately exist; adopting one would fence
        // against another collector's live marker.
        let body = self.gc_run_body(&claim, None)?;
        if !self
            .object_store()
            .create_if_absent(&claim.key, body)
            .await?
        {
            return Err(Error::Internal(format!(
                "gc run marker collision at {}",
                claim.key
            )));
        }
        Ok(claim)
    }

    /// Re-read and re-stamp the claim before an irreversible delete. `false`
    /// means the marker was lost or overwritten, so stop collecting: a writer
    /// may already have read it as expired and skipped its check.
    pub async fn gc_refresh(&self, claim: &GcClaim) -> Result<bool, Error> {
        match self.object_store().get(&claim.key).await {
            Ok(raw) => {
                let Ok(run) = serde_json::from_slice::<GcRun>(&raw) else {
                    return Ok(false);
                };
                if run.instance != claim.instance || run.expires_at < Utc::now() {
                    return Ok(false);
                }
            }
            Err(StorageError::NotFound) => return Ok(false),
            Err(e) => return Err(e.into()),
        }
        let body = self.gc_run_body(claim, None)?;
        self.object_store().put(&claim.key, body).await?;
        Ok(true)
    }

    /// Expire the claim once the range is done, rather than removing it: a
    /// writer whose reference key landed after this run's last liveness
    /// listing must still find the marker and back off. The linger has to
    /// outlast the gap between a writer's reference wave and its collector
    /// check, which is one listing plus the backoff budget above; writers reap
    /// what they read expired.
    pub async fn gc_release(&self, claim: GcClaim) -> Result<(), Error> {
        let body = self.gc_run_body(
            &claim,
            Some(Utc::now() + Duration::milliseconds(self.release_linger_ms)),
        )?;
        self.object_store()
            .put(&claim.key, body)
            .await
            .map_err(Error::from)
    }

    /// The claim's marker body, expiring at `expires_at`. `None` takes twice
    /// the grace, floored so a marker outlives its own publish under a tiny
    /// grace and capped so an absurd one cannot overflow the chrono
    /// arithmetic. Capping costs nothing: the expiry only bounds how long a
    /// crashed collector wedges writers, since a live one refreshes before
    /// every delete.
    fn gc_run_body(
        &self,
        claim: &GcClaim,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<Bytes, Error> {
        let expires_at = expires_at.unwrap_or_else(|| {
            let ttl = i64::try_from(self.gc_grace_secs)
                .unwrap_or(i64::MAX)
                .saturating_mul(2)
                .clamp(60, 86_400);
            Utc::now() + Duration::seconds(ttl)
        });
        Ok(Bytes::from(serde_json::to_vec(&GcRun {
            start: claim.start.clone(),
            end: claim.end.clone(),
            expires_at,
            instance: claim.instance.clone(),
        })?))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::time::sleep;

    use angos_oci::{Digest, MediaType, Namespace, Reference, Tag, UploadSessionId};

    use crate::{
        command::maintenance::{
            action::Action,
            executor::{ActionSink, Executor},
        },
        registry::{
            Error,
            blob_ownership::{GrantOutcome, grant_existing, promote_and_grant},
            keys::{DigestKeys, GC_ROOT, NamespaceKeys},
            metadata_store::{LinkKind, MetadataStore, Settings},
            test_utils::{FSRegistryTestCase, RegistryTestCase, upload_blob},
        },
    };
    /// How long a released marker keeps blocking `store`, as a std duration
    /// to sleep on.
    fn linger(store: &MetadataStore) -> Duration {
        Duration::from_millis(u64::try_from(store.release_linger_ms).unwrap_or(0))
    }

    async fn seed_blob(case: &FSRegistryTestCase, content: &[u8]) -> Digest {
        let digest = Digest::sha256_of_bytes(content);
        case.blob_store()
            .object_store()
            .put(&digest.blob_path(), Bytes::copy_from_slice(content))
            .await
            .unwrap();
        digest
    }

    /// An ownership key pins a blob unconditionally; only once it is revoked
    /// does the collector delete the bytes and the stale reference keys.
    #[tokio::test]
    async fn collector_never_reclaims_a_referenced_blob() {
        let case = FSRegistryTestCase::new();
        let store = case.metadata_store();
        let namespace = Namespace::new("gc-referenced").unwrap();
        let digest = seed_blob(&case, b"gc-referenced-bytes").await;
        store
            .insert_reference(&namespace, &digest, &LinkKind::Blob(digest.clone()))
            .await
            .unwrap();

        let executor = Executor::new_for_test(case.blob_store(), store.clone());
        executor
            .apply(Action::DeleteOrphanBlob(digest.clone()))
            .await
            .unwrap();
        assert!(
            case.blob_store().read(&digest).await.is_ok(),
            "an owned blob must never be reclaimed"
        );

        store
            .revoke_blob_ownership(&namespace, &digest)
            .await
            .unwrap();
        executor
            .apply(Action::DeleteOrphanBlob(digest.clone()))
            .await
            .unwrap();
        assert!(
            case.blob_store().read(&digest).await.is_err(),
            "an unreferenced blob past the grace must be reclaimed"
        );
        assert!(
            store.read_blob_index(&digest).await.is_err(),
            "the reclaimed blob's reference keys must be swept"
        );
    }

    /// An unexpired run covering a digest blocks writers, and releasing it
    /// unblocks them.
    #[tokio::test]
    async fn a_push_backs_off_while_a_collector_run_covers_its_blob() {
        let case = FSRegistryTestCase::new();
        let store = case.metadata_store();
        let digest = seed_blob(&case, b"gc-covered-bytes").await;

        let claim = store.gc_claim(&digest, &digest).await.unwrap();
        assert!(
            store.gc_blocked(&[&digest]).await.unwrap(),
            "an unexpired run covering the digest must block a writer"
        );

        // Releasing expires the marker rather than removing it: a writer whose
        // reference landed after the run's last liveness listing must still find
        // it, since a finished run leaves nothing else to find.
        store.gc_release(claim).await.unwrap();
        assert!(
            store.gc_blocked(&[&digest]).await.unwrap(),
            "a released run must keep blocking until its linger expires"
        );

        sleep(linger(&store) * 2).await;
        assert!(
            !store.gc_blocked(&[&digest]).await.unwrap(),
            "an expired run must unblock writers"
        );
        assert!(
            store
                .object_store()
                .list(GC_ROOT, 10, None)
                .await
                .unwrap()
                .items
                .is_empty(),
            "the writer that read the marker expired must have reaped it"
        );
    }

    /// A guarded grant against still-present bytes fails closed under a covering
    /// run rather than handing out bytes the collector may be deleting.
    #[tokio::test]
    async fn a_guarded_grant_fails_closed_while_a_run_covers_present_bytes() {
        let case = FSRegistryTestCase::new();
        let store = case.metadata_store();
        let namespace = Namespace::new("gc-blocked-grant").unwrap();
        let digest = seed_blob(&case, b"gc-blocked-grant-bytes").await;

        let claim = store.gc_claim(&digest, &digest).await.unwrap();
        let outcome = grant_existing(
            &case.blob_store(),
            case.registry().metadata_store(),
            &namespace,
            &digest,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            GrantOutcome::ReclaimBlocked,
            "present bytes under a covering run must report the reclaim"
        );

        let result = promote_and_grant(
            &case.blob_store(),
            store.as_ref(),
            &namespace,
            &UploadSessionId::generate(),
            &digest,
            22,
        )
        .await;
        assert!(
            matches!(result, Err(Error::ReclamationInProgress(_))),
            "promotion over a covering run must fail closed with a retryable conflict, got {result:?}"
        );
        store.gc_release(claim).await.unwrap();
    }

    /// A grant racing a sweep re-probes the blob after the collector check, so
    /// it never hands out a reference to reclaimed bytes.
    #[tokio::test]
    async fn a_guarded_grant_never_returns_a_reclaimed_blob() {
        let case = FSRegistryTestCase::new();
        let registry = case.registry();
        let namespace = Namespace::new("gc-mount").unwrap();
        let digest = seed_blob(&case, b"gc-mount-bytes").await;

        // The sweep wins the race.
        case.blob_store().delete_blob(&digest).await.unwrap();
        let outcome = grant_existing(
            &case.blob_store(),
            registry.metadata_store(),
            &namespace,
            &digest,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            GrantOutcome::BytesAbsent,
            "a reclaimed blob must never be granted"
        );
    }

    /// A crash between waves leaves only legal states, emulated by rewinding a
    /// completed push one wave at a time.
    #[tokio::test]
    async fn a_push_interrupted_between_waves_reads_consistently() {
        let case = FSRegistryTestCase::new();
        let registry = case.registry();
        let store = case.metadata_store();
        let namespace = Namespace::new("test-repo/torn-push").unwrap();

        let config_digest = upload_blob(registry, &namespace, br#"{"torn":true}"#).await;
        let media_type = MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap();
        let content = serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 2,
            "mediaType": media_type,
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest.to_string(),
                "size": 13
            },
            "layers": []
        }))
        .unwrap();
        let digest = registry
            .put_manifest(
                &namespace,
                &Reference::Tag(Tag::new("latest").unwrap()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap()
            .digest;

        // Rewind wave D.
        store
            .object_store()
            .delete_prefix(&namespace.tag_entry_dir(&Tag::new("latest").unwrap()))
            .await
            .unwrap();
        assert!(
            store
                .read_link(&namespace, &LinkKind::Tag(Tag::new("latest").unwrap()))
                .await
                .is_err(),
            "without its wave-D entry the tag must read as absent"
        );
        assert!(
            store
                .read_link(&namespace, &LinkKind::Digest(digest.clone()))
                .await
                .is_ok(),
            "the wave-C revision must stay resolvable: the legal push-by-digest state"
        );

        // Rewind wave C.
        store
            .object_store()
            .delete(&namespace.revision_record_path(&digest))
            .await
            .unwrap();
        assert!(
            store
                .read_link(&namespace, &LinkKind::Digest(digest))
                .await
                .is_err(),
            "without its record the revision must read as absent"
        );
    }

    /// The writer half of the marker protocol through the public push path: a
    /// push referencing a covered blob fails with the reclamation conflict, and a
    /// retry after the run's release succeeds.
    #[tokio::test]
    async fn a_manifest_push_fails_closed_while_a_run_covers_its_blob() {
        let case = FSRegistryTestCase::new();
        let registry = case.registry();
        let store = case.metadata_store();
        let namespace = Namespace::new("gc-writer-push").unwrap();

        let config_digest = upload_blob(registry, &namespace, br#"{"gc":true}"#).await;
        let media_type = MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap();
        let content = serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 2,
            "mediaType": media_type,
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest.to_string(),
                "size": 11
            },
            "layers": []
        }))
        .unwrap();
        let reference = Reference::Tag(Tag::new("latest").unwrap());

        let claim = store
            .gc_claim(&config_digest, &config_digest)
            .await
            .unwrap();
        let error = registry
            .put_manifest(&namespace, &reference, Some(&media_type), &content)
            .await
            .err();
        assert!(
            matches!(error, Some(Error::ReclamationInProgress(_))),
            "a push referencing a covered blob must fail closed with the reclamation conflict, got {error:?}"
        );

        store.gc_release(claim).await.unwrap();
        sleep(linger(&store) * 2).await;
        registry
            .put_manifest(&namespace, &reference, Some(&media_type), &content)
            .await
            .expect("a retry past the released run's linger must succeed");
    }

    /// The marker's TTL is clamped, so a grace period no chrono duration can hold
    /// still publishes a run instead of panicking in the expiry arithmetic.
    #[tokio::test]
    async fn an_absurd_grace_period_still_publishes_a_run_marker() {
        let case = FSRegistryTestCase::new();
        let store = MetadataStore::new(
            case.metadata_store().object_store().clone(),
            Settings {
                gc_grace_secs: u64::MAX,
                ..Settings::default()
            },
        );
        let digest = Digest::sha256_of_bytes(b"absurd-grace");

        let claim = store
            .gc_claim(&digest, &digest)
            .await
            .expect("an absurd grace must not stop a collector from claiming");
        store.gc_release(claim).await.expect("release the claim");
    }
}
