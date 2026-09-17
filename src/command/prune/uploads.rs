//! The `-u` window sweeps: upload sessions, orphan S3 multiparts, and byteless
//! blob-index reference entries. Grant-only blob ownership is a retention subject
//! instead; see `checker::sweep_orphan_grants`.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use futures_util::TryStreamExt;
use tracing::{debug, error, info, warn};

use angos_oci::{Digest, Namespace, UploadSessionId};

use crate::{
    command::maintenance::{
        Error,
        action::Action,
        categorize::{KeyCategory, categorize},
        executor::{ActionSink, object_younger_than_grace},
        walk,
    },
    registry::{
        Error as RegistryError,
        blob_store::{BlobStore, UploadSummary},
        keys::{DigestKeys, REF_ROOT},
        metadata_store::MetadataStore,
    },
};

enum UploadVerdict {
    /// Missing summary or corrupted data.
    DeleteInconsistent,
    /// Age exceeds the window.
    DeleteObsolete,
    Keep,
}

#[allow(clippy::match_same_arms)]
fn classify_upload(
    summary: Result<&UploadSummary, &RegistryError>,
    window: Duration,
    now: DateTime<Utc>,
) -> UploadVerdict {
    match summary {
        Ok(s) if now.signed_duration_since(s.started_at) > window => UploadVerdict::DeleteObsolete,
        Ok(_) => UploadVerdict::Keep,
        // A corrupt record never decodes on a retry, so the session can never
        // complete and is safe to reap at any age.
        Err(
            RegistryError::BlobUploadUnknown
            | RegistryError::NotFound
            | RegistryError::BlobUnknown
            | RegistryError::Corrupt(_),
        ) => UploadVerdict::DeleteInconsistent,
        // Deleting on a transient read failure would destroy a live upload.
        Err(_) => UploadVerdict::Keep,
    }
}

/// Delete every upload session older than the window or with broken state.
/// Sessions of one namespace are checked up to `concurrency` at a time.
pub async fn sweep_upload_sessions(
    blob_store: &Arc<BlobStore>,
    window: Duration,
    sink: &dyn ActionSink,
    concurrency: usize,
) -> Result<(), Error> {
    for namespace in blob_store.collect_upload_namespaces(None).await? {
        let namespace = &namespace;
        blob_store
            .stream_uploads(namespace)
            .err_into::<Error>()
            .try_for_each_concurrent(concurrency, |session_id| async move {
                if let Err(e) =
                    sweep_one_upload(blob_store, namespace, &session_id, window, sink).await
                {
                    error!("prune: failed to check upload '{namespace}/{session_id}': {e}");
                }
                Ok(())
            })
            .await?;
    }
    Ok(())
}

async fn sweep_one_upload(
    blob_store: &Arc<BlobStore>,
    namespace: &Namespace,
    session_id: &UploadSessionId,
    window: Duration,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    let summary = blob_store.upload_summary(namespace, session_id).await;
    match classify_upload(summary.as_ref(), window, Utc::now()) {
        UploadVerdict::DeleteInconsistent | UploadVerdict::DeleteObsolete => {
            debug!("prune: reaping upload '{namespace}/{session_id}'");
            sink.apply(Action::DeleteExpiredUpload {
                namespace: namespace.clone(),
                session_id: session_id.clone(),
            })
            .await
        }
        UploadVerdict::Keep => Ok(()),
    }
}

/// Abort every in-flight S3 multipart upload older than the window whose
/// session marker is gone (a crash between opening the multipart and writing
/// the marker leaves exactly this).
pub async fn sweep_orphan_multiparts(
    blob_store: &BlobStore,
    window: Duration,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    let orphans = blob_store.list_orphan_multipart_uploads(window).await?;
    let count = orphans.len();
    for orphan in orphans {
        sink.apply(Action::AbortMultipartUpload { upload: orphan })
            .await?;
    }
    info!("prune: found {count} orphan multipart upload(s)");
    Ok(())
}

/// Remove blob-index entries referencing byteless blobs once the entry is
/// older than the window: a grant whose bytes never landed or were reclaimed
/// out-of-band. A pull-through grant is purged too, since the next pull
/// re-fills and re-grants.
pub async fn sweep_byteless_refs(
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    window: Duration,
    sink: &dyn ActionSink,
    concurrency: usize,
) -> Result<(), Error> {
    let objects = metadata_store.object_store();
    let ctx = RefSweep {
        blob_store,
        metadata_store,
        window_secs: u64::try_from(window.num_seconds()).unwrap_or(0),
        sink,
    };
    let ctx = &ctx;
    walk::for_each_key(objects, REF_ROOT, concurrency, |key| async move {
        let KeyCategory::BlobRef {
            digest, namespace, ..
        } = categorize(&key)
        else {
            return;
        };
        if let Err(e) = sweep_one_ref(ctx, &key, &digest, &namespace).await {
            error!("prune: failed to check index entry '{key}': {e}");
        }
    })
    .await
}

struct RefSweep<'a> {
    blob_store: &'a Arc<BlobStore>,
    metadata_store: &'a Arc<MetadataStore>,
    window_secs: u64,
    sink: &'a dyn ActionSink,
}

async fn sweep_one_ref(
    ctx: &RefSweep<'_>,
    key: &str,
    blob: &Digest,
    namespace_raw: &str,
) -> Result<(), Error> {
    match ctx.blob_store.size(blob).await {
        Ok(_) => return Ok(()),
        Err(RegistryError::BlobUnknown | RegistryError::NotFound) => {}
        Err(e) => return Err(e.into()),
    }
    let Ok(namespace) = Namespace::new(namespace_raw) else {
        return Ok(());
    };
    // A key with no timestamp is kept rather than raced against an upload that
    // granted before its bytes landed; a gone key is already revoked.
    let store = ctx.metadata_store.object_store().as_ref();
    if object_younger_than_grace(store, key, ctx.window_secs)
        .await
        .map_err(RegistryError::from)?
        .unwrap_or(true)
    {
        return Ok(());
    }

    warn!("prune: purging index entries for byteless blob '{blob}' in '{namespace}'");
    let links = ctx
        .metadata_store
        .read_blob_index_namespace(&namespace, blob)
        .await?;
    for link in links {
        // The walked key's age gate does not cover its siblings: a fresh
        // `own` granted before its bytes land is normal, so each entry is
        // gated on its own reference key, a gone one reading as old.
        let entry_key = blob.blob_ref_path(&namespace, &link);
        if object_younger_than_grace(store, &entry_key, ctx.window_secs)
            .await
            .map_err(RegistryError::from)?
            .unwrap_or(false)
        {
            continue;
        }
        ctx.sink
            .apply(Action::RemoveBlobIndexLink {
                namespace: namespace.clone(),
                blob: blob.clone(),
                link,
            })
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use crate::{command::prune::uploads::*, registry::keys::NamespaceKeys};

    fn fixed_now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2024, 6, 1, 12, 0, 0).unwrap()
    }

    fn summary_started_at(offset_secs: i64) -> UploadSummary {
        UploadSummary {
            size: 0,
            started_at: fixed_now() - Duration::seconds(offset_secs),
        }
    }

    #[test]
    fn classify_upload_keeps_recent_upload() {
        let verdict = classify_upload(
            Ok(&summary_started_at(1800)),
            Duration::hours(1),
            fixed_now(),
        );
        assert!(matches!(verdict, UploadVerdict::Keep));
    }

    #[test]
    fn classify_upload_keeps_exactly_at_window() {
        let verdict = classify_upload(
            Ok(&summary_started_at(3600)),
            Duration::hours(1),
            fixed_now(),
        );
        assert!(matches!(verdict, UploadVerdict::Keep));
    }

    #[test]
    fn classify_upload_deletes_past_window() {
        let verdict = classify_upload(
            Ok(&summary_started_at(3601)),
            Duration::hours(1),
            fixed_now(),
        );
        assert!(matches!(verdict, UploadVerdict::DeleteObsolete));
    }

    #[test]
    fn classify_upload_deletes_broken_state() {
        for error in [
            RegistryError::BlobUploadUnknown,
            RegistryError::NotFound,
            RegistryError::BlobUnknown,
            RegistryError::Corrupt("hash state deserialization error".to_string()),
        ] {
            let verdict = classify_upload(Err(&error), Duration::hours(1), fixed_now());
            assert!(matches!(verdict, UploadVerdict::DeleteInconsistent));
        }
    }

    #[test]
    fn classify_upload_keeps_on_transient_error() {
        let error = RegistryError::Internal("transient backend error".to_string());
        let verdict = classify_upload(Err(&error), Duration::hours(1), fixed_now());
        assert!(matches!(verdict, UploadVerdict::Keep));
    }

    use std::{sync::Mutex, time::Duration as StdDuration};

    use bytes::Bytes;
    use tokio::time::sleep;

    use angos_oci::{Digest, Namespace};

    use crate::{
        command::maintenance::executor::Executor,
        registry::{metadata_store::LinkKind, test_utils::for_each_backend},
    };

    #[tokio::test]
    async fn sweep_reaps_obsolete_upload_and_keeps_recent() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let blob_store = test_case.blob_store();

            let old_uuid = UploadSessionId::generate();
            blob_store
                .create_upload(&namespace, &old_uuid, None)
                .await
                .unwrap();

            let executor = Executor::new_for_test(blob_store.clone(), test_case.metadata_store());

            // Zero window: the just-created upload is already past it.
            sweep_upload_sessions(&blob_store, Duration::zero(), &executor, 4)
                .await
                .unwrap();
            assert!(
                blob_store
                    .upload_summary(&namespace, &old_uuid)
                    .await
                    .is_err(),
                "an upload past the window must be reaped"
            );

            let fresh_uuid = UploadSessionId::generate();
            blob_store
                .create_upload(&namespace, &fresh_uuid, None)
                .await
                .unwrap();
            sweep_upload_sessions(&blob_store, Duration::days(1), &executor, 4)
                .await
                .unwrap();
            assert!(
                blob_store
                    .upload_summary(&namespace, &fresh_uuid)
                    .await
                    .is_ok(),
                "an upload within the window must be kept"
            );
        })
        .await;
    }

    /// The sweep ages a session on its own record, not on the container's
    /// creation time: a `session.json` backdated in place is reaped.
    #[tokio::test]
    async fn sweep_ages_a_session_on_its_record() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/shapes").unwrap();
            let blob_store = test_case.blob_store();
            let objects = blob_store.object_store();
            let old_ts = Utc::now() - Duration::hours(2);

            // New shape, backdated by rewriting `session.json` in place.
            let new_shape = UploadSessionId::generate();
            blob_store
                .create_upload(&namespace, &new_shape, None)
                .await
                .unwrap();
            let record = format!(
                r#"{{"last_activity":"{}","committed_offset":0,"hash_state":""}}"#,
                old_ts.to_rfc3339()
            );
            objects
                .put(
                    &namespace.upload_session_path(&new_shape),
                    Bytes::from(record),
                )
                .await
                .unwrap();

            let executor = Executor::new_for_test(blob_store.clone(), test_case.metadata_store());
            sweep_upload_sessions(&blob_store, Duration::hours(1), &executor, 4)
                .await
                .unwrap();

            assert!(
                blob_store
                    .upload_summary(&namespace, &new_shape)
                    .await
                    .is_err(),
                "an aged session must be reaped"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn sweep_dry_run_captures_without_deleting() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let blob_store = test_case.blob_store();

            let session_id = UploadSessionId::generate();
            blob_store
                .create_upload(&namespace, &session_id, None)
                .await
                .unwrap();

            let sink: Mutex<Vec<Action>> = Mutex::new(Vec::new());
            sweep_upload_sessions(&blob_store, Duration::zero(), &sink, 4)
                .await
                .unwrap();

            assert!(
                sink.lock()
                    .unwrap()
                    .iter()
                    .any(|a| matches!(a, Action::DeleteExpiredUpload { .. })),
                "the capture sink must record the delete"
            );
            assert!(
                blob_store
                    .upload_summary(&namespace, &session_id)
                    .await
                    .is_ok(),
                "a capture sink must not delete"
            );
        })
        .await;
    }

    /// An `own` grant landed after the sweep's cutoff must survive the purge
    /// its old sibling entry triggers.
    #[tokio::test]
    async fn byteless_purge_keeps_young_sibling_own_key() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/byteless-own").unwrap();
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let ghost = Digest::sha256_of_bytes(b"byteless with fresh own");
            let stale = LinkKind::ReferencedBy(ghost.clone());
            metadata_store
                .insert_reference(&namespace, &ghost, &stale)
                .await
                .unwrap();
            // A two-second window puts the cutoff between the two puts: the
            // reference entry reads old, the later `own` grant young. The sleeps
            // keep both clear of it on second-granularity store timestamps.
            sleep(StdDuration::from_millis(3000)).await;
            metadata_store
                .insert_reference(&namespace, &ghost, &LinkKind::Blob(ghost.clone()))
                .await
                .unwrap();

            let sink: Mutex<Vec<Action>> = Mutex::new(Vec::new());
            let ctx = RefSweep {
                blob_store: &blob_store,
                metadata_store: &metadata_store,
                window_secs: 2,
                sink: &sink,
            };
            let walked = ghost.blob_ref_path(&namespace, &stale);
            sweep_one_ref(&ctx, &walked, &ghost, namespace.as_ref())
                .await
                .unwrap();

            let actions = sink.into_inner().unwrap();
            assert!(
                actions.iter().any(
                    |a| matches!(a, Action::RemoveBlobIndexLink { link, .. } if link == &stale)
                ),
                "the old byteless entry must still be purged"
            );
            assert!(
                !actions.iter().any(|a| matches!(
                    a,
                    Action::RemoveBlobIndexLink {
                        link: LinkKind::Blob(_),
                        ..
                    }
                )),
                "a young sibling `own` grant must survive the purge"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn byteless_reference_entries_are_purged_past_the_window() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/byteless").unwrap();
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            // An index entry whose blob bytes never landed.
            let ghost = Digest::sha256_of_bytes(b"bytes-never-landed");
            metadata_store
                .insert_reference(&namespace, &ghost, &LinkKind::Blob(ghost.clone()))
                .await
                .unwrap();

            let executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

            sweep_byteless_refs(
                &blob_store,
                &metadata_store,
                Duration::days(1),
                &executor,
                4,
            )
            .await
            .unwrap();
            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &ghost)
                    .await
                    .is_ok(),
                "a byteless entry within the window must be kept"
            );

            sweep_byteless_refs(&blob_store, &metadata_store, Duration::zero(), &executor, 4)
                .await
                .unwrap();
            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &ghost)
                    .await
                    .is_err(),
                "a byteless entry past the window must be purged"
            );
        })
        .await;
    }
}
