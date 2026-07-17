//! The `-u` window sweeps: upload-lifecycle reclamation driven by `angos
//! prune`. Upload sessions, orphan S3 multiparts, and byteless blob-index
//! shard entries (a grant written by an upload whose bytes never landed).
//! Grant-only blob ownership is a retention subject instead; see
//! `checker::sweep_orphan_grants`.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use futures_util::{StreamExt, TryStreamExt};
use tracing::{debug, error, info, warn};

use angos_tx_engine::StorageError;

use crate::{
    command::scrub::{
        Error,
        action::Action,
        categorize::{KeyCategory, categorize},
        check::list_all,
        executor::ActionSink,
        walk,
    },
    oci::{Digest, Namespace},
    registry::{
        Error as RegistryError,
        blob_store::{BlobStore, MultipartCleanup, UploadSummary},
        metadata_store::MetadataStore,
        path_builder,
    },
};

enum UploadVerdict {
    /// Upload state is broken (missing summary or corrupted data): delete.
    DeleteInconsistent,
    /// Upload age exceeds the window: delete.
    DeleteObsolete,
    /// Upload is healthy or the failure was transient: leave alone.
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
        Err(
            RegistryError::BlobUploadUnknown | RegistryError::NotFound | RegistryError::BlobUnknown,
        ) => UploadVerdict::DeleteInconsistent,
        // A corrupt session record and a transient backend read both collapse
        // to `Internal`; err on the safe side and keep the upload rather than
        // risk deleting one on a transient error.
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
    let mut namespaces = list_all::upload_namespaces(blob_store);
    while let Some(namespace) = namespaces.next().await {
        let namespace = namespace?;
        let Ok(namespace) = Namespace::new(&namespace) else {
            // Invalid upload namespaces are scrub's concern.
            continue;
        };
        let namespace = &namespace;
        list_all::uploads(blob_store, namespace)
            .try_for_each_concurrent(concurrency, |uuid| async move {
                if let Err(e) = sweep_one_upload(blob_store, namespace, &uuid, window, sink).await {
                    error!("prune: failed to check upload '{namespace}/{uuid}': {e}");
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
    uuid: &str,
    window: Duration,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    let summary = blob_store.upload_summary(namespace, uuid).await;
    match classify_upload(summary.as_ref(), window, Utc::now()) {
        UploadVerdict::DeleteInconsistent | UploadVerdict::DeleteObsolete => {
            debug!("prune: reaping upload '{namespace}/{uuid}'");
            sink.apply(Action::DeleteExpiredUpload {
                namespace: namespace.clone(),
                uuid: uuid.to_string(),
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
    cleanup: &(dyn MultipartCleanup + Send + Sync),
    window: Duration,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    let orphans = cleanup
        .list_orphan_multipart_uploads(window)
        .await
        .map_err(Error::from)?;
    let count = orphans.len();
    for orphan in orphans {
        sink.apply(Action::AbortMultipartUpload {
            key: orphan.key,
            upload_id: orphan.upload_id,
        })
        .await?;
    }
    info!("prune: found {count} orphan multipart upload(s)");
    Ok(())
}

/// Remove blob-index entries that reference byteless blobs once the shard is
/// older than the window: a grant written by an upload whose bytes never
/// landed (or whose bytes were reclaimed out-of-band). A pull-through grant
/// whose lazy fill never completed is purged too; the next pull re-fills the
/// bytes and re-grants.
pub async fn sweep_byteless_shards(
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    window: Duration,
    sink: &dyn ActionSink,
    concurrency: usize,
) -> Result<(), Error> {
    let now = Utc::now();
    let objects = metadata_store.store().object_store();
    walk::for_each_key(
        objects,
        path_builder::blobs_root_dir(),
        concurrency,
        |key| async move {
            let KeyCategory::BlobIndexShard { digest, namespace } = categorize(&key) else {
                return;
            };
            if let Err(e) = sweep_one_shard(
                blob_store,
                metadata_store,
                &key,
                &digest,
                &namespace,
                window,
                now,
                sink,
            )
            .await
            {
                error!("prune: failed to check shard '{key}': {e}");
            }
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn sweep_one_shard(
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    key: &str,
    blob: &Digest,
    namespace_raw: &str,
    window: Duration,
    now: DateTime<Utc>,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    match blob_store.size(blob).await {
        Ok(_) => return Ok(()),
        Err(RegistryError::BlobUnknown | RegistryError::NotFound) => {}
        Err(e) => return Err(e.into()),
    }
    let Ok(namespace) = Namespace::new(namespace_raw) else {
        return Ok(());
    };
    let meta = match metadata_store.store().object_store().head(key).await {
        Ok(meta) => meta,
        Err(StorageError::NotFound) => return Ok(()),
        Err(e) => return Err(RegistryError::from(e).into()),
    };
    let Some(last_modified) = meta.last_modified else {
        // No timestamp to gate on: keep, rather than risk racing an upload
        // that granted before its bytes landed.
        return Ok(());
    };
    if now.signed_duration_since(last_modified) < window {
        return Ok(());
    }

    warn!("prune: purging index entries for byteless blob '{blob}' in '{namespace}'");
    let links = metadata_store
        .read_blob_index_namespace(&namespace, blob)
        .await?;
    for link in links {
        sink.apply(Action::RemoveBlobIndexLink {
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

    use super::*;

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

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    use async_trait::async_trait;

    use crate::{
        command::scrub::executor::Executor,
        oci::{Digest, Namespace},
        registry::{
            blob_store::OrphanMultipartUpload,
            metadata_store::{BlobIndexOperation, LinkKind},
            test_utils::for_each_backend,
        },
    };

    #[tokio::test]
    async fn sweep_reaps_obsolete_upload_and_keeps_recent() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let blob_store = test_case.blob_store();

            let old_uuid = uuid::Uuid::new_v4().to_string();
            blob_store
                .create_upload(&namespace, &old_uuid)
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

            let fresh_uuid = uuid::Uuid::new_v4().to_string();
            blob_store
                .create_upload(&namespace, &fresh_uuid)
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

    #[tokio::test]
    async fn sweep_dry_run_captures_without_deleting() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let blob_store = test_case.blob_store();

            let uuid = uuid::Uuid::new_v4().to_string();
            blob_store.create_upload(&namespace, &uuid).await.unwrap();

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
                blob_store.upload_summary(&namespace, &uuid).await.is_ok(),
                "a capture sink must not delete"
            );
        })
        .await;
    }

    struct SpyCleanup {
        list_called_timeout_secs: AtomicI64,
        abort_call_count: AtomicUsize,
        orphans: Vec<String>,
    }

    impl SpyCleanup {
        fn new(orphan_keys: Vec<&str>) -> Self {
            Self {
                list_called_timeout_secs: AtomicI64::new(-1),
                abort_call_count: AtomicUsize::new(0),
                orphans: orphan_keys.into_iter().map(str::to_owned).collect(),
            }
        }
    }

    #[async_trait]
    impl MultipartCleanup for SpyCleanup {
        async fn list_orphan_multipart_uploads(
            &self,
            timeout: Duration,
        ) -> Result<Vec<OrphanMultipartUpload>, RegistryError> {
            self.list_called_timeout_secs
                .store(timeout.num_seconds(), Ordering::SeqCst);
            Ok(self
                .orphans
                .iter()
                .map(|k| OrphanMultipartUpload {
                    key: k.clone(),
                    upload_id: "spy-upload-id".to_string(),
                })
                .collect())
        }

        async fn abort_orphan_multipart_upload(
            &self,
            _upload: &OrphanMultipartUpload,
        ) -> Result<(), RegistryError> {
            self.abort_call_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn multipart_sweep_forwards_timeout_and_captures_orphans() {
        let spy = SpyCleanup::new(vec!["ns/_uploads/uuid1/data", "ns/_uploads/uuid2/data"]);
        let sink: Mutex<Vec<Action>> = Mutex::new(Vec::new());

        sweep_orphan_multiparts(&spy, Duration::hours(2), &sink)
            .await
            .unwrap();

        assert_eq!(
            spy.list_called_timeout_secs.load(Ordering::SeqCst),
            7200,
            "timeout forwarded as 2 h = 7200 s"
        );
        assert_eq!(
            spy.abort_call_count.load(Ordering::SeqCst),
            0,
            "a capture sink must not invoke abort"
        );
        let sink = sink.into_inner().unwrap();
        assert_eq!(sink.len(), 2, "two orphans produce two actions");
        assert!(
            sink.iter()
                .all(|a| matches!(a, Action::AbortMultipartUpload { .. }))
        );
    }

    #[tokio::test]
    async fn byteless_shard_entries_are_purged_past_the_window() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/byteless").unwrap();
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            // An index entry whose blob bytes never landed.
            let ghost = Digest::sha256_of_bytes(b"bytes-never-landed");
            metadata_store
                .update_blob_index(
                    &namespace,
                    &ghost,
                    BlobIndexOperation::Insert(LinkKind::Blob(ghost.clone())),
                )
                .await
                .unwrap();

            let executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

            sweep_byteless_shards(
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

            sweep_byteless_shards(&blob_store, &metadata_store, Duration::zero(), &executor, 4)
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
