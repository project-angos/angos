//! Access-time recording: every path that stamps a link's `accessed_at`
//! lives here.
//!
//! CAS deployments stamp inline with a single conditional write whose lost
//! races are no-ops. Lock-coordinated deployments either buffer stamps in
//! [`AccessTimeWriter`] and flush them periodically (debounce configured) or
//! stamp inline with a read-modify-write transaction (debounce disabled).

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use tokio::{spawn, sync::Mutex, time::sleep};
use tracing::{info, instrument, warn};

use angos_tx_engine::{
    StorageError, error::Error as TxError, executor::DEFAULT_RETRY_BUDGET, store::Store,
    transaction::Mutation,
};

use crate::{
    oci::Namespace,
    registry::{
        metadata_store::{Error, LinkKind, LinkMetadata, MetadataStore, tx_error_to_meta},
        path_builder,
    },
};

// Build-time wiring

/// Access-time wiring decided at build time. CAS deployments stamp inline,
/// so no writer is spun up and a configured debounce is ignored;
/// lock-coordinated deployments with a debounce get the writer plus its
/// background flush task.
pub fn build_writer(
    store: &Arc<Store>,
    link_cache_ttl: u64,
    debounce_secs: u64,
) -> (Option<AccessTimeWriter>, Option<Arc<FlushHandle>>) {
    if store.cas_enabled() {
        if debounce_secs > 0 {
            info!("Access-time debounce ignored: CAS deployments stamp access times inline");
        }
        return (None, None);
    }
    if debounce_secs == 0 {
        return (None, None);
    }

    let writer = AccessTimeWriter::new();
    let shutdown = Arc::new(AtomicBool::new(false));
    let flush_backend = MetadataStore {
        store: store.clone(),
        cache: None,
        link_cache_ttl,
        access_time_writer: Some(writer.clone()),
        _flush_handle: None,
    };
    spawn_flush_task(
        flush_backend,
        shutdown.clone(),
        Duration::from_secs(debounce_secs),
    );

    (Some(writer), Some(Arc::new(FlushHandle::new(shutdown))))
}

fn spawn_flush_task(flush_backend: MetadataStore, shutdown: Arc<AtomicBool>, interval: Duration) {
    spawn(async move {
        loop {
            sleep(interval).await;
            flush_backend.flush_access_times().await;
            if shutdown.load(Ordering::Acquire) {
                return;
            }
        }
    });
}

// Access-time write debouncing (lock-coordinated deployments)

#[derive(Clone)]
pub struct AccessTimeWriter {
    pub pending: Arc<Mutex<HashMap<String, (Namespace, LinkKind)>>>,
}

impl AccessTimeWriter {
    pub fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn record(&self, namespace: &Namespace, link: &LinkKind) {
        let key = path_builder::link_path(link, namespace);
        self.pending
            .lock()
            .await
            .insert(key, (namespace.clone(), link.clone()));
    }

    pub async fn flush(&self, backend: &MetadataStore) {
        let entries: Vec<(Namespace, LinkKind)> = {
            let mut pending = self.pending.lock().await;
            pending.drain().map(|(_, v)| v).collect()
        };

        stream::iter(entries)
            .for_each_concurrent(10, |(namespace, link)| async move {
                if let Err(e) = backend.flush_one_access_time(&namespace, &link).await {
                    warn!("Failed to flush access time for {namespace}:{link}: {e}");
                }
            })
            .await;
    }
}

pub struct FlushHandle {
    shutdown: Arc<AtomicBool>,
}

impl FlushHandle {
    pub fn new(shutdown: Arc<AtomicBool>) -> Self {
        Self { shutdown }
    }
}

impl Drop for FlushHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

// Recording reads

impl MetadataStore {
    /// Like [`MetadataStore::read_link`] but records the link's access time.
    /// On CAS deployments the stamp is a single conditional write on the etag
    /// just read; otherwise it is deferred via the debounce writer when
    /// configured, else stamped inline with a read-modify-write transaction.
    /// The manifest pull path uses this when pull-time tracking is enabled.
    #[instrument(skip(self))]
    pub async fn read_link_recording_access(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        if self.store().cas_enabled() {
            let link_data = self.stamp_link_access_time(namespace, link).await?;
            self.cache_put(namespace, link, &link_data).await;
            return Ok(link_data);
        }
        let Some(writer) = &self.access_time_writer else {
            let link_data = self.update_link_access_time(namespace, link).await?;
            self.cache_put(namespace, link, &link_data).await;
            return Ok(link_data);
        };
        let link_data = self.read_link(namespace, link).await?;
        writer.record(namespace, link).await;
        Ok(link_data)
    }

    pub async fn flush_access_times(&self) {
        if let Some(writer) = &self.access_time_writer {
            writer.flush(self).await;
        }
    }

    /// Stamp the access time with one conditional write on the etag just
    /// read. Access times are advisory, so a concurrent writer winning the
    /// race drops this stamp as a no-op; there is nothing to retry.
    async fn stamp_link_access_time(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);
        self.store()
            .update_advisory(&link_path, |body| {
                let link_data = LinkMetadata::from_bytes(body.to_vec())
                    .map_err(|e| TxError::Build(e.to_string()))?
                    .accessed();
                let serialized =
                    Bytes::from(serde_json::to_vec(&link_data).map_err(TxError::Serde)?);
                Ok((serialized, link_data))
            })
            .await
            .map_err(tx_error_to_meta)
    }

    /// Mark the access time for `link` in `namespace` using a read-modify-write
    /// transaction, returning the updated [`LinkMetadata`]. Concurrent updaters
    /// resolve via content-hash conflict detection (last writer wins), which is
    /// fine for advisory access-time stamps.
    async fn update_link_access_time(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);
        let keys = [link_path.clone()];

        let (_, link_data) = self
            .store()
            .update_with_payload(
                &keys,
                |snaps| {
                    let link_path = link_path.clone();
                    async move {
                        let snap = &snaps[0];
                        if !snap.present {
                            return Err(TxError::Storage(StorageError::NotFound));
                        }
                        let link_data = LinkMetadata::from_bytes(snap.body.to_vec())
                            .map_err(|e| TxError::Build(e.to_string()))?
                            .accessed();
                        let serialized =
                            Bytes::from(serde_json::to_vec(&link_data).map_err(TxError::Serde)?);
                        Ok((
                            vec![Mutation::Put {
                                key: link_path,
                                body: serialized,
                                expected: None,
                            }],
                            link_data,
                        ))
                    }
                },
                DEFAULT_RETRY_BUDGET,
            )
            .await
            .map_err(tx_error_to_meta)?;

        Ok(link_data)
    }

    /// Flush a single access-time update via a read-modify-write transaction.
    ///
    /// A content-hash read guard lets a concurrent writer's fresher timestamp win
    /// on conflict; access times are advisory, so a `Conflict` after the retry
    /// budget is silently discarded.
    async fn flush_one_access_time(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
    ) -> Result<(), Error> {
        let link_path = path_builder::link_path(link, namespace);
        let keys = [link_path.clone()];
        self.store()
            .update(
                &keys,
                |snaps| {
                    let link_path = link_path.clone();
                    async move {
                        let snap = &snaps[0];
                        // Link vanished: nothing to stamp. An empty mutation set
                        // commits a no-op transaction.
                        if !snap.present {
                            return Ok(Vec::new());
                        }
                        let link_data = LinkMetadata::from_bytes(snap.body.to_vec())
                            .map_err(|e| TxError::Build(e.to_string()))?
                            .accessed();
                        let serialized =
                            Bytes::from(serde_json::to_vec(&link_data).map_err(TxError::Serde)?);
                        Ok(vec![Mutation::Put {
                            key: link_path,
                            body: serialized,
                            expected: None,
                        }])
                    }
                },
                DEFAULT_RETRY_BUDGET,
            )
            .await
            .map(|_| ())
            .map_err(tx_error_to_meta)
    }
}
