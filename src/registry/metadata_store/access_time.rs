//! Access-time recording: every path that stamps a link's `accessed_at`
//! lives here.
//!
//! With a debounce configured, stamps are buffered in [`AccessTimeWriter`] and
//! flushed periodically; otherwise each read stamps inline through the store's
//! advisory update, which picks a conditional write or a read-modify-write
//! transaction below the storage API.

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
use tracing::{instrument, warn};

use angos_tx_engine::{
    error::Error as TxError, executor::DEFAULT_RETRY_BUDGET, store::Store, transaction::Mutation,
};

use crate::{
    oci::Namespace,
    registry::{
        Error,
        metadata_store::{LinkKind, LinkMetadata, MetadataStore},
        path_builder,
    },
};

// Build-time wiring

/// Access-time wiring decided at build time. A configured debounce gets the
/// buffering writer plus its background flush task; otherwise stamps are
/// applied inline and no writer is spun up.
pub fn build_writer(
    store: &Arc<Store>,
    debounce_secs: u64,
) -> (Option<AccessTimeWriter>, Option<Arc<FlushHandle>>) {
    if debounce_secs == 0 {
        return (None, None);
    }

    let writer = AccessTimeWriter::new();
    let shutdown = Arc::new(AtomicBool::new(false));
    spawn_flush_task(
        store.clone(),
        writer.clone(),
        shutdown.clone(),
        Duration::from_secs(debounce_secs),
    );

    (Some(writer), Some(Arc::new(FlushHandle::new(shutdown))))
}

fn spawn_flush_task(
    store: Arc<Store>,
    writer: AccessTimeWriter,
    shutdown: Arc<AtomicBool>,
    interval: Duration,
) {
    spawn(async move {
        loop {
            sleep(interval).await;
            writer.flush(&store).await;
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

    pub async fn flush(&self, store: &Store) {
        let entries: Vec<(Namespace, LinkKind)> = {
            let mut pending = self.pending.lock().await;
            pending.drain().map(|(_, v)| v).collect()
        };

        stream::iter(entries)
            .for_each_concurrent(10, |(namespace, link)| async move {
                if let Err(e) = flush_one_access_time(store, &namespace, &link).await {
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
    /// A configured debounce defers the stamp via the writer; otherwise the
    /// stamp is applied inline through the store's advisory update. The manifest
    /// pull path uses this when pull-time tracking is enabled.
    #[instrument(skip(self))]
    pub async fn read_link_recording_access(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let Some(writer) = &self.access_time_writer else {
            let link_data = self.stamp_link_access_time(namespace, link).await?;
            self.cache_put(namespace, link, &link_data).await;
            return Ok(link_data);
        };
        let link_data = self.read_link(namespace, link).await?;
        writer.record(namespace, link).await;
        Ok(link_data)
    }

    pub async fn flush_access_times(&self) {
        if let Some(writer) = &self.access_time_writer {
            writer.flush(self.store()).await;
        }
    }

    /// Stamp the access time inline through the store's advisory update, which
    /// picks a conditional write or a read-modify-write transaction. Access
    /// times are advisory, so a lost race is dropped as a no-op.
    async fn stamp_link_access_time(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);
        self.store()
            .update_advisory(&link_path, |body| {
                let link_data = serde_json::from_slice::<LinkMetadata>(&body)
                    .map_err(|e| TxError::Build(e.to_string()))?
                    .accessed();
                let serialized =
                    Bytes::from(serde_json::to_vec(&link_data).map_err(TxError::Serde)?);
                Ok((serialized, link_data))
            })
            .await
            .map_err(Error::from)
    }
}

/// Flush a single access-time update via a read-modify-write transaction.
///
/// A content-hash read guard lets a concurrent writer's fresher timestamp win
/// on conflict; access times are advisory, so a `Conflict` after the retry
/// budget is silently discarded.
async fn flush_one_access_time(
    store: &Store,
    namespace: &Namespace,
    link: &LinkKind,
) -> Result<(), Error> {
    let link_path = path_builder::link_path(link, namespace);
    let keys = [link_path.clone()];
    store
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
                    let link_data = serde_json::from_slice::<LinkMetadata>(&snap.body)
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
        .map_err(Error::from)
}
