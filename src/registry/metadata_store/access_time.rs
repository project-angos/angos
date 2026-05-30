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
use tracing::warn;

use angos_tx_engine::{
    error::Error as TxError, executor::DEFAULT_RETRY_BUDGET, transaction::Mutation,
};

use crate::registry::{
    metadata_store::{
        Error, LinkMetadata, MetadataStore, link_kind::LinkKind, link_ops::tx_error_to_meta,
    },
    path_builder,
};

// ───────────────────────────────────────────────────────────────────────────
// Access-time write debouncing
// ───────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AccessTimeWriter {
    pub pending: Arc<Mutex<HashMap<String, (String, LinkKind)>>>,
}

impl AccessTimeWriter {
    pub fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn record(&self, namespace: &str, link: &LinkKind) {
        let key = path_builder::link_path(link, namespace);
        self.pending
            .lock()
            .await
            .insert(key, (namespace.to_string(), link.clone()));
    }

    pub async fn flush(&self, backend: &MetadataStore) {
        let entries: Vec<(String, LinkKind)> = {
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

impl MetadataStore {
    pub fn spawn_flush_task(
        flush_backend: MetadataStore,
        shutdown: Arc<AtomicBool>,
        interval: Duration,
    ) {
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

    /// Flush a single access-time update via a read-modify-write transaction.
    ///
    /// Reads the current link body, marks it accessed, and submits a
    /// `Transaction` with a content-hash read guard so a concurrent writer's
    /// fresher timestamp wins on conflict. Access times are advisory; a
    /// `Conflict` after the retry budget is silently discarded.
    pub async fn flush_one_access_time(
        &self,
        namespace: &str,
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
