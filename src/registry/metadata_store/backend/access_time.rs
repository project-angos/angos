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

use angos_storage::{ConditionalStore, Error as StorageError};

use crate::registry::{
    metadata_store::{
        Error, LinkMetadata, MetadataStore,
        backend::Backend,
        link_kind::LinkKind,
        lock_ops::{LockOps, link_lock_key, with_validated_lock},
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
        let key = link_lock_key(namespace, link);
        self.pending
            .lock()
            .await
            .insert(key, (namespace.to_string(), link.clone()));
    }

    pub async fn flush(&self, backend: &Backend) {
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

impl Backend {
    pub fn spawn_flush_task(flush_backend: Backend, shutdown: Arc<AtomicBool>, interval: Duration) {
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

    /// CAS-optimistic access-time read: reads the link with its `ETag`, then
    /// spawns a background task to write the `accessed_at` bump back. The
    /// spawned write is best-effort — a racing writer winning the CAS is
    /// acceptable because access times are advisory.
    pub async fn read_and_spawn_access_time_update(
        &self,
        namespace: &str,
        link: &LinkKind,
        cs: Arc<dyn ConditionalStore>,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);
        let (data, etag) = match cs.get_with_etag(&link_path).await {
            Ok(result) => result,
            Err(StorageError::NotFound) => return Err(Error::ReferenceNotFound),
            Err(e) => return Err(e.into()),
        };
        let link_data = LinkMetadata::from_bytes(data)?;

        if let Some(etag) = etag {
            let updated = link_data.clone().accessed();
            spawn(async move {
                let serialized = match serde_json::to_vec(&updated) {
                    Ok(v) => Bytes::from(v),
                    Err(_) => return,
                };
                let _ = cs.put_if_match(&link_path, &etag, serialized).await;
            });
        }

        Ok(link_data)
    }

    pub async fn flush_one_access_time(
        &self,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<(), Error> {
        let link_path = path_builder::link_path(link, namespace);

        if let Some(cs) = self.coordinator.conditional_store() {
            let (data, etag) = cs.get_with_etag(&link_path).await?;
            let link_data = LinkMetadata::from_bytes(data)?.accessed();
            if let Some(etag) = etag {
                let content = serde_json::to_vec(&link_data)
                    .map_err(|e| Error::InvalidData(e.to_string()))?;
                cs.put_if_match(&link_path, &etag, Bytes::from(content))
                    .await
                    .map_err(Error::from)?;
            }
        } else {
            let lock_keys = [link_lock_key(namespace, link)];
            with_validated_lock(
                self.lock(),
                &lock_keys,
                "lock invalidated during access time flush",
                || async {
                    let link_data = self.read_link_reference(namespace, link).await?.accessed();
                    self.write_link_reference(namespace, link, &link_data).await
                },
            )
            .await?;
        }

        Ok(())
    }
}
