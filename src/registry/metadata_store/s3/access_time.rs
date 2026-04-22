use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use futures_util::stream::{self, StreamExt};
use tokio::sync::Mutex;
use tracing::warn;

use super::Backend;
use crate::registry::{
    metadata_store::{Error, LinkMetadata, link_kind::LinkKind, lock_ops::LockOps},
    path_builder,
};

#[derive(Clone)]
pub(super) struct AccessTimeWriter {
    pub(super) pending: Arc<Mutex<HashMap<String, (String, LinkKind)>>>,
}

impl AccessTimeWriter {
    pub(super) fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(super) async fn record(&self, namespace: &str, link: &LinkKind) {
        let key = format!("{namespace}:{link}");
        self.pending
            .lock()
            .await
            .insert(key, (namespace.to_string(), link.clone()));
    }

    pub(super) async fn flush(&self, backend: &Backend) {
        let entries: Vec<(String, LinkKind)> = {
            let mut pending = self.pending.lock().await;
            pending.drain().map(|(_, v)| v).collect()
        };

        stream::iter(entries)
            .for_each_concurrent(10, |(namespace, link)| async move {
                if let Err(e) = Self::flush_one(backend, &namespace, &link).await {
                    warn!("Failed to flush access time for {namespace}:{link}: {e}");
                }
            })
            .await;
    }

    async fn flush_one(backend: &Backend, namespace: &str, link: &LinkKind) -> Result<(), Error> {
        if backend.conditional.put_if_match {
            let link_path = path_builder::link_path(link, namespace);
            let (data, etag) = backend.store.read_with_etag(&link_path).await?;
            let link_data = LinkMetadata::from_bytes(data)?.accessed();
            if let Some(etag) = etag {
                let content = serde_json::to_vec(&link_data)
                    .map_err(|e| Error::InvalidData(e.to_string()))?;
                backend
                    .store
                    .put_object_if_match(&link_path, &etag, content)
                    .await?;
            }
            return Ok(());
        }
        let guard = backend
            .lock
            .acquire(&[format!("{namespace}:{link}")])
            .await?;
        let link_data = backend
            .read_link_reference(namespace, link)
            .await?
            .accessed();
        if !guard.is_valid() {
            return Err(Error::Lock(
                "lock invalidated during access time flush".into(),
            ));
        }
        backend
            .write_link_reference(namespace, link, &link_data)
            .await?;
        guard.release().await;
        Ok(())
    }
}

pub(super) struct FlushHandle {
    pub(super) shutdown: Arc<AtomicBool>,
}

impl Drop for FlushHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}
