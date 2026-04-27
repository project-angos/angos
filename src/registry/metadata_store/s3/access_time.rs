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
use crate::registry::metadata_store::link_kind::LinkKind;

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
                if let Err(e) = backend
                    .coordinator
                    .flush_access_time(backend, &namespace, &link)
                    .await
                {
                    warn!("Failed to flush access time for {namespace}:{link}: {e}");
                }
            })
            .await;
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
