use std::{collections::HashMap, io::ErrorKind, time::Duration};

use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use tracing::{debug, warn};

use super::Backend;
use crate::registry::{
    data_store,
    metadata_store::{Error, simple_jitter},
    path_builder,
};

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct NamespaceRegistry {
    pub namespaces: Vec<String>,
}

impl Backend {
    pub async fn read_namespace_registry(&self) -> Result<Option<NamespaceRegistry>, Error> {
        // Read sharded format: list all shard files under _registry/ns/ and merge
        let shard_dir = path_builder::namespace_registry_shard_dir();
        let mut all_namespaces = Vec::new();
        let mut found_shards = false;
        let mut continuation_token = None;

        loop {
            let (_, objects, next_token) = self
                .store
                .list_prefixes(&shard_dir, "/", 1000, continuation_token, None)
                .await?;

            if !objects.is_empty() {
                found_shards = true;
            }

            let shard_results: Vec<Result<Vec<String>, Error>> =
                stream::iter(objects.into_iter().map(|obj| {
                    let shard_path = format!("{shard_dir}/{obj}");
                    async move {
                        match self.store.read(&shard_path).await {
                            Ok(data) => match serde_json::from_slice::<NamespaceRegistry>(&data) {
                                Ok(registry) => Ok(registry.namespaces),
                                Err(_) => Ok(Vec::new()),
                            },
                            Err(e) if e.kind() == ErrorKind::NotFound => Ok(Vec::new()),
                            Err(e) => Err(Error::from(e)),
                        }
                    }
                }))
                .buffer_unordered(10)
                .collect()
                .await;

            for result in shard_results {
                all_namespaces.extend(result?);
            }

            continuation_token = next_token;
            if continuation_token.is_none() {
                break;
            }
        }

        if !found_shards {
            // Fall back to legacy single-file format for backward compatibility
            let path = path_builder::namespace_registry_path();
            return match self.store.read(&path).await {
                Ok(data) => match serde_json::from_slice(&data) {
                    Ok(registry) => Ok(Some(registry)),
                    Err(e) => {
                        warn!("Corrupt namespace registry, will rebuild: {e}");
                        Ok(None)
                    }
                },
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                Err(e) => Err(Error::from(e)),
            };
        }

        all_namespaces.sort();
        all_namespaces.dedup();
        Ok(Some(NamespaceRegistry {
            namespaces: all_namespaces,
        }))
    }

    pub async fn rebuild_namespace_registry(&self) -> Result<(), Error> {
        let repo_dir = path_builder::repository_dir();
        let mut namespaces = Vec::new();
        self.collect_namespaces(repo_dir, "", &mut namespaces)
            .await?;
        namespaces.sort();
        namespaces.dedup();

        // Group namespaces by shard key
        let mut shards: HashMap<String, Vec<String>> = HashMap::new();
        for ns in &namespaces {
            let key = path_builder::namespace_shard_key(ns);
            shards.entry(key).or_default().push(ns.clone());
        }

        // Write each shard
        for (shard_key, shard_namespaces) in &shards {
            self.process_namespace_shard(shard_key, shard_namespaces)
                .await?;
        }

        Ok(())
    }

    pub async fn process_namespace_shard(
        &self,
        shard_key: &str,
        shard_namespaces: &[String],
    ) -> Result<(), Error> {
        let registry = NamespaceRegistry {
            namespaces: shard_namespaces.to_vec(),
        };
        let content = Bytes::from(serde_json::to_vec(&registry)?);
        let path = format!(
            "{}/{shard_key}.json",
            path_builder::namespace_registry_shard_dir()
        );

        if self.conditional.supports_cas() {
            let etag = match self.store.read_with_etag(&path).await {
                Ok((_, etag)) => etag,
                Err(e) if e.kind() == ErrorKind::NotFound => None,
                Err(e) => return Err(Error::from(e)),
            };
            let write_result = if let Some(ref etag) = etag {
                self.store
                    .put_object_if_match(&path, etag, content)
                    .await
                    .map(|_| ())
            } else {
                self.store
                    .put_object_if_not_exists(&path, content)
                    .await
                    .map(|_| ())
            };
            match write_result {
                Ok(()) => {}
                Err(data_store::Error::PreconditionFailed) => {
                    debug!(
                        shard_key,
                        "Namespace registry shard rebuild lost CAS race; concurrent write wins"
                    );
                }
                Err(e) => return Err(Error::StorageBackend(e.to_string())),
            }
        } else {
            let lock_key = format!("namespace_registry_shard_{shard_key}");
            let guard = self.lock.acquire(&[lock_key]).await?;
            self.store.put_object(&path, content).await?;
            guard.release().await;
        }

        Ok(())
    }

    pub async fn register_namespace(&self, namespace: &str) -> Result<(), Error> {
        if self.known_namespaces.lock().await.contains(namespace) {
            return Ok(());
        }

        if self.conditional.supports_cas() {
            self.register_namespace_cas(namespace).await
        } else {
            self.register_namespace_unconditional(namespace).await
        }
    }

    async fn register_namespace_unconditional(&self, namespace: &str) -> Result<(), Error> {
        let shard_key = path_builder::namespace_shard_key(namespace);
        let path = path_builder::namespace_registry_shard_path(namespace);
        let lock_key = format!("namespace_registry_shard_{shard_key}");
        let guard = self.lock.acquire(&[lock_key]).await?;

        let mut registry = match self.store.read(&path).await {
            Ok(data) => serde_json::from_slice::<NamespaceRegistry>(&data).unwrap_or_default(),
            Err(e) if e.kind() == ErrorKind::NotFound => NamespaceRegistry::default(),
            Err(e) => {
                guard.release().await;
                return Err(Error::from(e));
            }
        };

        if let Err(pos) = registry.namespaces.binary_search(&namespace.to_string()) {
            registry.namespaces.insert(pos, namespace.to_string());
            let content = Bytes::from(serde_json::to_vec(&registry)?);
            if let Err(e) = self.store.put_object(&path, content).await {
                guard.release().await;
                return Err(e.into());
            }
        }

        guard.release().await;
        self.known_namespaces
            .lock()
            .await
            .insert(namespace.to_string());
        Ok(())
    }

    async fn register_namespace_cas(&self, namespace: &str) -> Result<(), Error> {
        let path = path_builder::namespace_registry_shard_path(namespace);

        for attempt in 0..super::MAX_BLOB_INDEX_CAS_RETRIES {
            let (mut registry, etag) = match self.store.read_with_etag(&path).await {
                Ok((data, etag)) => match serde_json::from_slice::<NamespaceRegistry>(&data) {
                    Ok(r) => (r, etag),
                    Err(_) => (NamespaceRegistry::default(), etag),
                },
                Err(e) if e.kind() == ErrorKind::NotFound => (NamespaceRegistry::default(), None),
                Err(e) => return Err(Error::from(e)),
            };

            let Err(pos) = registry.namespaces.binary_search(&namespace.to_string()) else {
                self.known_namespaces
                    .lock()
                    .await
                    .insert(namespace.to_string());
                return Ok(());
            };
            registry.namespaces.insert(pos, namespace.to_string());

            let content = Bytes::from(serde_json::to_vec(&registry)?);
            let write_result = if let Some(ref etag) = etag {
                self.store
                    .put_object_if_match(&path, etag, content)
                    .await
                    .map(|_| ())
            } else {
                self.store
                    .put_object_if_not_exists(&path, content)
                    .await
                    .map(|_| ())
            };

            match write_result {
                Ok(()) => {
                    self.known_namespaces
                        .lock()
                        .await
                        .insert(namespace.to_string());
                    return Ok(());
                }
                Err(data_store::Error::PreconditionFailed) => {
                    debug!(
                        namespace,
                        attempt, "Namespace registry shard CAS conflict, retrying"
                    );
                    let max_ms = 50u64.saturating_mul(1u64 << attempt.min(4));
                    tokio::time::sleep(Duration::from_millis(simple_jitter(max_ms))).await;
                }
                Err(e) => return Err(Error::StorageBackend(e.to_string())),
            }
        }

        warn!(
            namespace,
            "Namespace registry shard CAS retries exhausted; namespace will appear after scrub reconciles"
        );
        Ok(())
    }

    pub async fn collect_namespaces(
        &self,
        path: &str,
        prefix: &str,
        namespaces: &mut Vec<String>,
    ) -> Result<(), Error> {
        let mut continuation_token = None;
        loop {
            let (prefixes, _, next_token) = self
                .store
                .list_prefixes(path, "/", 1000, continuation_token, None)
                .await?;

            for entry in &prefixes {
                if entry.starts_with('_') {
                    if entry == "_manifests" {
                        let namespace = prefix.strip_suffix('/').unwrap_or(prefix);
                        if !namespace.is_empty() {
                            namespaces.push(namespace.to_string());
                        }
                    }
                    continue;
                }

                let child_path = format!("{path}/{entry}");
                let child_prefix = format!("{prefix}{entry}/");
                Box::pin(self.collect_namespaces(&child_path, &child_prefix, namespaces)).await?;
            }

            continuation_token = next_token;
            if continuation_token.is_none() {
                break;
            }
        }
        Ok(())
    }
}
