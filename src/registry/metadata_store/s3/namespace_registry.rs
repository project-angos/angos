use std::{collections::HashMap, io::ErrorKind};

use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use tracing::warn;

use super::Backend;
use crate::registry::{metadata_store::Error, path_builder};

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

        self.coordinator
            .rebuild_namespace_registry_shard(self, shard_key, &path, content)
            .await
    }

    pub async fn register_namespace(&self, namespace: &str) -> Result<(), Error> {
        if self.known_namespaces.lock().await.contains(namespace) {
            return Ok(());
        }

        self.coordinator.register_namespace(self, namespace).await
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
