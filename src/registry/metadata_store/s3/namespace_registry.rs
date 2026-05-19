use std::io::ErrorKind;

use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use tracing::warn;

use super::Backend;
use crate::registry::{
    metadata_store::{Error, sharded},
    path_builder,
};

impl Backend {
    pub async fn read_namespace_registry(
        &self,
    ) -> Result<Option<sharded::NamespaceRegistry>, Error> {
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
                            Ok(data) => {
                                match serde_json::from_slice::<sharded::NamespaceRegistry>(&data) {
                                    Ok(registry) => Ok(registry.namespaces),
                                    Err(_) => Ok(Vec::new()),
                                }
                            }
                            Err(e) if e.kind() == ErrorKind::NotFound => Ok(Vec::new()),
                            Err(e) => Err(Error::from(e)),
                        }
                    }
                }))
                .buffer_unordered(sharded::SHARD_READ_CONCURRENCY)
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

        let all_namespaces = sharded::normalize_namespaces(all_namespaces);
        Ok(Some(sharded::NamespaceRegistry {
            namespaces: all_namespaces,
        }))
    }

    pub async fn rebuild_namespace_registry(&self) -> Result<(), Error> {
        let repo_dir = path_builder::repository_dir();
        let namespaces = self.collect_namespaces(repo_dir, "").await?;
        let namespaces = sharded::normalize_namespaces(namespaces);

        let shards = sharded::group_namespaces_by_shard(namespaces);

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
        let registry = sharded::registry_for_namespaces(shard_namespaces);
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

    async fn collect_namespaces(
        &self,
        root_path: &str,
        root_prefix: &str,
    ) -> Result<Vec<String>, Error> {
        // Children are pushed in reverse so that pop() yields them in original iteration order,
        // preserving depth-first pre-order traversal.
        let mut stack: Vec<(String, String)> =
            vec![(root_path.to_string(), root_prefix.to_string())];
        let mut namespaces = Vec::new();

        while let Some((path, prefix)) = stack.pop() {
            let mut continuation_token = None;
            loop {
                let (prefixes, _, next_token) = self
                    .store
                    .list_prefixes(&path, "/", 1000, continuation_token, None)
                    .await?;

                let mut children = Vec::new();
                for entry in &prefixes {
                    if entry.starts_with('_') {
                        let namespace = prefix.strip_suffix('/').unwrap_or(&prefix);
                        if !namespace.is_empty() {
                            namespaces.push(namespace.to_string());
                        }
                        continue;
                    }

                    let child_path = format!("{path}/{entry}");
                    let child_prefix = format!("{prefix}{entry}/");
                    children.push((child_path, child_prefix));
                }

                for child in children.into_iter().rev() {
                    stack.push(child);
                }

                continuation_token = next_token;
                if continuation_token.is_none() {
                    break;
                }
            }
        }

        Ok(namespaces)
    }
}
