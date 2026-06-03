use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use tracing::warn;

use angos_tx_engine::{
    StorageError,
    error::Error as TxError,
    executor::{DEFAULT_RETRY_BUDGET, execute_with_retry},
    transaction::{Mutation, Transaction},
};

use crate::registry::{
    metadata_store::{
        Error, MetadataStore,
        link_ops::tx_error_to_meta,
        sharded::{self, NamespaceRegistry},
    },
    path_builder,
};

impl MetadataStore {
    pub async fn read_namespace_registry(&self) -> Result<Option<NamespaceRegistry>, Error> {
        let shard_dir = path_builder::namespace_registry_shard_dir();
        let mut all_namespaces = Vec::new();
        let mut found_shards = false;
        let mut token = None;

        loop {
            let page = self
                .store()
                .list_children(&shard_dir, 1000, token, None)
                .await?;

            if !page.objects.is_empty() {
                found_shards = true;
            }

            let shard_results: Vec<Result<Vec<String>, Error>> =
                stream::iter(page.objects.into_iter().map(|obj| {
                    let shard_path = format!("{shard_dir}/{obj}");
                    async move {
                        match self.store().get(&shard_path).await {
                            Ok(data) => match serde_json::from_slice::<NamespaceRegistry>(&data) {
                                Ok(registry) => Ok(registry.namespaces),
                                Err(_) => Ok(Vec::new()),
                            },
                            Err(StorageError::NotFound) => Ok(Vec::new()),
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

            token = page.next_token;
            if token.is_none() {
                break;
            }
        }

        if !found_shards {
            let path = path_builder::namespace_registry_path();
            return match self.store().get(&path).await {
                Ok(data) => match serde_json::from_slice(&data) {
                    Ok(registry) => Ok(Some(registry)),
                    Err(e) => {
                        warn!("Corrupt namespace registry, will rebuild: {e}");
                        Ok(None)
                    }
                },
                Err(StorageError::NotFound) => Ok(None),
                Err(e) => Err(e.into()),
            };
        }

        let all_namespaces = sharded::normalize_namespaces(all_namespaces);
        Ok(Some(NamespaceRegistry {
            namespaces: all_namespaces,
        }))
    }

    pub async fn rebuild_namespace_registry(&self) -> Result<(), Error> {
        let repo_dir = path_builder::repository_dir();
        let namespaces = self.collect_namespaces(repo_dir, "").await?;
        let namespaces = sharded::normalize_namespaces(namespaces);
        let shards = sharded::group_namespaces_by_shard(namespaces);
        for (shard_key, shard_namespaces) in &shards {
            self.write_namespace_shard(shard_key, shard_namespaces)
                .await?;
        }
        Ok(())
    }

    async fn write_namespace_shard(
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

        execute_with_retry(
            self.executor(),
            || async {
                Ok(Transaction::builder()
                    .mutation(Mutation::Put {
                        key: path.clone(),
                        body: content.clone(),
                        expected: None,
                    })
                    .build())
            },
            DEFAULT_RETRY_BUDGET,
        )
        .await
        .map(|_| ())
        .map_err(tx_error_to_meta)
    }

    pub async fn register_namespace(&self, namespace: &str) -> Result<(), Error> {
        let path = path_builder::namespace_registry_shard_path(namespace);

        execute_with_retry(
            self.executor(),
            || async {
                let (raw, mut registry) = match self.store().get(&path).await {
                    Ok(data) => {
                        let raw = Bytes::from(data.clone());
                        let registry =
                            serde_json::from_slice::<NamespaceRegistry>(&data).unwrap_or_default();
                        (Some(raw), registry)
                    }
                    Err(StorageError::NotFound) => (None, NamespaceRegistry::default()),
                    Err(e) => {
                        return Err(TxError::Storage(e));
                    }
                };

                if !sharded::insert_sorted_unique(&mut registry.namespaces, namespace) {
                    return Ok(Transaction::builder().build());
                }

                let content = Bytes::from(serde_json::to_vec(&registry).map_err(TxError::Serde)?);
                let mut builder = Transaction::builder();
                if let Some(existing) = raw {
                    builder = builder.read(path.clone(), existing);
                }
                Ok(builder
                    .mutation(Mutation::Put {
                        key: path.clone(),
                        body: content,
                        expected: None,
                    })
                    .build())
            },
            DEFAULT_RETRY_BUDGET,
        )
        .await
        .map(|_| ())
        .map_err(tx_error_to_meta)
    }

    pub async fn collect_namespaces(
        &self,
        root_path: &str,
        root_prefix: &str,
    ) -> Result<Vec<String>, Error> {
        let mut stack: Vec<(String, String)> =
            vec![(root_path.to_string(), root_prefix.to_string())];
        let mut namespaces = Vec::new();

        while let Some((path, prefix)) = stack.pop() {
            let mut token = None;
            loop {
                let page = self.store().list_children(&path, 1000, token, None).await?;

                let mut children = Vec::new();
                for entry in &page.sub_prefixes {
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

                token = page.next_token;
                if token.is_none() {
                    break;
                }
            }
        }

        Ok(namespaces)
    }
}
