use crate::registry::metadata_store::{Error, MetadataStore};

/// Prefix of the pre-1.3 maintained namespace-registry index. The catalog is now
/// derived from content, so these objects are dead and are pruned by scrub.
const LEGACY_NAMESPACE_REGISTRY_PREFIX: &str = "_registry";

impl MetadataStore {
    /// Walk the repository tree under `root_path` and yield every path that is a
    /// namespace. A path is a namespace if and only if it has a `_manifests`
    /// child, which implies it holds at least one revision or tag (an
    /// `_uploads`-only path is not a namespace and is skipped). `_`-prefixed
    /// children are never descended into, so manifest/upload/blob substructure
    /// is not mistaken for nested namespaces.
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
            let mut is_namespace = false;
            let mut children = Vec::new();
            loop {
                let page = self.store().list_children(&path, 1000, token, None).await?;

                for entry in &page.sub_prefixes {
                    if entry == "_manifests" {
                        is_namespace = true;
                        continue;
                    }
                    if entry.starts_with('_') {
                        continue;
                    }
                    let child_path = format!("{path}/{entry}");
                    let child_prefix = format!("{prefix}{entry}/");
                    children.push((child_path, child_prefix));
                }

                token = page.next_token;
                if token.is_none() {
                    break;
                }
            }

            if is_namespace {
                let namespace = prefix.strip_suffix('/').unwrap_or(&prefix);
                if !namespace.is_empty() {
                    namespaces.push(namespace.to_string());
                }
            }
            for child in children.into_iter().rev() {
                stack.push(child);
            }
        }

        Ok(namespaces)
    }

    /// Delete the dead pre-1.3 namespace-registry objects (`_registry/`).
    /// Idempotent: a no-op once the prefix is gone.
    pub async fn delete_legacy_namespace_registry(&self) -> Result<(), Error> {
        self.store()
            .delete_prefix(LEGACY_NAMESPACE_REGISTRY_PREFIX)
            .await
            .map_err(Error::from)
    }
}
