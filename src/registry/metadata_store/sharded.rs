use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::registry::{
    metadata_store::{BlobIndex, BlobIndexOperation, Error, link_kind::LinkKind},
    path_builder,
};

pub const SHARD_READ_CONCURRENCY: usize = 10;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NamespaceRegistry {
    pub namespaces: Vec<String>,
}

pub fn decode_blob_index_shard_namespace(file_name: &str) -> String {
    file_name
        .strip_suffix(".json")
        .unwrap_or(file_name)
        .replace("%2F", "/")
        .replace("%25", "%")
}

pub fn normalize_namespaces(mut namespaces: Vec<String>) -> Vec<String> {
    namespaces.sort();
    namespaces.dedup();
    namespaces
}

pub fn group_namespaces_by_shard(
    namespaces: impl IntoIterator<Item = String>,
) -> HashMap<String, Vec<String>> {
    let mut shards: HashMap<String, Vec<String>> = HashMap::new();
    for namespace in namespaces {
        shards
            .entry(path_builder::namespace_shard_key(&namespace))
            .or_default()
            .push(namespace);
    }

    for namespaces in shards.values_mut() {
        namespaces.sort();
        namespaces.dedup();
    }
    shards
}

pub fn registry_for_namespaces(namespaces: &[String]) -> NamespaceRegistry {
    NamespaceRegistry {
        namespaces: namespaces.to_vec(),
    }
}

pub fn insert_sorted_unique(namespaces: &mut Vec<String>, namespace: &str) -> bool {
    match namespaces.binary_search_by(|probe| probe.as_str().cmp(namespace)) {
        Ok(_) => false,
        Err(pos) => {
            namespaces.insert(pos, namespace.to_string());
            true
        }
    }
}

pub fn collect_blob_index_shards(
    shards: impl IntoIterator<Item = (String, HashSet<LinkKind>)>,
) -> BlobIndex {
    let mut index = BlobIndex::default();
    for (namespace, links) in shards {
        if !links.is_empty() {
            index.namespace.insert(namespace, links);
        }
    }
    index
}

pub fn apply_blob_index_operations(
    links: &mut HashSet<LinkKind>,
    operations: &[BlobIndexOperation],
) {
    for operation in operations {
        match operation {
            BlobIndexOperation::Insert(link) => {
                links.insert(link.clone());
            }
            BlobIndexOperation::Remove(link) => {
                links.remove(link);
            }
        }
    }
}

pub fn non_empty_links_or_not_found(links: HashSet<LinkKind>) -> Result<HashSet<LinkKind>, Error> {
    if links.is_empty() {
        Err(Error::ReferenceNotFound)
    } else {
        Ok(links)
    }
}

pub fn namespace_links_from_index(
    index: &BlobIndex,
    namespace: &str,
) -> Result<HashSet<LinkKind>, Error> {
    index
        .namespace
        .get(namespace)
        .cloned()
        .filter(|links| !links.is_empty())
        .ok_or(Error::ReferenceNotFound)
}
