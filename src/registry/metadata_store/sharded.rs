use std::collections::HashSet;

use crate::registry::metadata_store::{BlobIndex, BlobIndexOperation, Error, link_kind::LinkKind};

pub const SHARD_READ_CONCURRENCY: usize = 10;

pub fn decode_blob_index_shard_namespace(file_name: &str) -> String {
    file_name
        .strip_suffix(".json")
        .unwrap_or(file_name)
        .replace("%2F", "/")
        .replace("%25", "%")
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
