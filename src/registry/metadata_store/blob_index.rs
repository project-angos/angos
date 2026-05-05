//! Blob-index types for tracking cross-namespace blob references.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::registry::metadata_store::link_kind::LinkKind;

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlobIndex {
    pub namespace: HashMap<String, HashSet<LinkKind>>,
}

#[derive(Debug, Clone)]
pub enum BlobIndexOperation {
    Insert(LinkKind),
    Remove(LinkKind),
}
