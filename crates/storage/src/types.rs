use chrono::{DateTime, Utc};

#[derive(Clone, Debug, PartialEq)]
pub struct ObjectMeta {
    pub size: u64,
    pub last_modified: Option<DateTime<Utc>>,
}

/// One page of a flat listing; `next_token` resumes it, `None` ends it.
#[derive(Clone, Debug, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_token: Option<String>,
}

/// Every immediate child under a prefix, unordered.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Children {
    pub sub_prefixes: Vec<String>,
    pub objects: Vec<String>,
}

/// One page of a one-level listing. Sub-prefix names are bare on every backend:
/// `prefix/v1/foo` yields `v1`, not `v1/`.
#[derive(Clone, Debug, PartialEq)]
pub struct ChildrenPage {
    pub sub_prefixes: Vec<String>,
    pub objects: Vec<String>,
    pub next_token: Option<String>,
}
