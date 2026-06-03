use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Opaque object-version identifier returned by the storage backend.
///
/// Compared by exact byte equality; the format is backend-specific (S3
/// `ETag`s are quoted strings, FS implementations may synthesise them from
/// content hashes or `(mtime, inode)` tuples). Consumers should treat it as
/// an opaque token used only with conditional operations.
///
/// Serialises transparently as the underlying string so on-disk records that
/// store `ETag`s as plain strings remain wire-compatible.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Etag(String);

impl Etag {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl Display for Etag {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// Metadata for a single stored object, returned by `ObjectStore::head`.
#[derive(Clone, Debug, PartialEq)]
pub struct ObjectMeta {
    pub size: u64,
    pub etag: Option<Etag>,
    pub last_modified: Option<DateTime<Utc>>,
}

/// One page of results from a flat listing (`ObjectStore::list`).
///
/// `next_token` is `Some` when the listing was truncated; pass it back to
/// the next `list` call to resume. `None` means the listing is complete.
#[derive(Clone, Debug, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_token: Option<String>,
}

/// One page of results from a hierarchical listing (`ObjectStore::list_children`).
///
/// `sub_prefixes` contains the immediate sub-prefix names (the "subdirectories"
/// directly under the requested prefix). Each name is **bare**: the delimiter
/// (trailing `/`) is stripped, so a key `prefix/v1/foo` yields the sub-prefix
/// `v1`, not `v1/`. All backends honour this so consumers can treat the names
/// identically regardless of backend. `objects` contains any keys sitting
/// directly at the requested prefix level with no further `/` separator.
/// `next_token` follows the same semantics as `Page::next_token`.
#[derive(Clone, Debug, PartialEq)]
pub struct ChildrenPage {
    pub sub_prefixes: Vec<String>,
    pub objects: Vec<String>,
    pub next_token: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{TimeZone, Utc};

    use crate::types::{ChildrenPage, Etag, ObjectMeta, Page};

    #[test]
    fn etag_round_trips_through_string() {
        let raw = "\"abc123\"";
        let etag = Etag::new(raw);
        assert_eq!(etag.as_str(), raw);
        assert_eq!(etag.to_string(), raw);
        assert_eq!(etag.clone().into_inner(), raw);
    }

    #[test]
    fn etag_serializes_transparently_as_string() {
        let etag = Etag::new("etag-value");
        let json = serde_json::to_string(&etag).expect("serialize");
        assert_eq!(json, "\"etag-value\"");
        let round_trip: Etag = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(round_trip, etag);
    }

    #[test]
    fn etag_works_as_hashmap_key() {
        let mut map: HashMap<Etag, u32> = HashMap::new();
        map.insert(Etag::new("a"), 1);
        map.insert(Etag::new("b"), 2);
        assert_eq!(map.get(&Etag::new("a")), Some(&1));
        assert_eq!(map.get(&Etag::new("c")), None);
    }

    #[test]
    fn object_meta_holds_optional_metadata() {
        let when = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let meta = ObjectMeta {
            size: 42,
            etag: Some(Etag::new("e")),
            last_modified: Some(when),
        };
        assert_eq!(meta.size, 42);
        assert_eq!(meta.etag.as_ref().map(Etag::as_str), Some("e"));
        assert_eq!(meta.last_modified, Some(when));
    }

    #[test]
    fn page_carries_items_and_continuation() {
        let page = Page {
            items: vec!["a".to_string(), "b".to_string()],
            next_token: Some("cursor".to_string()),
        };
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.next_token.as_deref(), Some("cursor"));
    }

    #[test]
    fn children_page_separates_sub_prefixes_from_objects() {
        let page = ChildrenPage {
            sub_prefixes: vec!["sub".to_string()],
            objects: vec!["leaf.json".to_string()],
            next_token: None,
        };
        assert_eq!(page.sub_prefixes, vec!["sub".to_string()]);
        assert_eq!(page.objects, vec!["leaf.json".to_string()]);
        assert!(page.next_token.is_none());
    }
}
