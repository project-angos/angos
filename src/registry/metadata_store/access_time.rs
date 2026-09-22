//! Every path that records a pull.
//!
//! A tag's or revision's access times are append-only entries named like tag
//! entries (inverted-millis ordinal plus a per-client suffix), so a listing
//! yields newest first and same-millisecond stamps from distinct clients
//! coexist. Each body records who pulled and when, making the directory a
//! rolling audit log scrub packs into compacted chunks past its gates.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};

use angos_oci::Namespace;

use crate::registry::{
    Error,
    keys::{NamespaceKeys, parse_atime_entry},
    metadata_store::{LIST_PAGE, LinkKind, MetadataStore},
};

/// The stored body of one access entry: who pulled, from where, and when.
#[derive(Debug, Serialize, Deserialize)]
pub struct AccessEntry {
    pub client: String,
    /// Absent on entries recorded before the address was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<String>,
    /// How the client authenticated. Absent on entries recorded before it was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    pub at: DateTime<Utc>,
}

/// Longest client identity an access entry records.
const MAX_CLIENT_CHARS: usize = 256;

/// Fan-out for reading one page of entry bodies.
const ENTRY_READ_CONCURRENCY: usize = 16;

impl MetadataStore {
    /// Append one access entry for `link` under `client`'s identity, a plain
    /// put with no read. Only tags and revisions are pull-tracked, so every
    /// other kind records nothing.
    pub async fn put_access_entry(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
        client: &str,
        client_ip: Option<&str>,
        method: Option<&str>,
    ) -> Result<(), Error> {
        // Identities come from token claims, which have no length bound of
        // their own. The body and the key's suffix must record the same one,
        // or two pulls by one client stop colliding on one entry.
        let client = match client.char_indices().nth(MAX_CLIENT_CHARS) {
            Some((cut, _)) => &client[..cut],
            None => client,
        };
        let at = Utc::now();
        let Some(key) = namespace.atime_entry_path(link, at, client) else {
            return Ok(());
        };
        let entry = serde_json::to_vec(&AccessEntry {
            client: client.to_string(),
            client_ip: client_ip.map(str::to_string),
            method: method.map(str::to_string),
            at,
        })?;
        self.object_store()
            .put(&key, Bytes::from(entry))
            .await
            .map_err(Error::from)
    }

    /// One page of `link`'s recorded pulls, newest first: its live entries,
    /// then its compacted chunks, `n` past the first `offset`, never past the
    /// configured history limit. The flag tells whether more follow. Only tags
    /// and revisions are pull-tracked, so every other kind has no entries. An
    /// entry or chunk deleted or corrupted mid-listing is skipped rather than
    /// failing the read.
    pub async fn read_access_entries(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
        offset: usize,
        n: usize,
    ) -> Result<(Vec<AccessEntry>, bool), Error> {
        let (Some(dir), Some(compacted)) = (
            namespace.atime_dir(link),
            namespace.atime_compacted_dir(link),
        ) else {
            return Ok((Vec::new(), false));
        };
        let limit = usize::try_from(self.atime_retention.history_limit.get()).unwrap_or(usize::MAX);
        let page_len = offset.saturating_add(n).min(limit).saturating_sub(offset);
        // One past the page tells whether another follows.
        let wanted = if offset.saturating_add(page_len) < limit {
            page_len + 1
        } else {
            page_len
        };
        let mut skip = offset;
        let mut entries = Vec::new();

        // Live entries are skipped by name, so skipping costs no reads.
        let mut token = None;
        while entries.len() < wanted {
            let page = self.object_store().list(&dir, LIST_PAGE, token).await?;
            let skipped = skip.min(page.items.len());
            skip -= skipped;
            let names = page
                .items
                .into_iter()
                .skip(skipped)
                .take(wanted - entries.len());
            // `buffered` keeps the listing's newest-first order.
            let read: Vec<AccessEntry> = stream::iter(names)
                .map(|name| {
                    let key = format!("{dir}/{name}");
                    async move {
                        let raw = self.object_store().get(&key).await.ok()?;
                        serde_json::from_slice::<AccessEntry>(&raw).ok()
                    }
                })
                .buffered(ENTRY_READ_CONCURRENCY)
                .filter_map(|entry| async move { entry })
                .collect()
                .await;
            entries.extend(read);
            token = page.next_token;
            if token.is_none() {
                break;
            }
        }

        let mut token = None;
        while entries.len() < wanted {
            let page = self
                .object_store()
                .list(&compacted, LIST_PAGE, token)
                .await?;
            for name in page.items {
                let Ok(raw) = self
                    .object_store()
                    .get(&format!("{compacted}/{name}"))
                    .await
                else {
                    continue;
                };
                let Ok(chunk) = serde_json::from_slice::<Vec<AccessEntry>>(&raw) else {
                    continue;
                };
                let skipped = skip.min(chunk.len());
                skip -= skipped;
                let remaining = wanted - entries.len();
                entries.extend(chunk.into_iter().skip(skipped).take(remaining));
                if entries.len() >= wanted {
                    break;
                }
            }
            token = page.next_token;
            if token.is_none() {
                break;
            }
        }

        let more = entries.len() > page_len;
        entries.truncate(page_len);
        Ok((entries, more))
    }

    /// `link`'s last recorded pull: the newest entry of its atime directory,
    /// whose ordinal encodes the stamp time. Entries list newest first, so one
    /// key answers. Only tags and revisions are pull-tracked, so every other
    /// kind reads as never pulled.
    pub async fn read_access_time(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
    ) -> Result<Option<DateTime<Utc>>, Error> {
        let Some(dir) = namespace.atime_dir(link) else {
            return Ok(None);
        };
        let page = self.object_store().list(&dir, 1, None).await?;
        Ok(page.items.first().and_then(|name| parse_atime_entry(name)))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bytes::Bytes;
    use chrono::Duration;

    use angos_oci::{Digest, Namespace, Tag};

    use super::*;
    use crate::registry::{
        keys::NamespaceKeys,
        metadata_store::{AccessEntry, LinkKind, MetadataStore},
        test_utils::{FSRegistryTestCase, RegistryTestCase, seed_links},
    };

    /// A namespace and a pull-tracked link, for the name-grammar assertions.
    fn namespace() -> Namespace {
        Namespace::new("atime-grammar").unwrap()
    }

    fn tag_link() -> LinkKind {
        LinkKind::Tag(Tag::new("v1").unwrap())
    }

    #[test]
    fn entry_names_round_trip_and_sort_newest_first() {
        let older = DateTime::from_timestamp_millis(1_000_000).unwrap();
        let newer = DateTime::from_timestamp_millis(2_000_000).unwrap();

        let entry_name = |at, client| {
            namespace()
                .atime_entry_path(&tag_link(), at, client)
                .unwrap()
                .rsplit_once('/')
                .unwrap()
                .1
                .to_string()
        };
        let older_name = entry_name(older, "alice");
        let newer_name = entry_name(newer, "alice");
        assert!(
            newer_name < older_name,
            "a newer entry must sort before an older one"
        );
        assert_eq!(parse_atime_entry(&newer_name), Some(newer));
    }

    /// Two clients stamping in the same millisecond must land on distinct
    /// entries, or one pull silently overwrites the other's audit record.
    #[test]
    fn same_millisecond_stamps_from_distinct_clients_do_not_collide() {
        let at = DateTime::from_timestamp_millis(1_000_000).unwrap();
        let namespace = namespace();
        assert_ne!(
            namespace.atime_entry_path(&tag_link(), at, "alice"),
            namespace.atime_entry_path(&tag_link(), at, "bob")
        );
    }

    #[test]
    fn foreign_entry_names_do_not_parse() {
        for name in [
            "",
            "0123.abcd1234",
            &format!("{:016x}.short", 1_u64),
            &format!("{:016x}.zzzzzzzz", 1_u64),
            &format!("{:016x}", 1_u64),
        ] {
            assert_eq!(parse_atime_entry(name), None, "name {name:?}");
        }
    }

    async fn stored_atime(
        backend: &MetadataStore,
        namespace: &Namespace,
        link: &LinkKind,
    ) -> Option<DateTime<Utc>> {
        let LinkKind::Tag(tag) = link else {
            panic!("stored_atime expects a tag link");
        };
        backend
            .read_access_time(namespace, &LinkKind::Tag(tag.clone()))
            .await
            .unwrap()
    }

    /// Plant one access entry at `at` as a raw put, the way a sibling replica's
    /// stamp would land.
    async fn put_entry_at(
        backend: &MetadataStore,
        namespace: &Namespace,
        link: &LinkKind,
        client: &str,
        at: DateTime<Utc>,
    ) {
        let body = serde_json::to_vec(&AccessEntry {
            client: client.to_string(),
            client_ip: None,
            method: None,
            at,
        })
        .unwrap();
        backend
            .object_store()
            .put(
                &namespace.atime_entry_path(link, at, client).unwrap(),
                Bytes::from(body),
            )
            .await
            .unwrap();
    }

    async fn create_tag(
        backend: &MetadataStore,
        namespace: &Namespace,
        tag: &LinkKind,
        hash: &str,
    ) {
        let ops = vec![(tag.clone(), Digest::from_str(hash).unwrap())];
        seed_links(backend, namespace, &ops).await.unwrap();
    }

    /// The entry body carries the acting client, and the newest entry wins the
    /// read over a backdated one.
    #[tokio::test]
    async fn a_pull_stamps_an_entry_carrying_the_client_and_newest_wins() {
        let test_case = FSRegistryTestCase::new();
        let backend = test_case.metadata_store();
        let namespace = Namespace::new("audit-entry-ns").unwrap();
        let tag_name = Tag::new("v1").unwrap();
        let tag = LinkKind::Tag(tag_name.clone());
        create_tag(
            &backend,
            &namespace,
            &tag,
            "sha256:ad01a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6",
        )
        .await;

        backend
            .put_access_entry(&namespace, &tag, "alice", Some("10.0.0.7"), Some("basic"))
            .await
            .unwrap();

        let dir = namespace.tag_atime_entry_dir(&tag_name);
        let page = backend.object_store().list(&dir, 10, None).await.unwrap();
        assert_eq!(page.items.len(), 1, "one pull appends one entry");
        let raw = backend
            .object_store()
            .get(&format!("{dir}/{}", page.items[0]))
            .await
            .unwrap();
        let entry: AccessEntry = serde_json::from_slice(&raw).unwrap();
        assert_eq!(entry.client, "alice", "the body must carry the actor");
        assert_eq!(entry.client_ip.as_deref(), Some("10.0.0.7"));
        assert_eq!(entry.method.as_deref(), Some("basic"));

        put_entry_at(
            &backend,
            &namespace,
            &tag,
            "bob",
            Utc::now() - Duration::hours(3),
        )
        .await;
        let read = backend
            .read_access_time(&namespace, &LinkKind::Tag(tag_name.clone()))
            .await
            .unwrap()
            .unwrap();
        assert!(
            Utc::now().signed_duration_since(read) < Duration::minutes(5),
            "the newest entry must win the read, not the backdated one"
        );
    }

    /// Distinct clients stamping the same millisecond coexist through the entry
    /// name's suffix.
    #[tokio::test]
    async fn each_pull_appends_one_entry_per_client() {
        let test_case = FSRegistryTestCase::new();
        let backend = test_case.metadata_store();
        let namespace = Namespace::new("audit-per-pull-ns").unwrap();
        let tag_name = Tag::new("v1").unwrap();
        let tag = LinkKind::Tag(tag_name.clone());
        create_tag(
            &backend,
            &namespace,
            &tag,
            "sha256:ad02b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1",
        )
        .await;

        for client in ["alice", "bob"] {
            backend
                .put_access_entry(&namespace, &tag, client, None, None)
                .await
                .unwrap();
        }

        let dir = namespace.tag_atime_entry_dir(&tag_name);
        let page = backend.object_store().list(&dir, 10, None).await.unwrap();
        assert_eq!(page.items.len(), 2, "each client's pull is its own entry");
    }

    /// Concurrent stamps are appends, so they never contend.
    #[tokio::test]
    async fn concurrent_stamps_never_contend() {
        let test_case = FSRegistryTestCase::new();
        let backend = test_case.metadata_store();
        let namespace = Namespace::new("audit-stamp-race").unwrap();
        let digest = Digest::from_str(
            "sha256:ad04b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1",
        )
        .unwrap();
        let tag = LinkKind::Tag(Tag::new("v1").unwrap());
        create_tag(
            &backend,
            &namespace,
            &tag,
            "sha256:ad04b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1",
        )
        .await;

        let mut stamps = Vec::new();
        for _ in 0..10 {
            stamps.push(backend.put_access_entry(&namespace, &tag, "racer", None, None));
        }
        for stamp in stamps {
            stamp.await.unwrap();
        }
        let meta = backend.read_link(&namespace, &tag).await.unwrap();
        assert_eq!(meta.target, digest);

        let raw = stored_atime(&backend, &namespace, &tag).await;
        assert!(raw.is_some(), "the racing stamps must have landed");
    }
}
