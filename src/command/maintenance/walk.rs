//! Raw-key walk over one object store, with bounded per-key concurrency.

use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use futures_util::TryStreamExt;

use angos_storage::{ObjectStore, paginated};

use crate::{command::maintenance::error::Error, registry::Error as RegistryError};

/// Keys fetched per listing page. A memory bound only; the concurrency of the
/// per-key work is the caller's knob.
const PAGE_SIZE: u16 = 1000;

/// Run `process` over every key under `prefix` in `store`, at most
/// `concurrency` keys in flight at once. Keys are absolute (the prefix is
/// re-joined onto the backend's prefix-relative listing results).
///
/// `process` is expected to be infallible per key (log-and-count); only a
/// listing failure aborts the walk, since pagination cannot continue without
/// its token.
pub async fn for_each_key<F, Fut>(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    concurrency: usize,
    process: F,
) -> Result<(), Error>
where
    F: Fn(String) -> Fut + Send + Sync,
    Fut: Future<Output = ()> + Send,
{
    let stream = paginated(move |token| async move {
        let page = store
            .list(prefix, PAGE_SIZE, token)
            .await
            .map_err(RegistryError::from)?;
        let items = page
            .items
            .into_iter()
            .map(|key| join_key(prefix, &key))
            .collect();
        Ok((items, page.next_token))
    });

    let process = &process;
    stream
        .try_for_each_concurrent(concurrency.max(1), |key| async move {
            process(key).await;
            Ok(())
        })
        .await
}

/// Rebuild the absolute key from a walk `prefix` (no trailing slash, or empty
/// for the store root) and a prefix-relative listing result.
fn join_key(prefix: &str, relative: &str) -> String {
    if prefix.is_empty() {
        relative.to_string()
    } else {
        format!("{prefix}/{relative}")
    }
}

/// Atomic tallies of one scrub run, logged as its summary.
#[derive(Default)]
pub struct WalkStats {
    /// Keys visited across all passes.
    pub keys: AtomicU64,
    /// Repair or reclaim actions emitted (excluding quarantines and
    /// corrupt-object deletions, counted separately).
    pub repairs: AtomicU64,
    /// Unknown keys handled: quarantined under the lost-and-found prefix,
    /// or deleted outright under `--delete-unknown`.
    pub quarantined: AtomicU64,
    /// Expected-shape objects deleted because their content was unreadable.
    pub corrupt: AtomicU64,
    /// Keys whose validation failed and was skipped (warned and counted;
    /// maintenance is best-effort).
    pub failures: AtomicU64,
}

impl WalkStats {
    /// One-line run summary for the final log. `unknown_deleted` picks the
    /// unknown-key wording, matching what the run actually did with them.
    pub fn summary(&self, unknown_deleted: bool) -> String {
        let unknown_word = if unknown_deleted {
            "unknown deleted"
        } else {
            "quarantined"
        };
        format!(
            "walked {} key(s): {} repair(s), {} {unknown_word}, {} corrupt deleted, {} failure(s)",
            self.keys.load(Ordering::Relaxed),
            self.repairs.load(Ordering::Relaxed),
            self.quarantined.load(Ordering::Relaxed),
            self.corrupt.load(Ordering::Relaxed),
            self.failures.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    use bytes::Bytes;

    use angos_storage::MemoryObjectStore;

    use super::*;

    async fn seeded_store(keys: &[&str]) -> Arc<dyn ObjectStore> {
        let store = MemoryObjectStore::new();
        for key in keys {
            store.put(key, Bytes::from_static(b"x")).await.unwrap();
        }
        Arc::new(store)
    }

    #[tokio::test]
    async fn walks_every_key_under_the_root() {
        let store = seeded_store(&[
            "v2/blobs/sha256/aa/aaaa/data",
            "_jobs/pending/cache/x.json",
            "top-level-file",
        ])
        .await;

        let seen = Mutex::new(BTreeSet::new());
        for_each_key(&store, "", 4, |key| async {
            seen.lock().unwrap().insert(key);
        })
        .await
        .unwrap();

        let seen = seen.into_inner().unwrap();
        assert_eq!(
            seen.into_iter().collect::<Vec<_>>(),
            vec![
                "_jobs/pending/cache/x.json",
                "top-level-file",
                "v2/blobs/sha256/aa/aaaa/data",
            ]
        );
    }

    #[tokio::test]
    async fn prefixed_walk_rejoins_absolute_keys_and_excludes_siblings() {
        let store = seeded_store(&[
            "v2/blobs/sha256/aa/aaaa/data",
            "v2/blobsx/decoy",
            "v2/repositories/ns/_manifests/tags/t/current/link",
        ])
        .await;

        let seen = Mutex::new(Vec::new());
        for_each_key(&store, "v2/blobs", 2, |key| async {
            seen.lock().unwrap().push(key);
        })
        .await
        .unwrap();

        assert_eq!(
            seen.into_inner().unwrap(),
            vec!["v2/blobs/sha256/aa/aaaa/data"]
        );
    }
}
