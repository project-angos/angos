use bytes::Bytes;
use tempfile::TempDir;

use crate::tests::object_store_conformance;
use crate::{ObjectStore, fs::Backend};

fn backend(dir: &TempDir) -> Backend {
    Backend::builder(dir.path()).build()
}

object_store_conformance!({
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    (store, dir)
});

#[tokio::test]
async fn delete_prefix_prunes_empty_ancestors_up_to_root() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store
        .put("x/y/z/data", Bytes::from_static(b"v"))
        .await
        .unwrap();

    store.delete_prefix("x/y/z").await.unwrap();

    // The whole now-empty chain collapses, but the store root survives.
    assert!(
        !dir.path().join("x").exists(),
        "empty ancestors must be pruned"
    );
    assert!(dir.path().exists(), "the store root is never removed");
}

/// Single-key `delete` must collapse now-empty ancestors exactly like
/// `delete_prefix`, matching S3 where a prefix vanishes with its last object.
/// Regression guard for hollow tag directories surviving a link delete and
/// being read back as live tags by directory-based listings.
#[tokio::test]
async fn delete_prunes_empty_ancestors_up_to_root() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store
        .put("tags/garbled/current/link", Bytes::from_static(b"v"))
        .await
        .unwrap();
    store
        .put("tags/live/current/link", Bytes::from_static(b"v"))
        .await
        .unwrap();

    store.delete("tags/garbled/current/link").await.unwrap();

    // The deleted tag's chain collapses up to the still-populated `tags/`.
    assert!(
        !dir.path().join("tags/garbled").exists(),
        "empty ancestors must be pruned after a single-key delete"
    );
    assert!(dir.path().join("tags/live/current").exists());
}

/// `move_object`'s rename fast path must sweep the source's now-empty parent
/// chain like `delete` does, or a quarantine-style move leaves a hollow
/// directory that directory-based listings surface as a live container.
#[tokio::test]
async fn move_object_prunes_empty_source_ancestors() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store
        .put("zz-alien/at-root", Bytes::from_static(b"alien"))
        .await
        .unwrap();

    store
        .move_object("zz-alien/at-root", "_lost_and_found/zz-alien/at-root")
        .await
        .unwrap();

    assert!(
        !dir.path().join("zz-alien").exists(),
        "the emptied source directory must be pruned after a move"
    );
    assert_eq!(
        store.get("_lost_and_found/zz-alien/at-root").await.unwrap(),
        b"alien"
    );
}

#[tokio::test]
async fn delete_prefix_stops_pruning_at_first_non_empty_ancestor() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store
        .put("shard/aa/blob1/data", Bytes::from_static(b"1"))
        .await
        .unwrap();
    store
        .put("shard/aa/blob2/data", Bytes::from_static(b"2"))
        .await
        .unwrap();

    store.delete_prefix("shard/aa/blob1").await.unwrap();

    // `shard/aa` still holds `blob2`, so pruning halts there.
    assert!(!dir.path().join("shard/aa/blob1").exists());
    assert_eq!(store.get("shard/aa/blob2/data").await.unwrap(), b"2");
    assert!(dir.path().join("shard/aa").exists());
}

/// A `..`-bearing key whose ancestors resolve *above* the store root must
/// never let pruning touch anything outside the root. `prune_empty_ancestors`
/// must lexically collapse the path before any `read_dir`/`remove_dir`, so a
/// crafted escape resolves to "not under root" and prunes nothing. Regression
/// guard for the lexical-`starts_with` hole that let `remove_dir` delete an
/// empty directory sitting next to the configured root.
#[tokio::test]
async fn prune_empty_ancestors_refuses_to_escape_root_via_dotdot() {
    use std::fs as stdfs;

    let dir = TempDir::new().unwrap();
    // The store root is a *sub*directory of the tempdir, so a sibling dir can
    // sit outside the root yet still inside the tempdir we control.
    let root = dir.path().join("root");
    stdfs::create_dir(&root).unwrap();
    let outside = dir.path().join("outside");
    stdfs::create_dir(&outside).unwrap();

    let store = Backend::builder(&root).build();

    // `root.join("../outside/leaf")` resolves at syscall time to
    // `<tmp>/outside/leaf`; its parent is the empty `<tmp>/outside`, which the
    // old lexical guard would have happily `remove_dir`'d.
    store.prune_empty_ancestors("../outside/leaf").await;

    assert!(
        outside.exists(),
        "pruning must never remove a directory outside the configured root"
    );
    assert!(root.exists(), "the store root itself must survive");
}

/// Companion to the escape guard: a normal nested key (no `.`/`..`) must still
/// have its now-empty in-root ancestors pruned exactly as before. The fix
/// changes nothing for well-formed keys.
#[tokio::test]
async fn prune_empty_ancestors_still_collapses_normal_in_root_chain() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);

    // Build an empty chain `p/q/r` with no leaf left behind, then prune from a
    // would-be leaf inside it.
    std::fs::create_dir_all(dir.path().join("p/q/r")).unwrap();

    store.prune_empty_ancestors("p/q/r/leaf").await;

    assert!(
        !dir.path().join("p").exists(),
        "empty in-root ancestors must still be pruned for normal keys"
    );
    assert!(dir.path().exists(), "the store root is never removed");
}

#[tokio::test]
async fn head_reports_mtime_but_no_etag() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store.put("k", Bytes::from_static(b"abcdef")).await.unwrap();
    let meta = store.head("k").await.unwrap();
    assert!(meta.last_modified.is_some());
    assert!(meta.etag.is_none(), "FS backend never synthesises ETags");
}

#[tokio::test]
async fn list_at_store_root_walks_recursively_in_sorted_order() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    for k in ["b/2", "a/1", "a/3", "c"] {
        store.put(k, Bytes::from_static(b"x")).await.unwrap();
    }
    let page = store.list("", 10, None).await.unwrap();
    assert_eq!(
        page.items,
        vec![
            "a/1".to_string(),
            "a/3".to_string(),
            "b/2".to_string(),
            "c".to_string(),
        ],
    );
    assert!(page.next_token.is_none());
}

/// Listing under a non-slash-terminated prefix must strip the prefix from each
/// item, so the FS backend matches the S3 backend's prefix-relative contract.
#[tokio::test]
async fn list_returns_prefix_relative_keys() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store.put("ns/a", Bytes::from_static(b"x")).await.unwrap();
    store
        .put("ns/sub/b", Bytes::from_static(b"y"))
        .await
        .unwrap();

    let page = store.list("ns", 10, None).await.unwrap();
    assert_eq!(page.items, vec!["a".to_string(), "sub/b".to_string()]);
}

/// `list_all_children` must return every child even when the directory
/// contains more entries than a single page (`page_size=2` used internally
/// to exercise the pagination loop).
#[tokio::test]
async fn list_all_children_returns_all_entries_across_pages() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);

    // Write five sub-directories' worth of files so we exceed any small page.
    for k in ["ns/a/x", "ns/b/x", "ns/c/x", "ns/d/x", "ns/e/x"] {
        store.put(k, Bytes::from_static(b"v")).await.unwrap();
    }
    // Also a direct object at the same level.
    store.put("ns/z", Bytes::from_static(b"v")).await.unwrap();

    let (sub_prefixes, objects) = store.list_all_children("ns").await.unwrap();
    assert_eq!(
        sub_prefixes,
        vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ],
        "all sub-prefixes must be returned even across page boundaries"
    );
    assert_eq!(objects, vec!["z".to_string()]);
}

#[tokio::test]
async fn list_all_children_on_missing_prefix_returns_empty() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    let (sub_prefixes, objects) = store.list_all_children("does/not/exist").await.unwrap();
    assert!(sub_prefixes.is_empty());
    assert!(objects.is_empty());
}

#[tokio::test]
async fn sync_to_disk_flag_does_not_change_observable_behaviour() {
    let dir = TempDir::new().unwrap();
    let store = Backend::builder(dir.path()).sync_to_disk(true).build();
    store
        .put("k", Bytes::from_static(b"durable"))
        .await
        .unwrap();
    assert_eq!(store.get("k").await.unwrap(), b"durable");
}

/// A stray atomic-write temporary sitting in a directory must never be
/// surfaced by `list`/`list_children`. Regression guard for the conformance
/// failure where a concurrent reader observed a 0-byte `.angos-write.*` temp
/// in a `blob-index/.../refs/` dir and failed to JSON-parse it.
#[tokio::test]
async fn listings_skip_atomic_write_temporaries() {
    use std::fs as stdfs;

    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store
        .put("ns/real.json", Bytes::from_static(b"[]"))
        .await
        .unwrap();

    // Simulate an in-flight atomic write: an empty temp file next to the real
    // object, with the reserved prefix.
    stdfs::write(dir.path().join("ns").join(".angos-write.deadbeef"), b"").unwrap();

    let page = store.list("ns", 100, None).await.unwrap();
    assert_eq!(page.items, vec!["real.json".to_string()]);

    let children = store.list_children("ns", 100, None, None).await.unwrap();
    assert_eq!(children.objects, vec!["real.json".to_string()]);
    assert!(children.next_token.is_none());
}

/// `ensure_parent`'s retry loop must absorb the transient macOS/APFS
/// sibling-`mkdir` race when concurrent writes share a parent directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writes_under_shared_parent_all_succeed() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);

    const TASKS: usize = 64;
    let mut handles = Vec::with_capacity(TASKS);
    for i in 0..TASKS {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            store
                .put(&format!("shard/file-{i}"), Bytes::from(format!("body-{i}")))
                .await
        }));
    }

    for handle in handles {
        handle
            .await
            .expect("write task must not panic")
            .expect("every concurrent put under a shared parent must succeed");
    }

    for i in 0..TASKS {
        let key = format!("shard/file-{i}");
        assert_eq!(
            store.get(&key).await.unwrap(),
            format!("body-{i}").into_bytes(),
        );
    }
}
