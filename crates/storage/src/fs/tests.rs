use bytes::Bytes;
use tempfile::TempDir;
use tokio::io::AsyncReadExt;

use crate::{Error, ObjectStore, fs::Backend};

fn backend(dir: &TempDir) -> Backend {
    Backend::builder()
        .root_dir(dir.path())
        .build()
        .expect("backend must build")
}

#[tokio::test]
async fn builder_requires_root_dir() {
    let err = Backend::builder().build().unwrap_err();
    assert!(
        matches!(err, Error::Backend(msg) if msg.contains("root_dir")),
        "missing root_dir must surface a clear Backend error"
    );
}

#[tokio::test]
async fn put_then_get_round_trips() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store
        .put("a/b/c", Bytes::from_static(b"hello"))
        .await
        .unwrap();
    assert_eq!(store.get("a/b/c").await.unwrap(), b"hello");
}

#[tokio::test]
async fn get_missing_key_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    assert_eq!(store.get("missing").await.unwrap_err(), Error::NotFound);
}

#[tokio::test]
async fn delete_is_idempotent_on_missing_key() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store.delete("ghost").await.unwrap();
    store.delete("ghost").await.unwrap();
}

#[tokio::test]
async fn delete_prefix_removes_subtree() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store.put("a/1", Bytes::from_static(b"x")).await.unwrap();
    store
        .put("a/sub/2", Bytes::from_static(b"y"))
        .await
        .unwrap();
    store.put("b/3", Bytes::from_static(b"z")).await.unwrap();

    store.delete_prefix("a").await.unwrap();

    assert_eq!(store.get("a/1").await.unwrap_err(), Error::NotFound);
    assert_eq!(store.get("a/sub/2").await.unwrap_err(), Error::NotFound);
    assert_eq!(store.get("b/3").await.unwrap(), b"z");
}

#[tokio::test]
async fn delete_prefix_on_missing_prefix_is_success() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store.delete_prefix("never-existed/").await.unwrap();
}

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

    let store = Backend::builder()
        .root_dir(&root)
        .build()
        .expect("backend must build");

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
/// have its now-empty in-root ancestors pruned exactly as before — the fix
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
async fn head_reports_size_and_mtime() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store.put("k", Bytes::from_static(b"abcdef")).await.unwrap();
    let meta = store.head("k").await.unwrap();
    assert_eq!(meta.size, 6);
    assert!(meta.last_modified.is_some());
    assert!(meta.etag.is_none(), "FS backend never synthesises ETags");
}

#[tokio::test]
async fn head_missing_key_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    assert_eq!(store.head("missing").await.unwrap_err(), Error::NotFound);
}

#[tokio::test]
async fn get_stream_reports_total_size_not_remaining() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store
        .put("k", Bytes::from_static(b"0123456789"))
        .await
        .unwrap();

    let (mut body, total) = store.get_stream("k", Some(3)).await.unwrap();
    assert_eq!(total, 10);
    let mut buf = Vec::new();
    body.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"3456789");
}

#[tokio::test]
async fn list_walks_recursively_in_sorted_order() {
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

/// Listing under a non-empty prefix must strip the prefix from each item, so
/// the FS backend matches the S3 backend's prefix-relative contract.
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

#[tokio::test]
async fn list_paginates_via_continuation_token() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    for k in ["a", "b", "c"] {
        store.put(k, Bytes::from_static(b"x")).await.unwrap();
    }
    let first = store.list("", 2, None).await.unwrap();
    assert_eq!(first.items, vec!["a".to_string(), "b".to_string()]);
    assert_eq!(first.next_token.as_deref(), Some("b"));

    let second = store.list("", 2, first.next_token).await.unwrap();
    assert_eq!(second.items, vec!["c".to_string()]);
    assert!(second.next_token.is_none());
}

#[tokio::test]
async fn list_children_separates_directories_from_objects() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store.put("ns/a", Bytes::from_static(b"x")).await.unwrap();
    store
        .put("ns/sub/b", Bytes::from_static(b"y"))
        .await
        .unwrap();
    store
        .put("ns/sub/c", Bytes::from_static(b"z"))
        .await
        .unwrap();
    store.put("ns/d", Bytes::from_static(b"w")).await.unwrap();

    let page = store.list_children("ns", 10, None, None).await.unwrap();
    assert_eq!(page.sub_prefixes, vec!["sub".to_string()]);
    assert_eq!(page.objects, vec!["a".to_string(), "d".to_string()]);
    assert!(page.next_token.is_none());
}

#[tokio::test]
async fn list_children_respects_start_after() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    for k in ["ns/a", "ns/b", "ns/c"] {
        store.put(k, Bytes::from_static(b"x")).await.unwrap();
    }
    let page = store
        .list_children("ns", 10, None, Some("a".to_string()))
        .await
        .unwrap();
    assert_eq!(page.objects, vec!["b".to_string(), "c".to_string()]);
}

#[tokio::test]
async fn copy_duplicates_object() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store
        .put("src", Bytes::from_static(b"payload"))
        .await
        .unwrap();
    store.copy("src", "dst/copied").await.unwrap();
    assert_eq!(store.get("src").await.unwrap(), b"payload");
    assert_eq!(store.get("dst/copied").await.unwrap(), b"payload");
}

/// `move_object` relocates the object (creating the destination's parent) and
/// removes the source. On the filesystem backend this is a same-FS `rename`, so
/// the body is never read into memory — the correctness contract is the same as
/// the trait default's copy + delete.
#[tokio::test]
async fn move_object_relocates_and_removes_source() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store
        .put("src", Bytes::from_static(b"payload"))
        .await
        .unwrap();
    store.move_object("src", "blob-data/moved").await.unwrap();
    assert_eq!(store.get("blob-data/moved").await.unwrap(), b"payload");
    assert!(
        matches!(store.head("src").await, Err(Error::NotFound)),
        "source must be gone after move",
    );
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
    let store = Backend::builder()
        .root_dir(dir.path())
        .sync_to_disk(true)
        .build()
        .unwrap();
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
