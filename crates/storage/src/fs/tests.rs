use std::fs as stdfs;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{TryStreamExt, stream};
use tempfile::TempDir;
use tokio::sync::oneshot;

use crate::fs::Backend;
use crate::test_util::frame;
use crate::tests::object_store_conformance;
use crate::{ByteStream, Error, ObjectStore};

fn backend(dir: &TempDir) -> Backend {
    Backend::new(dir.path(), false)
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

    assert!(
        !dir.path().join("x").exists(),
        "empty ancestors must be pruned"
    );
    assert!(dir.path().exists(), "the store root is never removed");
}

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

    assert!(
        !dir.path().join("tags/garbled").exists(),
        "empty ancestors must be pruned after a single-key delete"
    );
    assert!(dir.path().join("tags/live/current").exists());
}

/// A write that opened the staging file before the promotion, or appended
/// after the caller's size check, must not reach the promoted blob.
#[tokio::test]
async fn promote_upload_publishes_only_verified_bytes() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(backend(&dir));
    store
        .write_upload("up/data", frame(b"verified"), Some(8))
        .await
        .unwrap();

    // An in-flight PATCH: its file is open, its body held back.
    let (opened_tx, opened_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let body: ByteStream = Box::pin(stream::once(async move {
        let _ = opened_tx.send(());
        let _ = release_rx.await;
        Ok(Bytes::from_static(b"late"))
    }));
    let writer = tokio::spawn({
        let store = store.clone();
        async move { store.write_upload("up/data", body, Some(4)).await }
    });
    opened_rx.await.unwrap();
    store
        .write_upload("up/data", frame(b"tail"), Some(4))
        .await
        .unwrap();

    store
        .promote_upload("up/data", "blob/data", 8)
        .await
        .unwrap();
    release_tx.send(()).unwrap();
    writer.await.unwrap().unwrap();

    assert_eq!(store.get("blob/data").await.unwrap(), b"verified");
    assert!(matches!(store.head("up/data").await, Err(Error::NotFound)));
}

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
async fn prune_empty_ancestors_refuses_to_escape_root_via_dotdot() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("root");
    stdfs::create_dir(&root).unwrap();
    let outside = dir.path().join("outside");
    stdfs::create_dir(&outside).unwrap();

    let store = Backend::new(&root, false);

    store.prune_empty_ancestors("../outside/leaf").await;

    assert!(
        outside.exists(),
        "pruning must never remove a directory outside the configured root"
    );
    assert!(root.exists(), "the store root itself must survive");
}

#[tokio::test]
async fn head_reports_mtime() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store.put("k", Bytes::from_static(b"abcdef")).await.unwrap();
    let meta = store.head("k").await.unwrap();
    assert!(meta.last_modified.is_some());
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

/// Paging from any cursor yields every key above it, across names that sort
/// against their own directory (`a!` < `a/x` < `a0`).
#[tokio::test]
async fn list_after_pages_in_lexical_order_from_any_cursor() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    let mut keys = [
        "a!", "a.txt", "a/x", "a/y!", "a/y/z", "a0", "b/c!", "b/c/d", "e",
    ]
    .map(String::from);
    for key in &keys {
        store
            .put(&format!("p/{key}"), Bytes::from_static(b"x"))
            .await
            .unwrap();
    }
    keys.sort();

    let others = ["", "a", "a/", "a/y", "b", "z"].map(String::from);
    let cursors = keys.iter().chain(&others);
    for cursor in cursors {
        for n in 1..=3 {
            let mut listed = Vec::new();
            let mut page = store
                .list_after("p", n, None, Some(cursor.clone()))
                .await
                .unwrap();
            loop {
                listed.extend(page.items);
                let Some(token) = page.next_token else { break };
                page = store.list_after("p", n, Some(token), None).await.unwrap();
            }
            let above: Vec<&String> = keys.iter().filter(|key| *key > cursor).collect();
            assert_eq!(
                listed.iter().collect::<Vec<_>>(),
                above,
                "after {cursor:?} in pages of {n}"
            );
        }
    }
}

#[tokio::test]
async fn writes_round_trip_with_durability_enabled() {
    let dir = TempDir::new().unwrap();
    let store = Backend::new(dir.path(), true);

    store
        .put("dir/k", Bytes::from_static(b"durable"))
        .await
        .unwrap();
    assert_eq!(store.get("dir/k").await.unwrap(), b"durable");

    let written = store
        .write_upload("up/synced", frame(b"durable bytes"), Some(13))
        .await
        .unwrap();
    store.complete_upload("up/synced").await.unwrap();
    assert_eq!(written, 13);
    assert_eq!(store.get("up/synced").await.unwrap(), b"durable bytes");
}

#[tokio::test]
async fn listings_skip_atomic_write_temporaries() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);
    store
        .put("ns/real.json", Bytes::from_static(b"[]"))
        .await
        .unwrap();

    stdfs::write(dir.path().join("ns").join(".angos-write.deadbeef"), b"").unwrap();

    let page = store.list("ns", 100, None).await.unwrap();
    assert_eq!(page.items, vec!["real.json".to_string()]);

    let children = store.list_children("ns", 100, None, None).await.unwrap();
    assert_eq!(children.objects, vec!["real.json".to_string()]);
    assert!(children.next_token.is_none());
}

/// The macOS/APFS sibling-`mkdir` race.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writes_under_shared_parent_all_succeed() {
    const TASKS: usize = 64;

    let dir = TempDir::new().unwrap();
    let store = backend(&dir);

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

#[tokio::test]
async fn list_all_walks_the_whole_tree_once() {
    let dir = TempDir::new().unwrap();
    let store = backend(&dir);

    for k in ["w/a.txt", "w/a/x", "w/a/y", "w/b/c/d", "w/z"] {
        store.put(k, Bytes::from_static(b"v")).await.unwrap();
    }

    let mut keys: Vec<String> = store.list_all("w/").try_collect().await.unwrap();
    keys.sort();
    assert_eq!(keys, ["a.txt", "a/x", "a/y", "b/c/d", "z"]);
}
