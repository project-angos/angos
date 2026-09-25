//! Backend-agnostic conformance suites for the storage traits.
//!
//! [`object_store_conformance!`] stamps out one named `#[tokio::test]` per
//! contract clause, so every backend runs the identical suite under its own
//! module path and a failure names the backend. Each backend's own test module
//! instantiates the suite and keeps only backend-specific tests next to them.

/// Stamp out the [`ObjectStore`](crate::ObjectStore) contract tests against
/// one backend, in a child module named `object_store_suite`.
///
/// `$fixture` is an expression evaluated fresh in every test; it must yield a
/// `(store, guard)` tuple. `store` is the backend under test (a concrete
/// backend or a trait object) and `guard` is whatever must stay alive for the
/// test's duration (the FS backend's `TempDir`; `()` when nothing is needed).
/// The expression is expanded inside the child module, so names it references
/// resolve through `use super::*` in the instantiating module.
macro_rules! object_store_conformance {
    ($fixture:expr) => {
        mod object_store_suite {
            use bytes::Bytes;
            use tokio::io::AsyncReadExt;

            use super::*;
            use crate::Error;
            // Resolves trait methods on the concrete backends under test.
            use crate::ObjectStore;
            use crate::test_util::frame;

            #[tokio::test]
            async fn put_then_get_round_trips() {
                let (store, _guard) = $fixture;
                store
                    .put("rt/nested/key", Bytes::from_static(b"hello"))
                    .await
                    .unwrap();
                assert_eq!(store.get("rt/nested/key").await.unwrap(), b"hello");
            }

            #[tokio::test]
            async fn put_overwrites_existing_object() {
                let (store, _guard) = $fixture;
                store.put("ow/k", Bytes::from_static(b"v1")).await.unwrap();
                store.put("ow/k", Bytes::from_static(b"v2")).await.unwrap();
                assert_eq!(store.get("ow/k").await.unwrap(), b"v2");
            }

            #[tokio::test]
            async fn create_if_absent_first_create_wins_second_keeps_original() {
                let (store, _guard) = $fixture;
                assert!(
                    store
                        .create_if_absent("cia/k", Bytes::from_static(b"first"))
                        .await
                        .unwrap()
                );
                assert!(
                    !store
                        .create_if_absent("cia/k", Bytes::from_static(b"second"))
                        .await
                        .unwrap()
                );
                assert_eq!(store.get("cia/k").await.unwrap(), b"first");
            }

            #[tokio::test]
            async fn create_if_absent_succeeds_again_after_delete() {
                let (store, _guard) = $fixture;
                assert!(
                    store
                        .create_if_absent("cia/again", Bytes::from_static(b"v1"))
                        .await
                        .unwrap()
                );
                store.delete("cia/again").await.unwrap();
                assert!(
                    store
                        .create_if_absent("cia/again", Bytes::from_static(b"v2"))
                        .await
                        .unwrap()
                );
                assert_eq!(store.get("cia/again").await.unwrap(), b"v2");
            }

            #[tokio::test]
            async fn get_missing_key_returns_not_found() {
                let (store, _guard) = $fixture;
                assert_eq!(store.get("missing").await.unwrap_err(), Error::NotFound);
            }

            #[tokio::test]
            async fn head_missing_key_returns_not_found() {
                let (store, _guard) = $fixture;
                assert_eq!(store.head("missing").await.unwrap_err(), Error::NotFound);
            }

            #[tokio::test]
            async fn head_reports_object_size() {
                let (store, _guard) = $fixture;
                store
                    .put("hd/k", Bytes::from_static(b"abcdef"))
                    .await
                    .unwrap();
                assert_eq!(store.head("hd/k").await.unwrap().size, 6);
            }

            #[tokio::test]
            async fn delete_removes_object() {
                let (store, _guard) = $fixture;
                store.put("del/k", Bytes::from_static(b"v")).await.unwrap();
                store.delete("del/k").await.unwrap();
                assert_eq!(store.get("del/k").await.unwrap_err(), Error::NotFound);
            }

            #[tokio::test]
            async fn delete_is_idempotent_on_missing_key() {
                let (store, _guard) = $fixture;
                store.delete("ghost").await.unwrap();
                store.delete("ghost").await.unwrap();
            }

            #[tokio::test]
            async fn delete_prefix_removes_only_keys_under_prefix() {
                let (store, _guard) = $fixture;
                store.put("dp/a/1", Bytes::from_static(b"x")).await.unwrap();
                store
                    .put("dp/a/sub/2", Bytes::from_static(b"y"))
                    .await
                    .unwrap();
                store.put("dp/b/3", Bytes::from_static(b"z")).await.unwrap();

                store.delete_prefix("dp/a").await.unwrap();

                assert_eq!(store.get("dp/a/1").await.unwrap_err(), Error::NotFound);
                assert_eq!(store.get("dp/a/sub/2").await.unwrap_err(), Error::NotFound);
                assert_eq!(store.get("dp/b/3").await.unwrap(), b"z");
            }

            #[tokio::test]
            async fn delete_prefix_is_directory_scoped_not_string_prefix() {
                // Regression for the data-loss bug: a non-slash prefix is a
                // directory boundary, so it must delete keys under the
                // directory but never a sibling that merely shares a string
                // prefix (`tags/v1` must not affect `tags/v1-rc/...`).
                let (store, _guard) = $fixture;
                store
                    .put("dps/v1/1", Bytes::from_static(b"x"))
                    .await
                    .unwrap();
                store
                    .put("dps/v1/c/2", Bytes::from_static(b"y"))
                    .await
                    .unwrap();
                store
                    .put("dps/v1-rc/3", Bytes::from_static(b"z"))
                    .await
                    .unwrap();

                store.delete_prefix("dps/v1").await.unwrap();

                assert_eq!(store.get("dps/v1/1").await.unwrap_err(), Error::NotFound);
                assert_eq!(store.get("dps/v1/c/2").await.unwrap_err(), Error::NotFound);
                assert_eq!(
                    store.get("dps/v1-rc/3").await.unwrap(),
                    b"z",
                    "sibling sharing a string prefix must survive"
                );
            }

            #[tokio::test]
            async fn delete_prefix_missing_prefix_is_success() {
                let (store, _guard) = $fixture;
                store.delete_prefix("never-existed/").await.unwrap();
            }

            #[tokio::test]
            async fn delete_prefix_empty_prefix_is_noop() {
                // An empty prefix must not be treated as the store root and
                // wipe everything.
                let (store, _guard) = $fixture;
                store.put("dpe/1", Bytes::from_static(b"x")).await.unwrap();
                store.put("dpf/1", Bytes::from_static(b"y")).await.unwrap();

                store.delete_prefix("").await.unwrap();

                assert_eq!(store.get("dpe/1").await.unwrap(), b"x");
                assert_eq!(store.get("dpf/1").await.unwrap(), b"y");
            }

            #[tokio::test]
            async fn get_stream_reports_total_size_not_remaining() {
                let (store, _guard) = $fixture;
                store
                    .put("st/k", Bytes::from_static(b"0123456789"))
                    .await
                    .unwrap();
                let (mut body, total) = store.get_stream("st/k", Some(3)).await.unwrap();
                assert_eq!(total, 10);
                let mut buf = Vec::new();
                body.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, b"3456789");
            }

            #[tokio::test]
            async fn list_returns_sorted_prefix_relative_keys() {
                // `lst-outside/z` shares a string prefix with `lst/` and must
                // not appear in the listing.
                let (store, _guard) = $fixture;
                for k in ["lst/b", "lst/a", "lst/sub/c", "lst-outside/z"] {
                    store.put(k, Bytes::from_static(b"x")).await.unwrap();
                }
                let page = store.list("lst/", 10, None).await.unwrap();
                assert_eq!(
                    page.items,
                    vec!["a".to_string(), "b".to_string(), "sub/c".to_string()]
                );
                assert!(page.next_token.is_none());
            }

            #[tokio::test]
            async fn list_paginates_via_opaque_token() {
                let (store, _guard) = $fixture;
                for k in ["pg/a", "pg/b", "pg/c"] {
                    store.put(k, Bytes::from_static(b"x")).await.unwrap();
                }
                let page = store.list("pg/", 2, None).await.unwrap();
                assert_eq!(page.items, vec!["a".to_string(), "b".to_string()]);
                let page2 = store.list("pg/", 2, page.next_token).await.unwrap();
                assert_eq!(page2.items, vec!["c".to_string()]);
                assert!(page2.next_token.is_none());
            }

            #[tokio::test]
            async fn list_after_starts_strictly_after_the_given_key() {
                let (store, _guard) = $fixture;
                for k in ["la/a", "la/b", "la/c"] {
                    store.put(k, Bytes::from_static(b"x")).await.unwrap();
                }
                let page = store
                    .list_after("la/", 10, None, Some("a".to_string()))
                    .await
                    .unwrap();
                assert_eq!(page.items, vec!["b".to_string(), "c".to_string()]);
                assert!(page.next_token.is_none());
            }

            #[tokio::test]
            async fn list_after_paginates_across_page_boundaries() {
                let (store, _guard) = $fixture;
                for k in ["lap/a", "lap/b", "lap/c", "lap/d"] {
                    store.put(k, Bytes::from_static(b"x")).await.unwrap();
                }
                let page = store
                    .list_after("lap/", 2, None, Some("a".to_string()))
                    .await
                    .unwrap();
                assert_eq!(page.items, vec!["b".to_string(), "c".to_string()]);
                // `token` wins over `start_after` on resumed pages.
                let page2 = store
                    .list_after("lap/", 2, page.next_token, Some("a".to_string()))
                    .await
                    .unwrap();
                assert_eq!(page2.items, vec!["d".to_string()]);
                assert!(page2.next_token.is_none());
            }

            #[tokio::test]
            async fn list_missing_prefix_returns_empty() {
                let (store, _guard) = $fixture;
                let page = store.list("void/", 10, None).await.unwrap();
                assert!(page.items.is_empty());
                assert!(page.next_token.is_none());
            }

            #[tokio::test]
            async fn list_children_separates_sub_prefixes_from_objects() {
                let (store, _guard) = $fixture;
                for k in ["lc/obj-a", "lc/sub/b", "lc/sub/c", "lc/obj-d"] {
                    store.put(k, Bytes::from_static(b"x")).await.unwrap();
                }
                let page = store.list_children("lc/", 100, None, None).await.unwrap();
                assert_eq!(page.sub_prefixes, vec!["sub".to_string()]);
                assert_eq!(page.objects, vec!["obj-a".to_string(), "obj-d".to_string()]);
                assert!(page.next_token.is_none());
            }

            #[tokio::test]
            async fn list_children_paginates_via_opaque_token() {
                let (store, _guard) = $fixture;
                for k in ["pc/a", "pc/b", "pc/c"] {
                    store.put(k, Bytes::from_static(b"x")).await.unwrap();
                }
                let page = store.list_children("pc/", 2, None, None).await.unwrap();
                assert_eq!(page.objects, vec!["a".to_string(), "b".to_string()]);
                let page2 = store
                    .list_children("pc/", 2, page.next_token, None)
                    .await
                    .unwrap();
                assert_eq!(page2.objects, vec!["c".to_string()]);
                assert!(page2.next_token.is_none());
            }

            #[tokio::test]
            async fn list_children_respects_start_after() {
                let (store, _guard) = $fixture;
                for k in ["sa/a", "sa/b", "sa/c"] {
                    store.put(k, Bytes::from_static(b"x")).await.unwrap();
                }
                let page = store
                    .list_children("sa/", 10, None, Some("a".to_string()))
                    .await
                    .unwrap();
                assert_eq!(page.objects, vec!["b".to_string(), "c".to_string()]);
            }

            #[tokio::test]
            async fn list_children_start_after_spans_directory_children() {
                // `start_after` names a directory child, and `v1.2`/`v1.5` sort
                // between `v1` and `v1/`: a raw-key bound re-emits `v1` forever
                // and skips the two siblings.
                let (store, _guard) = $fixture;
                for key in ["sa2/v1/leaf", "sa2/v1.2/leaf"] {
                    store.put(key, Bytes::from_static(b"x")).await.unwrap();
                }
                for key in ["sa2/v1.5", "sa2/v2"] {
                    store.put(key, Bytes::from_static(b"x")).await.unwrap();
                }

                let page = store
                    .list_children("sa2/", 10, None, Some("v1".to_string()))
                    .await
                    .unwrap();

                let mut children = page.sub_prefixes;
                children.extend(page.objects);
                children.sort();
                assert_eq!(children, vec!["v1.2", "v1.5", "v2"]);
            }

            #[tokio::test]
            async fn list_all_children_is_complete_across_prefix_families() {
                let (store, _guard) = $fixture;
                // Names where one is a prefix of another continuing with a
                // byte below '/': raw S3 key order and bare-name order
                // disagree for exactly these, so they pin the completeness of
                // any range-partitioned enumeration.
                let dirs = ["v", "v1", "v1-rc", "v1.2", "v10", "v2", "x"];
                for name in dirs {
                    store
                        .put(&format!("fam/{name}/leaf"), Bytes::from_static(b"x"))
                        .await
                        .unwrap();
                }
                store
                    .put("fam/manifest", Bytes::from_static(b"m"))
                    .await
                    .unwrap();

                let children = store.list_all_children("fam").await.unwrap();
                let mut sub_prefixes = children.sub_prefixes;
                let objects = children.objects;
                sub_prefixes.sort();
                assert_eq!(sub_prefixes, dirs);
                assert_eq!(objects, vec!["manifest".to_string()]);
            }

            #[tokio::test]
            async fn list_all_children_missing_prefix_returns_empty() {
                let (store, _guard) = $fixture;
                let children = store.list_all_children("does/not/exist").await.unwrap();
                let sub_prefixes = children.sub_prefixes;
                let objects = children.objects;
                assert!(sub_prefixes.is_empty());
                assert!(objects.is_empty());
            }

            #[tokio::test]
            async fn copy_duplicates_object() {
                let (store, _guard) = $fixture;
                store
                    .put("cp/src", Bytes::from_static(b"payload"))
                    .await
                    .unwrap();
                assert_eq!(store.copy("cp/src", "cp/dst").await.unwrap(), 7);
                assert_eq!(store.get("cp/dst").await.unwrap(), b"payload");
                assert_eq!(store.get("cp/src").await.unwrap(), b"payload");
            }

            #[tokio::test]
            async fn copy_missing_source_returns_not_found() {
                let (store, _guard) = $fixture;
                assert_eq!(
                    store.copy("cp/absent", "cp/dst").await.unwrap_err(),
                    Error::NotFound
                );
            }

            #[tokio::test]
            async fn move_object_relocates_and_removes_source() {
                let (store, _guard) = $fixture;
                store
                    .put("mv/src", Bytes::from_static(b"payload"))
                    .await
                    .unwrap();
                store.move_object("mv/src", "mv/dst").await.unwrap();
                assert_eq!(store.get("mv/dst").await.unwrap(), b"payload");
                assert_eq!(store.head("mv/src").await.unwrap_err(), Error::NotFound);
            }

            #[tokio::test]
            async fn upload_round_trip_appends_across_calls() {
                // Uploads are keyed: there is no caller-held handle, so a
                // second write addressed at the same key picks up where the
                // first left off.
                let (store, _guard) = $fixture;
                store.create_upload("up/blob").await.unwrap();
                assert_eq!(
                    store
                        .write_upload("up/blob", frame("hello "), Some(6))
                        .await
                        .unwrap(),
                    6
                );
                assert_eq!(
                    store
                        .write_upload("up/blob", frame("world"), Some(5))
                        .await
                        .unwrap(),
                    11
                );
                store.complete_upload("up/blob").await.unwrap();
                assert_eq!(store.get("up/blob").await.unwrap(), b"hello world");
            }

            #[tokio::test]
            async fn upload_complete_with_no_writes_creates_empty_object() {
                let (store, _guard) = $fixture;
                store.create_upload("up/empty").await.unwrap();
                store.complete_upload("up/empty").await.unwrap();
                assert_eq!(store.get("up/empty").await.unwrap(), b"");
            }

            #[tokio::test]
            async fn upload_complete_rerun_preserves_object() {
                // Re-running `complete_upload` on an already-finalized upload
                // is a no-op and must not overwrite the assembled object with
                // an empty one.
                let (store, _guard) = $fixture;
                store.create_upload("up/rerun").await.unwrap();
                store
                    .write_upload("up/rerun", frame("payload"), Some(7))
                    .await
                    .unwrap();
                store.complete_upload("up/rerun").await.unwrap();
                assert_eq!(store.get("up/rerun").await.unwrap(), b"payload");

                store.complete_upload("up/rerun").await.unwrap();
                assert_eq!(store.get("up/rerun").await.unwrap(), b"payload");
            }

            #[tokio::test]
            async fn upload_rejects_a_body_longer_than_declared() {
                // `Some(len)` declares an exact count, so a longer body is an
                // error on every backend rather than a silent truncation.
                let (store, _guard) = $fixture;
                store.create_upload("up/long").await.unwrap();
                let error = store
                    .write_upload("up/long", frame("hello world"), Some(5))
                    .await
                    .expect_err("a body longer than declared must be rejected");
                let message = error.to_string();
                assert!(
                    message.contains("body") && message.contains('5'),
                    "the error must name the declared length, got: {message}"
                );
            }

            #[tokio::test]
            async fn upload_abort_leaves_no_object() {
                let (store, _guard) = $fixture;
                store.create_upload("up/abort").await.unwrap();
                store
                    .write_upload("up/abort", frame("partial"), Some(7))
                    .await
                    .unwrap();
                store.abort_upload("up/abort").await.unwrap();
                assert_eq!(store.get("up/abort").await.unwrap_err(), Error::NotFound);
            }

            #[tokio::test]
            async fn upload_chunked_writes_append_across_calls() {
                // `None` streams the body to EOF (a chunked request with no
                // declared length); successive chunked writes append, with the
                // running total growing by each frame's actual size.
                let (store, _guard) = $fixture;
                store.create_upload("up/chunked").await.unwrap();
                assert_eq!(
                    store
                        .write_upload("up/chunked", frame("hello "), None)
                        .await
                        .unwrap(),
                    6
                );
                assert_eq!(
                    store
                        .write_upload("up/chunked", frame("world"), None)
                        .await
                        .unwrap(),
                    11
                );
                store.complete_upload("up/chunked").await.unwrap();
                assert_eq!(store.get("up/chunked").await.unwrap(), b"hello world");
            }

            #[tokio::test]
            async fn upload_chunked_empty_write_returns_committed_size() {
                // An empty chunked frame appends nothing, so it reports the
                // size already committed by the prior write and the object
                // still round-trips.
                let (store, _guard) = $fixture;
                store.create_upload("up/chunked-empty").await.unwrap();
                store
                    .write_upload("up/chunked-empty", frame("data"), None)
                    .await
                    .unwrap();
                assert_eq!(
                    store
                        .write_upload("up/chunked-empty", frame(""), None)
                        .await
                        .unwrap(),
                    4
                );
                store.complete_upload("up/chunked-empty").await.unwrap();
                assert_eq!(store.get("up/chunked-empty").await.unwrap(), b"data");
            }
        }
    };
}

pub(crate) use object_store_conformance;
