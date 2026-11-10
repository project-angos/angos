/// Stamps out the [`ObjectStore`](crate::ObjectStore) contract tests for one
/// backend. `$fixture` is evaluated in every test, inside a child module, and
/// yields `(store, guard)`, the guard living as long as the test.
macro_rules! object_store_conformance {
    ($fixture:expr) => {
        mod object_store_suite {
            use bytes::Bytes;
            use tokio::io::AsyncReadExt;

            use super::*;
            use crate::test_util::frame;
            use crate::{Children, Error, ObjectStore};

            #[tokio::test]
            async fn put_overwrites_existing_object() {
                let (store, _guard) = $fixture;
                store
                    .put("ow/a/k", Bytes::from_static(b"v1"))
                    .await
                    .unwrap();
                store
                    .put("ow/a/k", Bytes::from_static(b"v2"))
                    .await
                    .unwrap();
                assert_eq!(store.get("ow/a/k").await.unwrap(), b"v2");
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
            async fn a_missing_key_reads_not_found() {
                let (store, _guard) = $fixture;
                assert_eq!(store.get("missing").await.unwrap_err(), Error::NotFound);
                assert_eq!(store.head("missing").await.unwrap_err(), Error::NotFound);
                assert_eq!(
                    store.copy("missing", "cp/dst").await.unwrap_err(),
                    Error::NotFound
                );
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
            async fn delete_is_idempotent() {
                let (store, _guard) = $fixture;
                store.put("del/k", Bytes::from_static(b"v")).await.unwrap();
                store.delete("del/k").await.unwrap();
                assert_eq!(store.get("del/k").await.unwrap_err(), Error::NotFound);
                store.delete("del/k").await.unwrap();
            }

            #[tokio::test]
            async fn delete_prefix_is_directory_scoped_not_string_prefix() {
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

                let mut children = store.list_all_children("fam").await.unwrap();
                children.sub_prefixes.sort();
                assert_eq!(children.sub_prefixes, dirs);
                assert_eq!(children.objects, vec!["manifest".to_string()]);
            }

            #[tokio::test]
            async fn list_all_children_missing_prefix_returns_empty() {
                let (store, _guard) = $fixture;
                let children = store.list_all_children("does/not/exist").await.unwrap();
                assert_eq!(children, Children::default());
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
                let (store, _guard) = $fixture;
                assert_eq!(
                    store
                        .write_upload("up/blob", frame(b"hello "), Some(6))
                        .await
                        .unwrap(),
                    6
                );
                assert_eq!(
                    store
                        .write_upload("up/blob", frame(b"world"), Some(5))
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
                store.complete_upload("up/empty").await.unwrap();
                assert_eq!(store.get("up/empty").await.unwrap(), b"");
            }

            #[tokio::test]
            async fn upload_complete_rerun_preserves_object() {
                let (store, _guard) = $fixture;
                store
                    .write_upload("up/rerun", frame(b"payload"), Some(7))
                    .await
                    .unwrap();
                store.complete_upload("up/rerun").await.unwrap();
                assert_eq!(store.get("up/rerun").await.unwrap(), b"payload");

                store.complete_upload("up/rerun").await.unwrap();
                assert_eq!(store.get("up/rerun").await.unwrap(), b"payload");
            }

            #[tokio::test]
            async fn upload_rejects_a_body_longer_than_declared() {
                let (store, _guard) = $fixture;
                let error = store
                    .write_upload("up/long", frame(b"hello world"), Some(5))
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
                store
                    .write_upload("up/abort", frame(b"partial"), Some(7))
                    .await
                    .unwrap();
                store.abort_upload("up/abort").await.unwrap();
                assert_eq!(store.get("up/abort").await.unwrap_err(), Error::NotFound);
            }

            #[tokio::test]
            async fn upload_chunked_writes_append_across_calls() {
                let (store, _guard) = $fixture;
                assert_eq!(
                    store
                        .write_upload("up/chunked", frame(b"hello "), None)
                        .await
                        .unwrap(),
                    6
                );
                assert_eq!(
                    store
                        .write_upload("up/chunked", frame(b"world"), None)
                        .await
                        .unwrap(),
                    11
                );
                store.complete_upload("up/chunked").await.unwrap();
                assert_eq!(store.get("up/chunked").await.unwrap(), b"hello world");
            }

            #[tokio::test]
            async fn upload_chunked_empty_write_returns_committed_size() {
                let (store, _guard) = $fixture;
                store
                    .write_upload("up/chunked-empty", frame(b"data"), None)
                    .await
                    .unwrap();
                assert_eq!(
                    store
                        .write_upload("up/chunked-empty", frame(b""), None)
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
