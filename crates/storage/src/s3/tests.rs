//! S3 backend tests, against a live rustfs at `ANGOS_TEST_S3_ENDPOINT`
//! (default `http://127.0.0.1:9000`) unless they mock it.

use std::sync::Arc;
use std::time::Duration;

use angos_s3_client::test_util::{integration_config, mock_config};
use angos_s3_client::{
    Backend as S3Backend, BackendConfig as S3Config, Error as S3Error, UploadedPart,
};
use bytes::Bytes;
use bytesize::ByteSize;
use futures_util::{StreamExt, TryStreamExt, stream};
use uuid::Uuid;
use wiremock::matchers::{body_bytes, header_exists, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{
    MAX_PART_SIZE, MIN_PART_SIZE, PartPlan, next_part_number, plan_known_length_parts, staged_key,
};
use crate::s3::Backend;
use crate::test_util::frame;
use crate::tests::object_store_conformance;
use crate::{ObjectStore, PresignedStore};

fn backend() -> Backend {
    backend_with(false, ByteSize::mib(5).as_u64())
}

fn backend_with(uniform_parts: bool, part_size: u64) -> Backend {
    let config = S3Config {
        multipart_copy_threshold: ByteSize::mib(5),
        multipart_copy_chunk_size: ByteSize::mib(5),
        multipart_part_size: ByteSize(part_size),
        ..integration_config(format!("storage-s3-tests/{}", Uuid::new_v4()))
    };
    let client = Arc::new(S3Backend::new(&config).expect("s3 client"));
    Backend::builder(client)
        .part_size(part_size)
        .uniform_parts(uniform_parts)
        .build()
}

object_store_conformance!((backend(), ()));

/// Over a page of children sharing a first character forces the re-split; the
/// boundary-named `v` dir and `v0` object pin the range edges (`v0` has no
/// same-named dir, as rustfs drops such a prefix). Compared against a serial
/// listing, since rustfs's `ListObjectsV2` can omit existing keys.
#[tokio::test]
async fn list_all_children_partitions_skewed_names_completely() {
    let store = backend();

    let mut dirs: Vec<String> = (0..1100).map(|i| format!("v{i:04}")).collect();
    dirs.extend(["v", "va", "va-x", "va.y", "vaz"].map(str::to_string));
    stream::iter(dirs.iter().cloned().map(|name| {
        let store = store.clone();
        async move {
            store
                .put(&format!("skew/{name}/leaf"), Bytes::from_static(b"x"))
                .await
                .unwrap();
        }
    }))
    .buffer_unordered(64)
    .collect::<Vec<_>>()
    .await;
    store
        .put("skew/v0", Bytes::from_static(b"o"))
        .await
        .unwrap();

    let children = store.list_all_children("skew").await.unwrap();
    let mut sub_prefixes = children.sub_prefixes;
    let mut objects = children.objects;
    let (mut plain_prefixes, mut plain_objects) = plain_list_children(&store, "skew").await;
    sub_prefixes.sort();
    objects.sort();
    plain_prefixes.sort();
    plain_objects.sort();
    assert_eq!(sub_prefixes, plain_prefixes);
    assert_eq!(objects, plain_objects);

    assert!(
        plain_prefixes.len() > 1000,
        "the fixture must exceed one page, got {}",
        plain_prefixes.len()
    );
    assert_eq!(plain_objects, ["v0"]);
}

async fn plain_list_children(store: &Backend, prefix: &str) -> (Vec<String>, Vec<String>) {
    let mut sub_prefixes = Vec::new();
    let mut objects = Vec::new();
    let mut token = None;
    loop {
        let page = store
            .list_children(prefix, 1000, token, None)
            .await
            .unwrap();
        sub_prefixes.extend(page.sub_prefixes);
        objects.extend(page.objects);
        match page.next_token {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    (sub_prefixes, objects)
}

/// The flat fan-out matches a serial listing (not the put set, see above), with
/// boundary-named keys and keys in the open end ranges.
#[tokio::test]
async fn list_all_matches_a_plain_serial_list() {
    let store = backend();

    let mut keys: Vec<String> = (0..1100).map(|i| format!("data/{i:04}/blob")).collect();
    keys.extend(
        [
            "0", "0z", "9", "A", "Z", "_", "a", "m", "m0", "z", "zz", "~edge",
        ]
        .map(str::to_string),
    );
    stream::iter(keys.iter().map(|key| {
        let store = store.clone();
        let key = format!("flat/{key}");
        async move { store.put(&key, Bytes::from_static(b"x")).await.unwrap() }
    }))
    .buffer_unordered(64)
    .collect::<Vec<_>>()
    .await;

    let mut plain: Vec<String> = store.list_in_order("flat").try_collect().await.unwrap();
    let mut scanned: Vec<String> = store.list_all("flat").try_collect().await.unwrap();
    plain.sort();
    scanned.sort();
    assert_eq!(scanned, plain);
}

async fn committed_part_sizes(store: &Backend, key: &str) -> Vec<u64> {
    let uploads = store
        .client
        .list_multipart_uploads(key, None, None)
        .await
        .unwrap()
        .uploads;
    match uploads.into_iter().find(|u| u.key == key) {
        Some(u) => store
            .client
            .list_parts(key, &u.upload_id)
            .await
            .unwrap()
            .iter()
            .map(|p| p.size)
            .collect(),
        None => Vec::new(),
    }
}

async fn has_open_multipart(store: &Backend, key: &str) -> bool {
    let uploads = store
        .client
        .list_multipart_uploads(key, None, None)
        .await
        .unwrap()
        .uploads;
    uploads.into_iter().any(|u| u.key == key)
}

#[tokio::test]
async fn upload_uniform_round_trip() {
    let store = backend_with(true, 5 * 1024 * 1024);
    let key = format!("up/uniform/{}/data", Uuid::new_v4());

    let chunks: Vec<Vec<u8>> = vec![
        vec![0x41; 2 * 1024 * 1024],
        vec![0x42; 4 * 1024 * 1024],
        vec![0x43; 6 * 1024 * 1024],
    ];
    let mut total = 0u64;
    for chunk in &chunks {
        let len = chunk.len() as u64;
        total = store
            .write_upload(&key, frame(chunk), Some(len))
            .await
            .unwrap();
    }
    let expected_total: u64 = chunks.iter().map(|c| c.len() as u64).sum();
    assert_eq!(total, expected_total);
    store.complete_upload(&key).await.unwrap();
    let assembled = store.get(&key).await.unwrap();
    assert_eq!(assembled.len() as u64, expected_total);
    let mut expected = Vec::with_capacity(assembled.len());
    for chunk in &chunks {
        expected.extend_from_slice(chunk);
    }
    assert_eq!(assembled, expected);
}

/// A chunked (`None`) upload, as `docker push` sends, flushes parts of the
/// configured `part_size` and stages the tail.
#[tokio::test]
async fn upload_unknown_length_emits_part_size_parts() {
    const PART: u64 = 10 * 1024 * 1024;
    let store = backend_with(false, PART);
    let key = format!("up/chunked-partsize/{}/data", Uuid::new_v4());
    // Two 10 MiB parts and a 3 MiB remainder.
    let data: Vec<u8> = (0..23 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();

    let total = store.write_upload(&key, frame(&data), None).await.unwrap();
    assert_eq!(total, data.len() as u64);
    assert_eq!(
        committed_part_sizes(&store, &key).await,
        vec![PART, PART],
        "chunked uploads flush part_size parts, not 5 MiB parts"
    );
    store.complete_upload(&key).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), data);
}

#[tokio::test]
async fn upload_uniform_mixed_known_then_chunked_stays_uniform() {
    const PART: u64 = 10 * 1024 * 1024;
    let store = backend_with(true, PART);
    let key = format!("up/uniform-mixed/{}/data", Uuid::new_v4());
    let first: Vec<u8> = (0..PART).map(|i| (i % 251) as u8).collect();
    let second: Vec<u8> = (0..23 * 1024 * 1024u32).map(|i| (i % 241) as u8).collect();

    let first_len = first.len() as u64;
    store
        .write_upload(&key, frame(&first), Some(first_len))
        .await
        .unwrap();
    let total = store
        .write_upload(&key, frame(&second), None)
        .await
        .unwrap();
    assert_eq!(total, (first.len() + second.len()) as u64);

    let sizes = committed_part_sizes(&store, &key).await;
    assert!(
        sizes.iter().all(|&s| s == PART),
        "mixing a known-length and a chunked PATCH must keep all non-final parts at part_size, got {sizes:?}"
    );

    store.complete_upload(&key).await.unwrap();
    let mut expected = first;
    expected.extend_from_slice(&second);
    assert_eq!(store.get(&key).await.unwrap(), expected);
}

/// The staged tail of a known-length write folds into the chunked one.
#[tokio::test]
async fn upload_nonuniform_known_then_chunked_round_trips() {
    const PART: u64 = 10 * 1024 * 1024;
    let store = backend_with(false, PART);
    let key = format!("up/nonuniform-cross/{}/data", Uuid::new_v4());
    let first: Vec<u8> = (0..7 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    let second: Vec<u8> = (0..13 * 1024 * 1024u32).map(|i| (i % 241) as u8).collect();

    let first_len = first.len() as u64;
    store
        .write_upload(&key, frame(&first), Some(first_len))
        .await
        .unwrap();
    let total = store
        .write_upload(&key, frame(&second), None)
        .await
        .unwrap();
    assert_eq!(total, (first.len() + second.len()) as u64);

    store.complete_upload(&key).await.unwrap();
    let mut expected = first;
    expected.extend_from_slice(&second);
    assert_eq!(store.get(&key).await.unwrap(), expected);
}

#[tokio::test]
async fn upload_unknown_length_small_chunked_body_round_trips() {
    const PART: u64 = 10 * 1024 * 1024;
    let store = backend_with(false, PART);
    let key = format!("up/chunked-small/{}/data", Uuid::new_v4());
    let data: Vec<u8> = (0..1024 * 1024u32).map(|i| (i % 251) as u8).collect();

    let total = store.write_upload(&key, frame(&data), None).await.unwrap();
    assert_eq!(total, data.len() as u64);
    assert!(
        !has_open_multipart(&store, &key).await,
        "a sub-floor chunked body must not open a main multipart upload"
    );
    assert_eq!(
        store
            .client
            .head_object(&staged_key(&key, 0))
            .await
            .unwrap()
            .size,
        data.len() as u64,
        "the body must be staged directly at the committed offset"
    );

    store.complete_upload(&key).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), data);
}

#[tokio::test]
async fn upload_unknown_length_resumes_across_writes() {
    let store = backend_with(true, 5 * 1024 * 1024);
    let key = format!("up/chunked-resume/{}/data", Uuid::new_v4());
    let first: Vec<u8> = vec![0x41; 7 * 1024 * 1024]; // 1 part + 2 MiB staged
    let second: Vec<u8> = vec![0x42; 6 * 1024 * 1024]; // combined 8 MiB staged -> 1 part + 3 MiB

    store.write_upload(&key, frame(&first), None).await.unwrap();
    let total = store
        .write_upload(&key, frame(&second), None)
        .await
        .unwrap();
    assert_eq!(total, (first.len() + second.len()) as u64);
    store.complete_upload(&key).await.unwrap();

    let mut expected = first.clone();
    expected.extend_from_slice(&second);
    assert_eq!(store.get(&key).await.unwrap(), expected);
}

/// Non-uniform mode flushes at the configured `part_size`, not the 5 MiB floor.
#[tokio::test]
async fn upload_nonuniform_flushes_at_configured_part_size_not_min() {
    const PART_SIZE: u64 = 8 * 1024 * 1024;
    let store = backend_with(false, PART_SIZE);
    let key = format!("up/nonuniform-cfg/{}/data", Uuid::new_v4());

    let first: Vec<u8> = (0..6 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    let first_len = first.len() as u64;
    let total = store
        .write_upload(&key, frame(&first), Some(first_len))
        .await
        .unwrap();
    assert_eq!(
        total, first_len,
        "total tracks staged bytes below the threshold"
    );

    assert_eq!(
        committed_part_sizes(&store, &key).await.len(),
        0,
        "6 MiB < 8 MiB part_size: no part may be emitted yet"
    );
    assert_eq!(
        store
            .client
            .head_object(&staged_key(&key, 0))
            .await
            .unwrap()
            .size,
        first_len,
        "all bytes must remain staged below the configured threshold"
    );
    assert!(
        !has_open_multipart(&store, &key).await,
        "no multipart upload should open before the first flush"
    );

    let second: Vec<u8> = (0..3 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    let second_len = second.len() as u64;
    store
        .write_upload(&key, frame(&second), Some(second_len))
        .await
        .unwrap();

    assert_eq!(
        committed_part_sizes(&store, &key).await.len(),
        1,
        "crossing part_size must emit exactly one part"
    );
    assert!(
        has_open_multipart(&store, &key).await,
        "a multipart upload must now be open"
    );

    store.complete_upload(&key).await.unwrap();
    let assembled = store.get(&key).await.unwrap();
    let mut expected = first;
    expected.extend_from_slice(&second);
    assert_eq!(assembled, expected);
}

#[tokio::test]
async fn upload_abort_removes_orphans_and_staged() {
    let store = backend();
    let prefix = format!("up/abort/{}", Uuid::new_v4());
    let key = format!("{prefix}/data");

    store.client.create_multipart_upload(&key).await.unwrap();
    store.client.create_multipart_upload(&key).await.unwrap();
    store
        .client
        .put_object(&staged_key(&key, 0), Bytes::from_static(b"leftover"))
        .await
        .unwrap();

    store.abort_upload(&key).await.unwrap();

    let remaining = store
        .client
        .list_multipart_uploads(&key, None, None)
        .await
        .unwrap()
        .uploads;
    assert!(remaining.iter().all(|u| u.key != key));
    assert!(
        matches!(
            store
                .client
                .head_object(&staged_key(&key, 0))
                .await
                .unwrap_err(),
            S3Error::NotFound(_)
        ),
        "staged remainder must be deleted by abort"
    );
    store.delete_prefix(&prefix).await.unwrap();
}

#[tokio::test]
async fn presign_get_returns_a_url() {
    let store = backend();
    let url = store
        .presign_get("blob/x", Duration::from_mins(1), None)
        .await
        .unwrap();
    assert!(
        url.contains("blob/x") && url.contains("X-Amz-Signature"),
        "expected a SigV4 presigned URL, got: {url}",
    );
}

/// Two prune runs sharing a bucket race to abort the same upload.
#[tokio::test]
async fn abort_upload_tolerates_an_upload_a_peer_already_aborted() {
    let server = MockServer::start().await;
    let key = "blob/pending";
    let config = S3Config {
        key_prefix: "abort-race".to_string(),
        ..mock_config(server.uri())
    };

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListMultipartUploadsResult>
              <IsTruncated>false</IsTruncated>
              <Upload>
                <Key>{}/{key}</Key>
                <UploadId>upload-1</UploadId>
                <Initiated>2026-01-01T00:00:00Z</Initiated>
              </Upload>
            </ListMultipartUploadsResult>"#,
            config.key_prefix
        )))
        .mount(&server)
        .await;

    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(404).set_body_string(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <Error><Code>NoSuchUpload</Code></Error>"#,
        ))
        .mount(&server)
        .await;

    let client = Arc::new(S3Backend::new(&config).expect("s3 client"));
    let backend = Backend::builder(client).build();

    backend
        .abort_upload(key)
        .await
        .expect("an upload a peer already aborted must count as aborted");
}

#[test]
fn known_length_parts_stay_within_the_s3_part_bounds() {
    let cases = [
        // (uniform, part_size, available)
        (false, MIN_PART_SIZE, 1024),
        (false, MIN_PART_SIZE, MIN_PART_SIZE),
        (false, MIN_PART_SIZE, MAX_PART_SIZE),
        (false, MIN_PART_SIZE, MAX_PART_SIZE + 1),
        (false, MIN_PART_SIZE, 100 * 1024 * 1024 * 1024),
        (true, MIN_PART_SIZE, 100 * 1024 * 1024 * 1024),
    ];

    for (uniform, part_size, available) in cases {
        let PartPlan {
            count,
            emit_size,
            restaged,
        } = plan_known_length_parts(uniform, part_size, available);
        assert_eq!(
            count * emit_size + restaged,
            available,
            "{uniform}/{part_size}/{available} must account for every byte"
        );
        if count > 0 {
            assert!(
                (MIN_PART_SIZE..=MAX_PART_SIZE).contains(&emit_size),
                "{uniform}/{part_size}/{available} emitted a {emit_size}-byte part"
            );
        }
    }
}

/// The remainder is buffered in memory, so it must stay small.
#[test]
fn splitting_a_huge_append_leaves_a_small_remainder() {
    let PartPlan {
        count, restaged, ..
    } = plan_known_length_parts(false, MIN_PART_SIZE, 100 * 1024 * 1024 * 1024);
    assert!(
        restaged < count,
        "restaging {restaged} bytes defeats the point of splitting"
    );
}

#[test]
fn part_size_below_the_s3_floor_is_raised_to_it() {
    let client =
        Arc::new(S3Backend::new(&mock_config("http://127.0.0.1:1".to_owned())).expect("s3 client"));
    let backend = Backend::builder(client).part_size(1024).build();

    let plan = plan_known_length_parts(true, backend.part_size, MIN_PART_SIZE * 3);
    assert_eq!(plan.emit_size, MIN_PART_SIZE);
}

/// Without this guard the 10,001st part streams in full before S3 refuses it.
#[test]
fn part_number_past_the_s3_limit_is_refused() {
    let part = |n: u32| UploadedPart {
        part_number: n,
        e_tag: String::new(),
        size: MIN_PART_SIZE,
    };
    let mut parts: Vec<UploadedPart> = (1..=9_999).map(part).collect();

    assert_eq!(
        next_part_number(&parts).expect("10,000th part fits"),
        10_000
    );

    parts.push(part(10_000));
    let error = next_part_number(&parts).expect_err("the 10,001st part must be refused");
    assert!(error.to_string().contains("10000"), "got: {error}");
}

/// The request bounds on the bare name, and the directory child of that name
/// is dropped rather than re-emitted forever.
#[tokio::test]
async fn list_children_bounds_on_the_bare_name_and_drops_the_named_child() {
    let server = MockServer::start().await;
    let config = S3Config {
        key_prefix: "kp".to_string(),
        ..mock_config(server.uri())
    };

    Mock::given(method("GET"))
        // Not `kp/sa/v1/`: that skips `v1.2`, which sorts before it.
        .and(query_param("start-after", "kp/sa/v1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult>
              <IsTruncated>false</IsTruncated>
              <CommonPrefixes><Prefix>kp/sa/v1/</Prefix></CommonPrefixes>
              <CommonPrefixes><Prefix>kp/sa/v1.2/</Prefix></CommonPrefixes>
              <Contents><Key>kp/sa/v2</Key></Contents>
            </ListBucketResult>"#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    let client = Arc::new(S3Backend::new(&config).expect("s3 client"));
    let page = Backend::builder(client)
        .build()
        .list_children("sa", 10, None, Some("v1".to_string()))
        .await
        .expect("listing must succeed");

    assert_eq!(
        page.sub_prefixes,
        vec!["v1.2".to_string()],
        "the directory named by start_after must not be re-emitted"
    );
    assert_eq!(page.objects, vec!["v2".to_string()]);
}

#[tokio::test]
async fn complete_upload_puts_a_small_upload_without_a_copy() {
    let server = MockServer::start().await;
    let config = S3Config {
        key_prefix: "pu".to_string(),
        ..mock_config(server.uri())
    };

    Mock::given(header_exists("x-amz-copy-source"))
        .respond_with(ResponseTemplate::new(500))
        .with_priority(1)
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/test-bucket"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListMultipartUploadsResult>
              <IsTruncated>false</IsTruncated>
            </ListMultipartUploadsResult>"#,
        ))
        .mount(&server)
        .await;
    Mock::given(method("HEAD"))
        .and(path("/test-bucket/pu/blob/staged/0"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-length", "5"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/test-bucket/pu/blob/staged/0"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello".to_vec()))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/test-bucket/pu/blob/data"))
        .and(body_bytes(b"hello".to_vec()))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let client = Arc::new(S3Backend::new(&config).expect("s3 client"));
    Backend::builder(client)
        .build()
        .complete_upload("blob/data")
        .await
        .expect("a small upload must complete");
}
