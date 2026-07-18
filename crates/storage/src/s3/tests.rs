//! Integration tests for the S3 backend. These talk to a live rustfs
//! instance (the workspace-wide S3 test backend, in CI and locally) at
//! `ANGOS_TEST_S3_ENDPOINT`, defaulting to `http://127.0.0.1:9000` as CI
//! provides. The backend is expected to be running; tests fail loudly
//! otherwise.
//!
//! The trait contracts are covered by the shared conformance suites
//! instantiated below; the tests in this file pin S3-specific behaviour:
//! multipart part sizing, staged remainders, presigned URLs, and `ETag`
//! surfacing.

use std::{sync::Arc, time::Duration};

use angos_s3_client::{
    Backend as S3Backend, BackendConfig as S3Config, test_util::integration_config,
};
use bytes::Bytes;
use bytesize::ByteSize;
use futures_util::{StreamExt, stream};
use uuid::Uuid;

use super::staged_key;
use crate::test_util::frame;
use crate::tests::{conditional_store_conformance, object_store_conformance};
use crate::{ObjectStore, PresignedStore, s3::Backend};

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

conditional_store_conformance!((backend(), ()));

/// More than one page of children all starting with the same character drives
/// the whole partitioned enumeration: the probe truncates, the `v` range
/// re-splits on its second character, the boundary-named dir (`v`) and object
/// (`v0`) pin the exclusive-start bookkeeping between adjacent ranges. The
/// boundary object deliberately has no same-named dir: rustfs merges a
/// same-named object and prefix, dropping the prefix from any listing.
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

    let (mut sub_prefixes, objects) = store.list_all_children("skew").await.unwrap();
    sub_prefixes.sort();
    dirs.sort();
    assert_eq!(sub_prefixes, dirs);
    assert_eq!(objects, ["v0"]);
}

#[tokio::test]
async fn head_surfaces_etag() {
    let store = backend();
    store
        .put("hd/k", Bytes::from_static(b"abcdef"))
        .await
        .unwrap();
    let meta = store.head("hd/k").await.unwrap();
    assert!(meta.etag.is_some(), "S3 always surfaces an ETag");
}

// uploads

/// Number of committed parts for the in-flight multipart upload at `key`, or 0
/// when no multipart upload is open.
async fn committed_part_count(store: &Backend, key: &str) -> usize {
    committed_part_sizes(store, key).await.len()
}

/// Sizes of the committed parts of the open multipart at `key`, in order.
async fn committed_part_sizes(store: &Backend, key: &str) -> Vec<u64> {
    let (uploads, _, _) = store
        .client
        .list_multipart_uploads(Some(key), None, None)
        .await
        .unwrap();
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

/// Whether an in-flight multipart upload exists at `key`.
async fn has_open_multipart(store: &Backend, key: &str) -> bool {
    let (uploads, _, _) = store
        .client
        .list_multipart_uploads(Some(key), None, None)
        .await
        .unwrap();
    uploads.into_iter().any(|u| u.key == key)
}

/// Uniform mode: each `write_upload` emits as many fixed-size parts as fit
/// in the combined (staged + incoming) bytes, then restages the remainder.
#[tokio::test]
async fn upload_uniform_round_trip() {
    let store = backend_with(true, 5 * 1024 * 1024);
    let key = format!("up/uniform/{}/data", Uuid::new_v4());

    let chunks: Vec<Vec<u8>> = vec![
        vec![0x41; 2 * 1024 * 1024],
        vec![0x42; 4 * 1024 * 1024],
        vec![0x43; 6 * 1024 * 1024],
    ];
    store.create_upload(&key).await.unwrap();
    let mut total = 0u64;
    for chunk in &chunks {
        let len = chunk.len() as u64;
        total = store
            .write_upload(&key, frame(chunk.clone()), Some(len))
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

/// Unknown-length upload (`None`): a chunked request with no `Content-Length`
/// streams the body to EOF, flushing whole parts and restaging the trailing
/// remainder. This is the `docker push` code path.
#[tokio::test]
async fn upload_unknown_length_streams_to_eof() {
    let store = backend_with(true, 5 * 1024 * 1024);
    let key = format!("up/chunked/{}/data", Uuid::new_v4());
    // 13 MiB: two full 5 MiB parts plus a 3 MiB remainder.
    let data: Vec<u8> = (0..13 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();

    store.create_upload(&key).await.unwrap();
    let total = store
        .write_upload(&key, frame(data.clone()), None)
        .await
        .unwrap();
    assert_eq!(total, data.len() as u64);
    assert_eq!(
        committed_part_count(&store, &key).await,
        2,
        "two full 5 MiB parts must flush; the 3 MiB remainder stays staged"
    );
    store.complete_upload(&key).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), data);
}

/// Chunked (`None`) uploads flush `part_size` parts, not 5 MiB parts, when the
/// backend is configured with a larger `part_size`.
#[tokio::test]
async fn upload_unknown_length_emits_part_size_parts() {
    // part_size above the 5 MiB floor: the chunked path buffers up to part_size
    // and flushes part_size parts, honoring the configured size.
    const PART: u64 = 10 * 1024 * 1024;
    let store = backend_with(false, PART);
    let key = format!("up/chunked-partsize/{}/data", Uuid::new_v4());
    // 20 MiB = exactly two 10 MiB parts, no remainder.
    let data: Vec<u8> = (0..20 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();

    store.create_upload(&key).await.unwrap();
    let total = store
        .write_upload(&key, frame(data.clone()), None)
        .await
        .unwrap();
    assert_eq!(total, data.len() as u64);
    assert_eq!(
        committed_part_sizes(&store, &key).await,
        vec![PART, PART],
        "chunked uploads flush part_size parts, not 5 MiB parts"
    );
    store.complete_upload(&key).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), data);
}

/// A `part_size` that is not a 5 MiB multiple is still honored exactly: each
/// part seals on `part_size` rather than a 5 MiB boundary.
#[tokio::test]
async fn upload_unknown_length_non_multiple_part_size() {
    const PART: u64 = 12 * 1024 * 1024;
    let store = backend_with(false, PART);
    let key = format!("up/chunked-nonmult/{}/data", Uuid::new_v4());
    // 24 MiB = exactly two 12 MiB parts.
    let data: Vec<u8> = (0..24 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();

    store.create_upload(&key).await.unwrap();
    let total = store
        .write_upload(&key, frame(data.clone()), None)
        .await
        .unwrap();
    assert_eq!(total, data.len() as u64);
    assert_eq!(
        committed_part_sizes(&store, &key).await,
        vec![PART, PART],
        "parts must be exactly part_size (12 MiB), not rounded up to 15 MiB"
    );
    store.complete_upload(&key).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), data);
}

/// Uniform mode never runs the coalescer: a chunked (`None`) push with a
/// configured `part_size` above the 5 MiB floor takes the direct streaming
/// path, emitting uniform `part_size` non-final parts (matching the
/// known-length uniform path) rather than 5 MiB parts or the coalescer's
/// variable parts.
#[tokio::test]
async fn upload_unknown_length_uniform_uses_equal_parts() {
    const PART: u64 = 10 * 1024 * 1024;
    let store = backend_with(true, PART);
    let key = format!("up/chunked-uniform/{}/data", Uuid::new_v4());
    // 23 MiB: two full 10 MiB parts plus a 3 MiB remainder. Two non-final
    // parts prove uniformity rather than a single part trivially equal to
    // itself.
    let data: Vec<u8> = (0..23 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();

    store.create_upload(&key).await.unwrap();
    let total = store
        .write_upload(&key, frame(data.clone()), None)
        .await
        .unwrap();
    assert_eq!(total, data.len() as u64);

    let sizes = committed_part_sizes(&store, &key).await;
    assert!(
        sizes.iter().all(|&s| s == PART),
        "uniform mode must emit equal part_size parts via the direct path, got {sizes:?}"
    );
    assert_eq!(
        sizes,
        vec![PART, PART],
        "each non-final part is part_size (10 MiB), not 5 MiB or a coalescer size"
    );

    store.complete_upload(&key).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), data);
}

/// Regression for the mixed-mode bug: a uniform session that takes a
/// known-length PATCH (`Some`) followed by a chunked PATCH (`None`) on the same
/// key must stay uniform. The chunked direct path now flushes `part_size`
/// parts, so every non-final committed part is `part_size` and the assembled
/// object is the concatenation of both writes.
#[tokio::test]
async fn upload_uniform_mixed_known_then_chunked_stays_uniform() {
    const PART: u64 = 10 * 1024 * 1024;
    let store = backend_with(true, PART);
    let key = format!("up/uniform-mixed/{}/data", Uuid::new_v4());
    // First a known-length part_size body, then a chunked body large enough to
    // flush two more parts plus a remainder.
    let first: Vec<u8> = (0..PART).map(|i| (i % 251) as u8).collect();
    let second: Vec<u8> = (0..23 * 1024 * 1024u32).map(|i| (i % 241) as u8).collect();

    store.create_upload(&key).await.unwrap();
    let first_len = first.len() as u64;
    store
        .write_upload(&key, frame(first.clone()), Some(first_len))
        .await
        .unwrap();
    let total = store
        .write_upload(&key, frame(second.clone()), None)
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

/// Non-uniform cross-mode resume: a known-length PATCH (`Some`) followed by a
/// chunked PATCH (`None`) on the same key, with a `part_size` above the floor,
/// must round-trip byte-for-byte after complete.
#[tokio::test]
async fn upload_nonuniform_known_then_chunked_round_trips() {
    const PART: u64 = 10 * 1024 * 1024;
    let store = backend_with(false, PART);
    let key = format!("up/nonuniform-cross/{}/data", Uuid::new_v4());
    let first: Vec<u8> = (0..7 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    let second: Vec<u8> = (0..13 * 1024 * 1024u32).map(|i| (i % 241) as u8).collect();

    store.create_upload(&key).await.unwrap();
    let first_len = first.len() as u64;
    store
        .write_upload(&key, frame(first.clone()), Some(first_len))
        .await
        .unwrap();
    let total = store
        .write_upload(&key, frame(second.clone()), None)
        .await
        .unwrap();
    assert_eq!(total, (first.len() + second.len()) as u64);

    store.complete_upload(&key).await.unwrap();
    let mut expected = first;
    expected.extend_from_slice(&second);
    assert_eq!(store.get(&key).await.unwrap(), expected);
}

/// An empty chunked body (`None`) on the coalescer path appends nothing: it
/// returns the committed size unchanged, opens neither a main nor a scratch
/// multipart, and `complete_upload` round-trips an empty object.
#[tokio::test]
async fn upload_unknown_length_empty_body_coalesce_path() {
    const PART: u64 = 10 * 1024 * 1024;
    let store = backend_with(false, PART);
    let key = format!("up/chunked-empty-coalesce/{}/data", Uuid::new_v4());

    store.create_upload(&key).await.unwrap();
    let total = store
        .write_upload(&key, frame(Vec::new()), None)
        .await
        .unwrap();
    assert_eq!(total, 0, "an empty chunked body appends nothing");
    assert!(
        !has_open_multipart(&store, &key).await,
        "an empty body must not open a main multipart upload"
    );

    store.complete_upload(&key).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), b"");
}

/// An empty chunked body (`None`) on the direct path (`part_size` at the floor)
/// behaves identically: committed size unchanged, no multipart opened, empty
/// object after complete.
#[tokio::test]
async fn upload_unknown_length_empty_body_direct_path() {
    let store = backend_with(false, 5 * 1024 * 1024);
    let key = format!("up/chunked-empty-direct/{}/data", Uuid::new_v4());

    store.create_upload(&key).await.unwrap();
    let total = store
        .write_upload(&key, frame(Vec::new()), None)
        .await
        .unwrap();
    assert_eq!(total, 0, "an empty chunked body appends nothing");
    assert!(
        !has_open_multipart(&store, &key).await,
        "an empty body must not open a main multipart upload"
    );

    store.complete_upload(&key).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), b"");
}

/// Large chunked write: a body well above `part_size` must still seal exact
/// `part_size` parts. This uses a moderately large materialized body since the
/// test helpers build streams from a `Vec`.
#[tokio::test]
async fn upload_unknown_length_large_body_emits_part_size_parts() {
    const PART: u64 = 10 * 1024 * 1024;
    let store = backend_with(false, PART);
    let key = format!("up/chunked-large/{}/data", Uuid::new_v4());
    // 35 MiB = three 10 MiB parts plus a 5 MiB tail (which seals as a 5 MiB
    // part too, leaving nothing staged).
    let data: Vec<u8> = (0..35 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();

    store.create_upload(&key).await.unwrap();
    let total = store
        .write_upload(&key, frame(data.clone()), None)
        .await
        .unwrap();
    assert_eq!(total, data.len() as u64);

    let sizes = committed_part_sizes(&store, &key).await;
    assert!(
        sizes.iter().take(3).all(|&s| s == PART),
        "the first three parts must each be exactly part_size, got {sizes:?}"
    );

    store.complete_upload(&key).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), data);
}

/// A chunked (`None`) body that is entirely below the 5 MiB floor with a
/// `part_size` above it must short-circuit the scratch multipart and stage the
/// bytes directly, still round-tripping byte-for-byte.
#[tokio::test]
async fn upload_unknown_length_small_chunked_body_round_trips() {
    const PART: u64 = 10 * 1024 * 1024;
    let store = backend_with(false, PART);
    let key = format!("up/chunked-small/{}/data", Uuid::new_v4());
    // 1 MiB: well below the 5 MiB floor, so nothing can flush as a part.
    let data: Vec<u8> = (0..1024 * 1024u32).map(|i| (i % 251) as u8).collect();

    store.create_upload(&key).await.unwrap();
    let total = store
        .write_upload(&key, frame(data.clone()), None)
        .await
        .unwrap();
    assert_eq!(total, data.len() as u64);
    assert!(
        !has_open_multipart(&store, &key).await,
        "a sub-floor chunked body must not open a main multipart upload"
    );
    assert_eq!(
        store
            .client
            .object_size(&staged_key(&key, 0))
            .await
            .unwrap(),
        data.len() as u64,
        "the body must be staged directly at the committed offset"
    );

    store.complete_upload(&key).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), data);
}

/// Non-uniform chunked uploads resume across writes: a sub-floor trailing piece
/// is staged and folded into the next `part_size` part of the following write.
#[tokio::test]
async fn upload_unknown_length_nonuniform_resumes_across_writes() {
    const PART: u64 = 10 * 1024 * 1024;
    let store = backend_with(false, PART);
    let key = format!("up/chunked-nonuniform-resume/{}/data", Uuid::new_v4());
    let first: Vec<u8> = (0..13 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect(); // 1 part + 3 MiB tail
    let second: Vec<u8> = (0..10 * 1024 * 1024u32).map(|i| (i % 241) as u8).collect();

    store.create_upload(&key).await.unwrap();
    store
        .write_upload(&key, frame(first.clone()), None)
        .await
        .unwrap();
    let total = store
        .write_upload(&key, frame(second.clone()), None)
        .await
        .unwrap();
    assert_eq!(total, (first.len() + second.len()) as u64);
    store.complete_upload(&key).await.unwrap();

    let mut expected = first.clone();
    expected.extend_from_slice(&second);
    assert_eq!(store.get(&key).await.unwrap(), expected);
}

/// Two unknown-length writes (multiple chunked PATCH calls) resume across the
/// staged remainder and assemble in order.
#[tokio::test]
async fn upload_unknown_length_resumes_across_writes() {
    let store = backend_with(true, 5 * 1024 * 1024);
    let key = format!("up/chunked-resume/{}/data", Uuid::new_v4());
    let first: Vec<u8> = vec![0x41; 7 * 1024 * 1024]; // 1 part + 2 MiB staged
    let second: Vec<u8> = vec![0x42; 6 * 1024 * 1024]; // combined 8 MiB staged -> 1 part + 3 MiB

    store.create_upload(&key).await.unwrap();
    store
        .write_upload(&key, frame(first.clone()), None)
        .await
        .unwrap();
    let total = store
        .write_upload(&key, frame(second.clone()), None)
        .await
        .unwrap();
    assert_eq!(total, (first.len() + second.len()) as u64);
    store.complete_upload(&key).await.unwrap();

    let mut expected = first.clone();
    expected.extend_from_slice(&second);
    assert_eq!(store.get(&key).await.unwrap(), expected);
}

/// Non-uniform mode flushes at the operator-configured `part_size`, not at the
/// S3 5 MiB floor. With `part_size = 8 MiB`, a ~6 MiB write must emit ZERO
/// parts (everything stays staged); only once the combined bytes reach
/// `part_size` does exactly one part get emitted.
#[tokio::test]
async fn upload_nonuniform_flushes_at_configured_part_size_not_min() {
    const PART_SIZE: u64 = 8 * 1024 * 1024;
    let store = backend_with(false, PART_SIZE);
    let key = format!("up/nonuniform-cfg/{}/data", Uuid::new_v4());

    // ~6 MiB: above the 5 MiB S3 floor but below the 8 MiB configured
    // threshold. Nothing may flush; it all stays staged.
    let first: Vec<u8> = (0..6 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    store.create_upload(&key).await.unwrap();
    let first_len = first.len() as u64;
    let total = store
        .write_upload(&key, frame(first.clone()), Some(first_len))
        .await
        .unwrap();
    assert_eq!(
        total, first_len,
        "total tracks staged bytes below the threshold"
    );

    assert_eq!(
        committed_part_count(&store, &key).await,
        0,
        "6 MiB < 8 MiB part_size: no part may be emitted yet"
    );
    assert_eq!(
        store
            .client
            .object_size(&staged_key(&key, 0))
            .await
            .unwrap(),
        first_len,
        "all bytes must remain staged below the configured threshold"
    );
    assert!(
        !has_open_multipart(&store, &key).await,
        "no multipart upload should open before the first flush"
    );

    // A second ~3 MiB write pushes the combined total (~9 MiB) over the 8 MiB
    // threshold, so exactly one part is emitted and the surplus is restaged.
    let second: Vec<u8> = (0..3 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    let second_len = second.len() as u64;
    store
        .write_upload(&key, frame(second.clone()), Some(second_len))
        .await
        .unwrap();

    assert_eq!(
        committed_part_count(&store, &key).await,
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

/// Regression guard for the default config: with `part_size = 5 MiB`
/// (== the S3 floor), non-uniform mode still flushes a ~6 MiB write into one
/// part with nothing left staged, exactly as before the threshold fix.
#[tokio::test]
async fn upload_nonuniform_default_still_flushes_at_5_mib() {
    let store = backend_with(false, 5 * 1024 * 1024);
    let key = format!("up/nonuniform-default/{}/data", Uuid::new_v4());
    let data: Vec<u8> = (0..6 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();

    store.create_upload(&key).await.unwrap();
    let len = data.len() as u64;
    store
        .write_upload(&key, frame(data.clone()), Some(len))
        .await
        .unwrap();

    assert_eq!(
        committed_part_count(&store, &key).await,
        1,
        "the 6 MiB write must emit one part"
    );
    // The single emitted part holds the whole 6 MiB, so the staged remainder at
    // that offset must be absent (nothing left staged).
    assert_eq!(
        store
            .client
            .object_size(&staged_key(&key, len))
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::NotFound,
        "nothing should be left staged"
    );
    assert!(
        has_open_multipart(&store, &key).await,
        "a multipart upload must be open"
    );

    store.complete_upload(&key).await.unwrap();
    let assembled = store.get(&key).await.unwrap();
    assert_eq!(assembled, data);
}

/// Small upload that never crosses the multipart threshold: `complete_upload`
/// promotes the staging key to the canonical key without ever creating a
/// multipart upload.
#[tokio::test]
async fn upload_small_upload_takes_singleshot_path() {
    let store = backend_with(false, 5 * 1024 * 1024);
    let key = format!("up/small/{}/data", Uuid::new_v4());
    store.create_upload(&key).await.unwrap();
    store
        .write_upload(&key, frame(b"hello".to_vec()), Some(5))
        .await
        .unwrap();
    assert!(
        !has_open_multipart(&store, &key).await,
        "small upload must not open a multipart upload"
    );
    store.complete_upload(&key).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), b"hello");
}

/// Keyless recovery: a write followed by an independent write at the same key
/// resumes from the recovered parts + staged remainder, with no caller state.
#[tokio::test]
async fn upload_resumes_from_recovered_state() {
    let store = backend_with(true, 5 * 1024 * 1024);
    let key = format!("up/resume/{}/data", Uuid::new_v4());
    store.create_upload(&key).await.unwrap();
    let head = vec![0x55; 6 * 1024 * 1024];
    store
        .write_upload(&key, frame(head.clone()), Some(head.len() as u64))
        .await
        .unwrap();

    // No handle is threaded; the next call recovers state from S3 by key.
    let tail = vec![0x66; 512 * 1024];
    let total = store
        .write_upload(&key, frame(tail.clone()), Some(tail.len() as u64))
        .await
        .unwrap();
    assert_eq!(total, (head.len() + tail.len()) as u64);
    store.complete_upload(&key).await.unwrap();
    let assembled = store.get(&key).await.unwrap();
    let mut expected = head;
    expected.extend_from_slice(&tail);
    assert_eq!(assembled, expected);
}

/// `abort_upload` clears every in-flight multipart upload at `key`, including
/// those started out-of-band, plus any staged remainder.
#[tokio::test]
async fn upload_abort_removes_orphans_and_staged() {
    let store = backend();
    let prefix = format!("up/abort/{}", Uuid::new_v4());
    let key = format!("{prefix}/data");

    // Manually start two multipart uploads at the same key and stage a
    // remainder, so we have every orphan kind to clean up.
    store.client.create_multipart_upload(&key).await.unwrap();
    store.client.create_multipart_upload(&key).await.unwrap();
    store
        .client
        .put_object(&staged_key(&key, 0), Bytes::from_static(b"leftover"))
        .await
        .unwrap();

    store.abort_upload(&key).await.unwrap();

    let (remaining, _, _) = store
        .client
        .list_multipart_uploads(Some(&key), None, None)
        .await
        .unwrap();
    assert!(remaining.iter().all(|u| u.key != key));
    assert_eq!(
        store
            .client
            .object_size(&staged_key(&key, 0))
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::NotFound,
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
