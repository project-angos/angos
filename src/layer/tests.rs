use std::{
    io::{Cursor, Write},
    sync::Arc,
};

use flate2::{Compression, write::GzEncoder};
use http::StatusCode;
use http_body_util::BodyExt;

use angos_oci::{Digest, Namespace};

use crate::{
    jobs::{
        Queue,
        store::{ClaimMode, JobStore},
    },
    layer::{
        Checkpoints, IndexLayerJobHandler, Kind, classify, extract_gzip, index_stream, read_listing,
    },
    registry::{
        Registry, RegistryConfig,
        layers::{LayerEntriesRequest, LayerFileRequest},
        test_utils::{
            fs_test_stack, repository_with_replication, seed_manifest, single_repo_resolver,
        },
    },
};

/// A layer as a build produces it: directories, files, links, whiteouts and
/// a name too long for the classic header, with `padding` bytes of filler
/// first so a checkpoint lands before the interesting entries.
fn layer_tar(padding: usize) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    let mut add = |path: &str, kind: tar::EntryType, data: &[u8], link: Option<&str>| {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(kind);
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_mtime(1_700_000_000);
        header.set_uid(0);
        header.set_gid(0);
        if let Some(link) = link {
            builder
                .append_link(&mut header, path, link)
                .expect("link entry");
        } else {
            builder
                .append_data(&mut header, path, data)
                .expect("data entry");
        }
    };
    let filler: Vec<u8> = (0..padding).map(|i| b"lorem ipsum "[i % 12]).collect();
    add("./filler.bin", tar::EntryType::Regular, &filler, None);
    add("./usr/", tar::EntryType::Directory, &[], None);
    add("./usr/bin/", tar::EntryType::Directory, &[], None);
    add(
        "./usr/bin/hello",
        tar::EntryType::Regular,
        b"#!/bin/sh\necho hello\n",
        None,
    );
    add("./usr/bin/hi", tar::EntryType::Symlink, &[], Some("hello"));
    add(
        "./usr/bin/hello-again",
        tar::EntryType::Link,
        &[],
        Some("usr/bin/hello"),
    );
    add("./etc/.wh.motd", tar::EntryType::Regular, &[], None);
    add(
        "./var/cache/.wh..wh..opq",
        tar::EntryType::Regular,
        &[],
        None,
    );
    let long = format!(
        "./opt/{}/deep.txt",
        "a-rather-long-directory-name-".repeat(5)
    );
    add(&long, tar::EntryType::Regular, b"deep", None);
    builder.into_inner().expect("tar bytes")
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(6));
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

#[test]
fn classify_names_whiteouts_and_normalises_paths() {
    let file = tar::EntryType::Regular;
    assert_eq!(
        classify("./usr/bin/", tar::EntryType::Directory),
        Some(("usr/bin".to_string(), Kind::Dir))
    );
    assert_eq!(
        classify("etc/.wh.motd", file),
        Some(("etc/motd".to_string(), Kind::Whiteout))
    );
    assert_eq!(
        classify(".wh.top", file),
        Some(("top".to_string(), Kind::Whiteout))
    );
    assert_eq!(
        classify("var/cache/.wh..wh..opq", file),
        Some(("var/cache".to_string(), Kind::Opaque))
    );
    assert_eq!(classify("./", tar::EntryType::Directory), None);
    assert_eq!(classify(".", tar::EntryType::Directory), None);
}

/// The listing carries every entry with its kind and offset, the checkpoints
/// let a file past the first megabytes be read without decoding them again,
/// and a plain tar lists the same with no checkpoints.
#[test]
fn indexes_a_gzipped_layer_and_extracts_a_file_from_a_checkpoint() {
    let tar = layer_tar(6 * 1024 * 1024);
    let compressed = gzip(&tar);
    let (listing, checkpoints) = index_stream(Cursor::new(&compressed)).unwrap();
    assert!(listing.compressed);
    assert_eq!(listing.uncompressed_size, tar.len() as u64);
    let kinds: Vec<(&str, Kind)> = listing
        .entries
        .iter()
        .map(|entry| (entry.path.as_str(), entry.kind))
        .collect();
    assert_eq!(
        kinds[..8],
        [
            ("filler.bin", Kind::File),
            ("usr", Kind::Dir),
            ("usr/bin", Kind::Dir),
            ("usr/bin/hello", Kind::File),
            ("usr/bin/hi", Kind::Symlink),
            ("usr/bin/hello-again", Kind::Hardlink),
            ("etc/motd", Kind::Whiteout),
            ("var/cache", Kind::Opaque),
        ]
    );
    assert!(
        kinds[8].0.ends_with("/deep.txt"),
        "long names survive: {}",
        kinds[8].0
    );
    let hello = &listing.entries[3];
    assert_eq!(hello.size, 21);
    assert_eq!(hello.mode, 0o644);
    assert_eq!(
        &tar[usize::try_from(hello.offset).unwrap()..usize::try_from(hello.offset).unwrap() + 21],
        b"#!/bin/sh\necho hello\n"
    );
    assert_eq!(listing.entries[4].link.as_deref(), Some("hello"));
    assert_eq!(listing.entries[5].link.as_deref(), Some("usr/bin/hello"));

    assert!(
        !checkpoints.is_empty(),
        "6 MiB of filler must produce a checkpoint"
    );
    let stored = Checkpoints::from_inflater(checkpoints);
    let checkpoint = stored
        .before(hello.offset)
        .expect("a checkpoint before the file");
    assert!(
        checkpoint.out_offset > 1024 * 1024,
        "the checkpoint sits past the filler's start"
    );
    let mut out = Vec::new();
    extract_gzip(
        Cursor::new(&compressed[usize::try_from(checkpoint.in_offset).unwrap()..]),
        Some(&checkpoint),
        hello.offset,
        hello.size,
        |chunk| {
            out.extend_from_slice(chunk);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(out, b"#!/bin/sh\necho hello\n");

    let (plain, none) = index_stream(Cursor::new(&tar)).unwrap();
    assert!(!plain.compressed);
    assert!(none.is_empty());
    assert_eq!(plain.entries, listing.entries);
}

/// The job stores the listing and its checkpoints by the layer digest, and a
/// second run finds them and does nothing.
#[tokio::test]
async fn the_job_indexes_a_stored_layer_once() {
    let stack = fs_test_stack();
    let compressed = gzip(&layer_tar(16));
    let digest = Digest::sha256_of_bytes(&compressed);
    stack
        .blob_store
        .put_blob(&digest, compressed.into())
        .await
        .unwrap();
    let handler = IndexLayerJobHandler::new(stack.blob_store.clone(), stack.metadata_store.clone());
    handler.index(&digest, false).await.unwrap();
    let listing = read_listing(&stack.metadata_store, &digest)
        .await
        .unwrap()
        .expect("a listing");
    assert_eq!(listing.entries.len(), 9);
    handler.index(&digest, false).await.unwrap();

    let gone = Digest::sha256_of_bytes(b"never stored");
    handler.index(&gone, false).await.unwrap();
    assert!(
        read_listing(&stack.metadata_store, &gone)
            .await
            .unwrap()
            .is_none()
    );
}

/// The entries endpoint answers 202 and enqueues the job for a layer the
/// namespace owns, then the listing once it ran; the file endpoint streams a
/// file, follows a hard link, and refuses what is not a file.
#[tokio::test]
async fn the_endpoints_index_on_demand_and_serve_a_file() {
    let stack = fs_test_stack();
    let namespace = Namespace::new("apps/web").unwrap();
    let compressed = gzip(&layer_tar(16));
    let digest = Digest::sha256_of_bytes(&compressed);
    stack
        .blob_store
        .put_blob(&digest, compressed.into())
        .await
        .unwrap();
    let job_store = Arc::new(JobStore::new(
        stack.store.clone(),
        "index-test",
        ClaimMode::Atomic,
    ));
    let registry = Registry::new(
        stack.blob_store.clone(),
        stack.metadata_store.clone(),
        single_repo_resolver("apps", repository_with_replication("apps", Vec::new())),
        RegistryConfig::new(job_store.clone()),
    );
    let entries = |namespace: &Namespace| {
        registry.get_layer_entries(LayerEntriesRequest {
            namespace: namespace.clone(),
            digest: digest.clone(),
        })
    };

    // Not owned: as unknown as the blob would be.
    assert!(entries(&namespace).await.is_err());
    stack
        .metadata_store
        .grant(&namespace, &digest)
        .await
        .unwrap();

    let response = entries(&namespace).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(job_store.count_pending(Queue::Index, 0).await.unwrap(), 1);
    IndexLayerJobHandler::new(stack.blob_store.clone(), stack.metadata_store.clone())
        .index(&digest, false)
        .await
        .unwrap();
    let response = entries(&namespace).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let listing: crate::layer::Listing = serde_json::from_slice(&body).unwrap();
    assert_eq!(listing.entries.len(), 9);

    let file = |path: &str| {
        registry.get_layer_file(LayerFileRequest {
            namespace: namespace.clone(),
            digest: digest.clone(),
            path: path.to_string(),
            download: true,
        })
    };
    let response = file("usr/bin/hello").await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-disposition"],
        "attachment; filename=\"hello\""
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"#!/bin/sh\necho hello\n");

    let body = file("usr/bin/hello-again")
        .await
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(
        &body[..],
        b"#!/bin/sh\necho hello\n",
        "a hard link serves its target"
    );
    assert!(
        file("usr/bin/hi").await.is_err(),
        "a symlink has no bytes of its own"
    );
    assert!(file("nope").await.is_err());
}

/// A push into an `index = true` repository enqueues one job per tar layer,
/// none for the config or a zstd layer, and none at all without the flag.
#[tokio::test]
async fn a_push_enqueues_an_index_job_per_tar_layer_of_an_indexing_repository() {
    use std::io::Cursor;

    use angos_oci::{MediaType, Reference, request::PutManifestRequest};

    let stack = fs_test_stack();
    let namespace = Namespace::new("apps/web").unwrap();
    let (_, config, layer) = seed_manifest(&stack.store, &stack.metadata_store, &namespace).await;
    let job_store = Arc::new(JobStore::new(
        stack.store.clone(),
        "index-test",
        ClaimMode::Atomic,
    ));
    let mut repository = repository_with_replication("apps", Vec::new());
    repository.index = true;
    let registry = Registry::new(
        stack.blob_store.clone(),
        stack.metadata_store.clone(),
        single_repo_resolver("apps", repository),
        RegistryConfig {
            validate_manifest_references: false,
            ..RegistryConfig::new(job_store.clone())
        },
    );
    let descriptor = |media: &str, digest: &Digest| {
        format!(r#"{{"mediaType":"{media}","digest":"{digest}","size":1}}"#)
    };
    let zstd = Digest::sha256_of_bytes(b"zstd layer");
    let image = format!(
        r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{},"layers":[{},{},{}]}}"#,
        descriptor("application/vnd.oci.image.config.v1+json", &config),
        descriptor("application/vnd.oci.image.layer.v1.tar+gzip", &layer),
        descriptor("application/vnd.docker.image.rootfs.diff.tar.gzip", &config),
        descriptor("application/vnd.oci.image.layer.v1.tar+zstd", &zstd),
    );
    let request = PutManifestRequest {
        namespace: namespace.clone(),
        reference: Reference::Digest(Digest::sha256_of_bytes(image.as_bytes())),
        content_type: Some(MediaType::oci_manifest()),
        tags: Vec::new(),
        source_ts: None,
    };
    registry
        .accept_put_manifest(None, request, Cursor::new(image.into_bytes()))
        .await
        .unwrap();
    assert_eq!(
        job_store.count_pending(Queue::Index, 0).await.unwrap(),
        2,
        "the two tar layers, not the config nor the zstd layer"
    );
}
