use std::{
    cell::RefCell,
    io::{Cursor, Read, Write},
    sync::Arc,
};

use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use http::StatusCode;
use http_body_util::BodyExt;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use angos_extension_service::{
    ElfDetails, LayerEntriesRequest, LayerFileDetailsRequest, LayerFileRequest,
};
use angos_oci::http_range::RequestRange;
use angos_oci::{Digest, Namespace};

use crate::{
    jobs::{
        Queue,
        store::{ClaimMode, JobStore},
    },
    layer::IndexAction,
    layer::SecretKind::{
        AwsAccessKey, AwsCredentials, GitCredentials, GithubToken, GitlabToken, Kubeconfig, Netrc,
        NpmToken, PrivateKey, RegistryAuth, SlackToken, StripeKey,
    },
    layer::{
        Checkpoints, IndexLayerJobHandler, IndexLayerPayload, Kind, SecretKind, SecretScanner,
        classify, elf, extract_gzip, index_stream, mime_type, pem, read_listing,
    },
    policy::{ImagePolicy, PolicyConfig},
    registry::{
        Error as RegistryError, Registry, RegistryConfig, Repository,
        keys::DigestKeys,
        manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
        repository::Config,
        test_utils::{
            fs_test_stack, repository_with_replication, seed_manifest, single_repo_resolver,
        },
    },
    test_fixtures::{client::test_client_config, tls},
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

#[test]
fn mime_type_trusts_magic_then_the_extension_then_the_bytes() {
    assert_eq!(
        mime_type("usr/lib/libc.so.6", b"\x7fELF\x02\x01"),
        "application/x-executable"
    );
    assert_eq!(mime_type("app/main.PY", b"import os\n"), "text/x-python");
    assert_eq!(mime_type("etc/app.json", b"{}"), "application/json");
    assert_eq!(mime_type("etc/passwd", b"root:x:0:0\n"), "text/plain");
    assert_eq!(mime_type("etc/.profile", b"export A=1\n"), "text/plain");
    assert_eq!(
        mime_type("var/lib/blob", b"\x01\x00\x02"),
        "application/octet-stream"
    );
    assert_eq!(
        mime_type("usr/bin/pip", b"#!/usr/bin/env -S python3.12 -u\n"),
        "text/x-python"
    );
    assert_eq!(
        mime_type("entrypoint", b"#!/bin/bash\r\nset -e\n"),
        "application/x-sh"
    );
    assert_eq!(mime_type("run.py", b"#!/bin/sh\n"), "text/x-python");
    assert_eq!(
        mime_type("usr/bin/awkish", b"#!/usr/bin/awk -f\n"),
        "text/plain"
    );
    assert_eq!(
        mime_type("etc/ssl/server.key.pem", b"-----BEGIN PRIVATE KEY-----\n"),
        "application/x-pem-file"
    );
    assert_eq!(
        mime_type("etc/ssl/certs/ca.crt", b"\n-----BEGIN CERTIFICATE-----\n"),
        "application/x-pem-file"
    );
    assert_eq!(mime_type("docs/README.md", b"# Title\n"), "text/markdown");
}

#[test]
fn secret_spots_keys_and_credential_files_only() {
    let found = |path: &str, head: &[u8]| scan(path, head, 8192);
    let file = |head: &[u8]| found("app/config", head);
    assert_eq!(
        file(b"-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAA\n"),
        [(PrivateKey, 1)]
    );
    assert_eq!(
        file(b"{\n  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkq\"\n}"),
        [(PrivateKey, 2)]
    );
    assert_eq!(
        file(b"[default]\naws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCY8Hq2Lk9Tz3\n"),
        [(AwsCredentials, 2)]
    );
    // Every one, in line order.
    assert_eq!(
        file(
            b"{\n\"auths\": {\n\"a.io\": {\"auth\": \"dTpw\"},\n\"b.io\": {\"auth\": \"dTpw\"}\n}}"
        ),
        [(RegistryAuth, 3), (RegistryAuth, 4)]
    );
    assert_eq!(
        file(b"{\"auths\":{\"ghcr.io\":{\"auth\":\"dTpw\"}}}"),
        [(RegistryAuth, 1)]
    );
    let none = [
        b"-----BEGIN CERTIFICATE-----\nMIIB\n".as_slice(),
        b"    aws_secret_access_key=None,\n",
        b"aws_secret_access_key = <secret>\n",
        // Documentation showing a key's shape holds none.
        b"    \"private_key\": \"-----BEGIN EC PRIVATE KEY-----\\n<key bytes>\\n\",\n",
        b"-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----\n",
        b"{\"auths\": {}}",
        b"{\"auths\": {\"ghcr.io\": {\"auth\": \"\"}}}",
        b"  <match value=\"-----BEGIN RSA PRIVATE KEY-----\" type=\"string\"/>\n",
        b"\x1c\x04\0-----BEGIN RSA PRIVATE KEY-----\n",
    ];
    for head in none {
        assert_eq!(file(head), [], "{}", String::from_utf8_lossy(head));
    }

    // Logins documentation quotes count only in the file that holds them.
    let npmrc = b"registry=https://npm.example.com/\n//npm.example.com/:_authToken=npm_abc\n";
    assert_eq!(found("root/.npmrc", npmrc), [(NpmToken, 2)]);
    assert_eq!(found("docs/npmrc.md", npmrc), []);
    let unset = b"//npm.example.com/:_authToken=${NPM_TOKEN}\n";
    assert_eq!(found("root/.npmrc", unset), []);
    let url = b"https://ci:ghp_abc@github.com\n";
    assert_eq!(found("root/.git-credentials", url), [(GitCredentials, 1)]);
    assert_eq!(found("docs/urls.txt", url), []);
    assert_eq!(
        found("root/.git-credentials", b"https://host:8443/a@b\n"),
        []
    );
    let netrc = b"machine api.example.com\n  login ci\n  password s3cret\n";
    assert_eq!(found("root/.netrc", netrc), [(Netrc, 3)]);
    assert_eq!(found("contrib/test.netrc", netrc), []);
}

/// A file's secrets as kinds and lines, its bytes fed `chunk` at a time.
fn scan(path: &str, bytes: &[u8], chunk: usize) -> Vec<(SecretKind, usize)> {
    let mut scanner = SecretScanner::new(path);
    for part in bytes.chunks(chunk) {
        scanner.feed(part);
    }
    scanner
        .finish()
        .into_iter()
        .map(|secret| (secret.kind, secret.line))
        .collect()
}

#[test]
fn secrets_are_found_past_the_first_bytes_however_they_arrive() {
    let mut text = "# notes\n".repeat(2000).into_bytes();
    text.extend_from_slice(b"-----BEGIN OPENSSH PRIVATE KEY-----\r\nb3BlbnNzaC1rZXktdjEAAAAA\r\n");
    for chunk in [1, 7, 8192] {
        assert_eq!(scan("app/key", &text, chunk), [(PrivateKey, 2001)]);
    }
    // A NUL past the first 8 KiB leaves a text file text; one before makes it binary.
    let mut late = text.clone();
    late.insert(9000, 0);
    assert_eq!(scan("app/key", &late, 4096), [(PrivateKey, 2001)]);
    let mut early = text;
    early.insert(100, 0);
    assert_eq!(scan("app/key", &early, 4096), []);
}

#[test]
fn service_tokens_count_when_they_look_random() {
    // Assembled here, so nothing token-shaped is committed.
    let random = "q7Rk2Vx9LmP4sT8wZc3Nf6Hj1Bd5Gy0Ea2Ku";
    let env = [
        format!("GITHUB_TOKEN=ghp_{random}"),
        format!("export GH=github_pat_{random}_{random}"),
        format!("gitlab: glpat-{}", &random[..20]),
        format!("slack = \"xoxb-1234567890-{}\"", &random[..24]),
        format!("STRIPE=sk_live_{}", &random[..24]),
        format!("AWS_ACCESS_KEY_ID=AKIA{}", "Z7QXK2MPL4RB9TNC"),
        // Documentation's placeholders, and a prefix inside a longer word.
        format!("GITHUB_TOKEN=ghp_{}", "x".repeat(36)),
        format!("aws_access_key_id = AKIA{}", "IOSFODNN7EXAMPLE"),
        "aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        format!("not_ghp_{random}"),
    ]
    .join("\n");
    assert_eq!(
        scan("app/.env", env.as_bytes(), 8192),
        [
            (GithubToken, 1),
            (GithubToken, 2),
            (GitlabToken, 3),
            (SlackToken, 4),
            (StripeKey, 5),
            (AwsAccessKey, 6)
        ]
    );

    let kubeconfig = format!(
        "users:\n- name: ci\n  user:\n    client-key-data: {}\n    token: {random}\n",
        "LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVkt"
    );
    assert_eq!(
        scan("root/.kube/config", kubeconfig.as_bytes(), 8192),
        [(Kubeconfig, 4), (Kubeconfig, 5)]
    );
    // A bearer token counts only in a kubeconfig, and a redacted key nowhere.
    assert_eq!(
        scan("ci/workflow.yaml", kubeconfig.as_bytes(), 8192),
        [(Kubeconfig, 4)]
    );
    let redacted = b"    client-key-data: DATA+OMITTED\n    token: REDACTED\n";
    assert_eq!(scan("root/.kube/config", redacted, 8192), []);
}

#[test]
fn capabilities_come_from_the_security_xattr() {
    // Revision 2, effective: cap_net_raw, bit 13, and cap_bpf, bit 39.
    let mut value = 0x0200_0001u32.to_le_bytes().to_vec();
    for word in [1u32 << 13, 0, 1 << 7, 0] {
        value.extend(word.to_le_bytes());
    }
    let mut builder = tar::Builder::new(Vec::new());
    builder
        .append_pax_extensions([("SCHILY.xattr.security.capability", value.as_slice())])
        .unwrap();
    for path in ["usr/bin/ping", "usr/bin/true"] {
        let mut header = tar::Header::new_gnu();
        header.set_size(4);
        header.set_mode(0o755);
        header.set_uid(0);
        header.set_gid(0);
        builder
            .append_data(&mut header, path, b"\x7fELF".as_slice())
            .unwrap();
    }
    let (listing, _) = index_stream(Cursor::new(builder.into_inner().unwrap())).unwrap();
    assert_eq!(listing.entries[0].capabilities, ["cap_net_raw", "cap_bpf"]);
    assert!(listing.entries[1].capabilities.is_empty());
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
    let content = hello.content.as_ref().expect("a file carries its content");
    assert_eq!(
        content.sha256,
        "bfdeaeb08cffb6a36438bcd12dda25417e3cdd36f1e7e482a2849d539225288b"
    );
    assert_eq!(
        content.sha512,
        "21c8b1b3d6bb72ee5a2025efa2a9eab0e18f79bc346e30f269b474b9865edf2c53cb02538027314d4511bc21ef7f4dd46296c8dd7f885a97afe4420dac62cca4"
    );
    assert_eq!(content.mime_type, "application/x-sh");
    assert!(
        listing.entries[1..8]
            .iter()
            .filter(|entry| entry.kind != Kind::File)
            .all(|entry| entry.content.is_none()),
        "only files carry content"
    );

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
        registry.handle_list_layer_entries(LayerEntriesRequest {
            namespace: namespace.clone(),
            digest: digest.clone(),
            gzip: false,
        })
    };

    // Not owned: as unknown as the blob would be.
    assert!(entries(&namespace).await.is_err());
    stack
        .metadata_store
        .grant(&namespace, &digest)
        .await
        .unwrap();

    let response = entries(&namespace).await.unwrap().into_response().unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(job_store.count_pending(Queue::Index, 0).await.unwrap(), 1);
    IndexLayerJobHandler::new(stack.blob_store.clone(), stack.metadata_store.clone())
        .index(&digest, false)
        .await
        .unwrap();
    let response = entries(&namespace).await.unwrap().into_response().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let listing: crate::layer::Listing = serde_json::from_slice(&body).unwrap();
    assert_eq!(listing.entries.len(), 9);
    let flags: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(flags["refreshing"], false);

    let file = |path: &str| {
        registry.handle_get_layer_file(LayerFileRequest {
            namespace: namespace.clone(),
            digest: digest.clone(),
            path: path.to_string(),
            download: true,
            range: None,
        })
    };
    let response = file("usr/bin/hello")
        .await
        .unwrap()
        .into_response(registry.blob_stream_frame_size())
        .unwrap();
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
        .into_response(registry.blob_stream_frame_size())
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

/// A listing an older version wrote, its files without contents, is served as
/// it is while a job walks the layer again and fills them in.
#[tokio::test]
async fn an_older_listing_is_served_and_walked_again() {
    let stack = fs_test_stack();
    let namespace = Namespace::new("apps/web").unwrap();
    let compressed = gzip(&layer_tar(16));
    let digest = Digest::sha256_of_bytes(&compressed);
    stack
        .blob_store
        .put_blob(&digest, compressed.into())
        .await
        .unwrap();
    stack
        .metadata_store
        .grant(&namespace, &digest)
        .await
        .unwrap();
    let handler = IndexLayerJobHandler::new(stack.blob_store.clone(), stack.metadata_store.clone());
    handler.index(&digest, false).await.unwrap();
    let mut listing = read_listing(&stack.metadata_store, &digest)
        .await
        .unwrap()
        .expect("a listing");
    listing.version = 0;
    for entry in &mut listing.entries {
        entry.content = None;
    }
    stack
        .metadata_store
        .object_store()
        .put(
            &digest.layer_entries_path(),
            serde_json::to_vec(&listing).unwrap().into(),
        )
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
    let response = registry
        .handle_list_layer_entries(LayerEntriesRequest {
            namespace,
            digest: digest.clone(),
            gzip: true,
        })
        .await
        .unwrap()
        .into_response()
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-encoding"], "gzip");
    let zipped = response.into_body().collect().await.unwrap().to_bytes();
    let mut json = Vec::new();
    GzDecoder::new(&zipped[..]).read_to_end(&mut json).unwrap();
    let served: crate::layer::Listing = serde_json::from_slice(&json).unwrap();
    assert_eq!(served, listing, "the outdated listing, served as it is");
    let flags: serde_json::Value = serde_json::from_slice(&json).unwrap();
    assert_eq!(flags["refreshing"], true);

    let claimed = job_store
        .claim_one(Queue::Index)
        .await
        .unwrap()
        .claimed
        .expect("a job walks the layer again");
    let payload: IndexLayerPayload = serde_json::from_value(claimed.envelope.payload).unwrap();
    // No need to force it: the outdated listing counts as none.
    handler.index(&digest, payload.force).await.unwrap();
    let listing = read_listing(&stack.metadata_store, &digest)
        .await
        .unwrap()
        .expect("a listing");
    assert!(
        listing
            .entries
            .iter()
            .filter(|entry| entry.kind == Kind::File)
            .all(|entry| entry.content.is_some())
    );
}

/// A `Range` gets the part of the file it names, decoded from the checkpoint
/// before it, sandboxed like the whole file; one past its end is refused.
#[tokio::test]
async fn the_file_endpoint_serves_a_range() {
    let stack = fs_test_stack();
    let namespace = Namespace::new("apps/web").unwrap();
    let compressed = gzip(&layer_tar(6 * 1024 * 1024));
    let digest = Digest::sha256_of_bytes(&compressed);
    stack
        .blob_store
        .put_blob(&digest, compressed.into())
        .await
        .unwrap();
    stack
        .metadata_store
        .grant(&namespace, &digest)
        .await
        .unwrap();
    IndexLayerJobHandler::new(stack.blob_store.clone(), stack.metadata_store.clone())
        .index(&digest, false)
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
        RegistryConfig::new(job_store),
    );
    let file = |range: &str| {
        registry.handle_get_layer_file(LayerFileRequest {
            namespace: namespace.clone(),
            digest: digest.clone(),
            path: "usr/bin/hello".to_string(),
            download: false,
            range: RequestRange::parse(range).unwrap(),
        })
    };

    let response = file("bytes=10-13")
        .await
        .unwrap()
        .into_response(registry.blob_stream_frame_size())
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()["content-range"], "bytes 10-13/21");
    assert_eq!(response.headers()["content-security-policy"], "sandbox");
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"echo");
    let body = file("bytes=-6")
        .await
        .unwrap()
        .into_response(registry.blob_stream_frame_size())
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(&body[..], b"hello\n");
    assert!(matches!(
        file("bytes=21-").await,
        Err(RegistryError::RangeNotSatisfiable)
    ));
}

/// A 64-bit x86-64 PIE, `size` bytes long, its dynamic section at
/// `dynamic_at` and the names that points at 1 KiB further.
fn elf_binary(size: usize, dynamic_at: usize) -> Vec<u8> {
    const BASE: u64 = 0x10000;
    let strings_at = dynamic_at + 1024;
    let interpreter = b"/lib/ld-musl-x86_64.so.1\0";
    let note = b"\x04\0\0\0\x04\0\0\0\x03\0\0\0GNU\0\xde\xad\xbe\xef";
    let mut elf = vec![0; size];
    let mut put = |at: usize, bytes: &[u8]| elf[at..at + bytes.len()].copy_from_slice(bytes);
    put(0, b"\x7fELF\x02\x01\x01");
    put(16, &3u16.to_le_bytes());
    put(18, &62u16.to_le_bytes());
    put(24, &0x1040u64.to_le_bytes());
    put(32, &64u64.to_le_bytes());
    put(54, &56u16.to_le_bytes());
    put(56, &6u16.to_le_bytes());
    // Kind, flags, offset and size; each loads at `BASE` past its offset.
    let segments = [
        (3u32, 4u32, 512, interpreter.len()),
        (4, 4, 600, note.len()),
        (1, 5, 0, size),
        (2, 6, dynamic_at, 112),
        (0x6474_e551, 6, 0, 0),
        (0x6474_e552, 4, dynamic_at, 112),
    ];
    for (index, (kind, flags, offset, length)) in segments.into_iter().enumerate() {
        let at = 64 + index * 56;
        put(at, &kind.to_le_bytes());
        put(at + 4, &flags.to_le_bytes());
        put(at + 8, &(offset as u64).to_le_bytes());
        put(at + 16, &(BASE + offset as u64).to_le_bytes());
        put(at + 32, &(length as u64).to_le_bytes());
    }
    put(512, interpreter);
    put(600, note);
    let dynamic = [
        (1u64, 1u64),
        (1, 23),
        (14, 33),
        (5, BASE + strings_at as u64),
        (10, 64),
        (30, 8),
        (0, 0),
    ];
    for (index, (tag, value)) in dynamic.into_iter().enumerate() {
        put(dynamic_at + index * 16, &tag.to_le_bytes());
        put(dynamic_at + index * 16 + 8, &value.to_le_bytes());
    }
    put(
        strings_at,
        b"\0libc.musl-x86_64.so.1\0libz.so.1\0libapp.so\0",
    );
    elf
}

fn demo_elf_details() -> ElfDetails {
    ElfDetails {
        kind: "PIE executable".to_string(),
        machine: "x86-64".to_string(),
        bits: 64,
        endian: "little".to_string(),
        entry: "0x1040".to_string(),
        interpreter: Some("/lib/ld-musl-x86_64.so.1".to_string()),
        dynamic: true,
        needed: vec!["libc.musl-x86_64.so.1".to_string(), "libz.so.1".to_string()],
        soname: Some("libapp.so".to_string()),
        build_id: Some("deadbeef".to_string()),
        relro: "full".to_string(),
        executable_stack: false,
    }
}

/// Past the head, only the dynamic section and the stretch of the string
/// table holding the names are read.
#[tokio::test]
async fn describe_reads_an_elf_binary_past_its_head() {
    let binary = elf_binary(4096, 2048);
    let reads = RefCell::new(Vec::new());
    let details = elf::describe(&binary[..1024], |offset, length| {
        reads.borrow_mut().push((offset, length));
        let range = usize::try_from(offset).unwrap()..usize::try_from(offset + length).unwrap();
        let bytes = binary[range].to_vec();
        async move { Ok::<_, ()>(bytes) }
    })
    .await
    .unwrap();
    assert_eq!(details, Some(demo_elf_details()));
    assert_eq!(*reads.borrow(), [(2048, 112), (3073, 63)]);

    let none = elf::describe(b"#!/bin/sh\n", |_, _| async { Ok::<_, ()>(Vec::new()) });
    assert_eq!(none.await, Ok(None));
}

/// A forged table repeating every segment a detail is read from costs one
/// read for the loader, one for the dynamic section and four for notes, not
/// one per entry.
#[tokio::test]
async fn describe_reads_a_bounded_number_of_segments() {
    let count = 900;
    let mut head = vec![0; 64 + 56 * count];
    head[..7].copy_from_slice(b"\x7fELF\x02\x01\x01");
    head[32..40].copy_from_slice(&64u64.to_le_bytes());
    head[54..56].copy_from_slice(&56u16.to_le_bytes());
    head[56..58].copy_from_slice(&u16::try_from(count).unwrap().to_le_bytes());
    for index in 0..count {
        let at = 64 + index * 56;
        let kind = [3u32, 4, 2][index % 3];
        head[at..at + 4].copy_from_slice(&kind.to_le_bytes());
        head[at + 8..at + 16].copy_from_slice(&(1u64 << 30).to_le_bytes());
        head[at + 32..at + 40].copy_from_slice(&16u64.to_le_bytes());
    }
    let reads = RefCell::new(0);
    let details = elf::describe(&head, |_, length| {
        *reads.borrow_mut() += 1;
        async move { Ok::<_, ()>(vec![0; usize::try_from(length).unwrap()]) }
    })
    .await
    .unwrap();
    assert!(details.is_some());
    assert_eq!(*reads.borrow(), 6);
}

#[test]
fn pem_blocks_decode_certificates_and_name_the_rest() {
    let text = format!("{}{}", tls::server_cert_pem(), tls::server_key_pem());
    assert!(pem::holds_certificates(text.as_bytes()));
    assert!(!pem::holds_certificates(tls::server_key_pem().as_bytes()));
    let blocks = pem::blocks(text.as_bytes());
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].label, "CERTIFICATE");
    let certificate = blocks[0].certificate.as_ref().expect("a certificate");
    assert_eq!(certificate.subject, "CN=example.com");
    assert_eq!(certificate.issuer, "CN=example.com");
    assert_eq!(certificate.names, ["example.com"]);
    assert!(certificate.not_before < certificate.not_after);
    assert_eq!(blocks[1].label, "PRIVATE KEY");
    assert_eq!(blocks[1].certificate, None);
}

/// Details decode a binary whose libraries lie past the bytes first read and
/// a PEM file's certificates, out of a gzipped layer, and are empty otherwise.
#[tokio::test]
async fn details_describe_binaries_and_certificates() {
    let stack = fs_test_stack();
    let namespace = Namespace::new("apps/web").unwrap();
    let mut builder = tar::Builder::new(Vec::new());
    let files: [(&str, &[u8]); 3] = [
        ("usr/bin/app", &elf_binary(128 * 1024, 100 * 1024)),
        ("etc/ssl/cert.pem", tls::server_cert_pem().as_bytes()),
        ("etc/motd", b"hello\n"),
    ];
    for (path, data) in files {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o755);
        header.set_uid(0);
        header.set_gid(0);
        builder.append_data(&mut header, path, data).unwrap();
    }
    let compressed = gzip(&builder.into_inner().unwrap());
    let digest = Digest::sha256_of_bytes(&compressed);
    stack
        .blob_store
        .put_blob(&digest, compressed.into())
        .await
        .unwrap();
    stack
        .metadata_store
        .grant(&namespace, &digest)
        .await
        .unwrap();
    IndexLayerJobHandler::new(stack.blob_store.clone(), stack.metadata_store.clone())
        .index(&digest, false)
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
        RegistryConfig::new(job_store),
    );
    let details = |path: &str| {
        registry.handle_get_layer_file_details(LayerFileDetailsRequest {
            namespace: namespace.clone(),
            digest: digest.clone(),
            path: path.to_string(),
        })
    };

    let binary = details("usr/bin/app").await.unwrap();
    assert_eq!(binary.elf, Some(demo_elf_details()));
    assert_eq!(binary.certificates, None);
    let certificates = details("etc/ssl/cert.pem").await.unwrap();
    assert_eq!(certificates.elf, None);
    assert_eq!(
        certificates.certificates.unwrap()[0]
            .certificate
            .as_ref()
            .map(|certificate| certificate.subject.as_str()),
        Some("CN=example.com")
    );
    let text = details("etc/motd").await.unwrap();
    assert_eq!((text.elf, text.certificates), (None, None));
    assert!(matches!(
        details("etc/none").await,
        Err(RegistryError::NotFound)
    ));
}

/// A push an index policy applies to enqueues one job per tar layer,
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
    repository.index = Some(ImagePolicy::new(&PolicyConfig {
        default: Some(IndexAction::Index),
        rules: Vec::new(),
    }));
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
        .handle_put_manifest(None, request, Cursor::new(image.into_bytes()))
        .await
        .unwrap();
    assert_eq!(
        job_store.count_pending(Queue::Index, 0).await.unwrap(),
        2,
        "the two tar layers, not the config nor the zstd layer"
    );
}

/// A pull-through layer the namespace holds no grant for is asked of the
/// upstream, one the manifest pull linked before its bytes were fetched is
/// not; both get the cache fill enqueued behind 202, and a layer the upstream
/// does not have stays as unknown as the blob.
#[tokio::test]
async fn the_entries_endpoint_fills_the_cache_of_a_pull_through_layer() {
    let stack = fs_test_stack();
    let namespace = Namespace::new("mirror/app").unwrap();
    let digest = Digest::sha256_of_bytes(b"never fetched");
    let upstream = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path(format!("/v2/app/blobs/{digest}")))
        .respond_with(ResponseTemplate::new(200).insert_header("Content-Length", "13"))
        .mount(&upstream)
        .await;
    let job_store = Arc::new(JobStore::new(
        stack.store.clone(),
        "fill-test",
        ClaimMode::Atomic,
    ));
    let config = Config {
        upstream: vec![test_client_config(upstream.uri())],
        ..Default::default()
    };
    let cache_backend = angos_cache::Config::Memory.to_backend().unwrap();
    let repository = Repository::new(
        "mirror",
        &config,
        &cache_backend,
        DEFAULT_MAX_MANIFEST_SIZE_BYTES,
    )
    .await
    .unwrap();
    let registry = Registry::new(
        stack.blob_store.clone(),
        stack.metadata_store.clone(),
        single_repo_resolver("mirror", repository),
        RegistryConfig::new(job_store.clone()),
    );
    let entries = |digest: &Digest| {
        registry.handle_list_layer_entries(LayerEntriesRequest {
            namespace: namespace.clone(),
            digest: digest.clone(),
            gzip: false,
        })
    };

    assert!(
        entries(&Digest::sha256_of_bytes(b"not upstream"))
            .await
            .is_err()
    );

    let response = entries(&digest).await.unwrap().into_response().unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(job_store.count_pending(Queue::Cache, 0).await.unwrap(), 1);

    stack
        .metadata_store
        .grant(&namespace, &digest)
        .await
        .unwrap();
    let response = entries(&digest).await.unwrap().into_response().unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(job_store.count_pending(Queue::Cache, 0).await.unwrap(), 1);
    assert_eq!(job_store.count_pending(Queue::Index, 0).await.unwrap(), 0);
}
