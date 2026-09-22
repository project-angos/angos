use std::{
    io::Cursor,
    sync::{Arc, Mutex, atomic::Ordering},
};

use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use serde_json::json;
use uuid::Uuid;

use angos_oci::{
    Digest, Namespace, Reference, Tag,
    request::{DeleteBlobRequest, PutManifestRequest},
};

use crate::{
    command::{
        maintenance::{
            action::{Action, LOST_AND_FOUND_PREFIX},
            executor::{ActionSink, Executor},
            walk::{self, WalkStats},
        },
        scrub::validate::{Pass, Validator},
    },
    jobs::{
        Queue,
        store::{JobEnvelope, LockKey, job_lock_key_index_path},
    },
    registry::{
        Error as RegistryError,
        blob_store::BlobStore,
        keys::{DigestKeys, NamespaceKeys, REF_ROOT},
        metadata_store::{AccessEntry, LinkKind, MetadataStore, Settings},
        test_utils::{
            RegistryTestCase, create_test_registry_with, for_each_backend, fs_test_stack,
            media_type, put_blob_direct, upload_blob,
        },
    },
};

/// Run the three walk passes over the test case's stores with `sink`,
/// mirroring `Command::run`.
async fn run_passes(
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    sink: Arc<dyn ActionSink>,
) -> Arc<WalkStats> {
    run_passes_with(blob_store, metadata_store, sink, false).await
}

/// `run_passes` with the `--delete-unknown` disposition made explicit.
async fn run_passes_with(
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    sink: Arc<dyn ActionSink>,
    delete_unknown: bool,
) -> Arc<WalkStats> {
    let stats = Arc::new(WalkStats::default());
    let validator = Arc::new(Validator::new(
        blob_store.clone(),
        metadata_store.clone(),
        sink,
        stats.clone(),
        delete_unknown,
    ));

    let meta_objects = metadata_store.object_store();
    let passes = [
        (Pass::MetadataLinks, "", meta_objects),
        (Pass::MetadataReferences, REF_ROOT, meta_objects),
        (Pass::Blob, "", blob_store.object_store()),
    ];
    for (pass, prefix, objects) in passes {
        let validator = &validator;
        walk::for_each_key(objects, prefix, 4, |key| async move {
            validator.process(pass, &key).await;
        })
        .await
        .expect("walk pass");
    }
    stats
}

/// Full scrub with a real executor: repairs are applied.
async fn scrub_apply(test_case: &dyn RegistryTestCase) {
    let blob_store = test_case.blob_store();
    let metadata_store = test_case.metadata_store();
    let sink: Arc<dyn ActionSink> = Arc::new(Executor::new_for_test(
        blob_store.clone(),
        metadata_store.clone(),
    ));
    run_passes(&blob_store, &metadata_store, sink).await;
}

/// Capture-only scrub: returns the actions a run would apply.
async fn scrub_capture(test_case: &dyn RegistryTestCase) -> Vec<Action> {
    let blob_store = test_case.blob_store();
    let metadata_store = test_case.metadata_store();
    let sink = Arc::new(Mutex::new(Vec::new()));
    run_passes(
        &blob_store,
        &metadata_store,
        sink.clone() as Arc<dyn ActionSink>,
    )
    .await;
    match sink.lock() {
        Ok(mut actions) => actions.drain(..).collect(),
        Err(poisoned) => poisoned.into_inner().drain(..).collect(),
    }
}

/// Push a healthy image (config and layer blobs, manifest, `v1` tag) through
/// the real write path, so every link, back-reference, and grant exists.
async fn push_healthy_image(
    test_case: &dyn RegistryTestCase,
    namespace: &Namespace,
) -> (Digest, Digest, Digest) {
    let registry = test_case.registry();
    let config_bytes = br#"{"healthy":true}"#;
    let layer_bytes = b"healthy-layer-bytes";
    let config_digest = upload_blob(registry, namespace, config_bytes).await;
    let layer_digest = upload_blob(registry, namespace, layer_bytes).await;

    let manifest = format!(
        r#"{{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {{
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "{config_digest}",
            "size": {}
        }},
        "layers": [{{
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "digest": "{layer_digest}",
            "size": {}
        }}]
    }}"#,
        config_bytes.len(),
        layer_bytes.len(),
    );
    let response = registry
        .put_manifest_direct(
            namespace,
            &Reference::Tag(Tag::new("v1").unwrap()),
            Some(&media_type("application/vnd.oci.image.manifest.v1+json")),
            manifest.as_bytes(),
        )
        .await
        .expect("healthy push");
    (response.digest, config_digest, layer_digest)
}

fn tag(name: &str) -> LinkKind {
    LinkKind::Tag(Tag::new(name).unwrap())
}

#[tokio::test]
async fn healthy_registry_emits_zero_actions() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/healthy").unwrap();
        push_healthy_image(test_case, namespace).await;

        let actions = scrub_capture(test_case).await;
        assert!(
            actions.is_empty(),
            "a healthy registry must produce zero actions, got: {:?}",
            actions.iter().map(ToString::to_string).collect::<Vec<_>>()
        );
    })
    .await;
}

/// A referrer lives as a record of its own, so its link check
/// has to consult the record. Judged by the legacy path alone, every
/// attestation in the store was recreated on every run: the repair writes the
/// record it already has and never the file it was judged by, so the next run
/// reached the same verdict.
#[tokio::test]
async fn an_attested_image_emits_zero_actions() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/attested").unwrap();
        let (subject_digest, config_digest, layer_digest) =
            push_healthy_image(test_case, namespace).await;
        let blob_store = test_case.blob_store();
        let sizes = async |digest: &Digest| blob_store.read(digest).await.unwrap().len();
        let (subject_size, config_size, layer_size) = (
            sizes(&subject_digest).await,
            sizes(&config_digest).await,
            sizes(&layer_digest).await,
        );

        // An attestation is a manifest naming another as its `subject`, which
        // is what makes the write path record a referrer.
        let attestation = format!(
            r#"{{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "artifactType": "application/vnd.dev.cosign.artifact.sig.v1+json",
        "config": {{
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "{config_digest}",
            "size": {config_size}
        }},
        "layers": [{{
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "digest": "{layer_digest}",
            "size": {layer_size}
        }}],
        "subject": {{
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": "{subject_digest}",
            "size": {subject_size}
        }}
    }}"#
        );
        let attestation_digest = test_case
            .registry()
            .put_manifest_direct(
                namespace,
                &Reference::Digest(Digest::sha256_of_bytes(attestation.as_bytes())),
                Some(&media_type("application/vnd.oci.image.manifest.v1+json")),
                attestation.as_bytes(),
            )
            .await
            .expect("attestation push")
            .digest;

        let metadata_store = test_case.metadata_store();
        metadata_store
            .object_store()
            .head(&namespace.referrer_record_path(&subject_digest, &attestation_digest))
            .await
            .expect("the push must record the referrer");

        let actions = scrub_capture(test_case).await;
        assert!(
            actions.is_empty(),
            "an attested image is healthy and must produce zero actions, got: {:?}",
            actions.iter().map(ToString::to_string).collect::<Vec<_>>()
        );
    })
    .await;
}

#[tokio::test]
async fn scrub_regrants_missing_per_referrer_entries() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/heal-links").unwrap();
        let (manifest_digest, config_digest, layer_digest) =
            push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        // Break the config and layer pins out-of-band.
        let entry = LinkKind::ReferencedBy(manifest_digest.clone());
        for digest in [&config_digest, &layer_digest] {
            metadata_store
                .remove_reference(namespace, digest, &entry)
                .await
                .unwrap();
        }

        scrub_apply(test_case).await;

        for digest in [&config_digest, &layer_digest] {
            let links = metadata_store
                .read_blob_index_namespace(namespace, digest)
                .await
                .unwrap();
            assert!(
                links.contains(&entry),
                "the per-referrer entry on '{digest}' must be re-granted"
            );
        }
    })
    .await;
}

#[tokio::test]
async fn scrub_recreates_a_missing_revision_record_for_a_tag() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/heal-digest").unwrap();
        let (manifest_digest, _, _) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        let digest_link = LinkKind::Digest(manifest_digest.clone());
        metadata_store
            .object_store()
            .delete(&namespace.revision_record_path(&manifest_digest))
            .await
            .unwrap();

        scrub_apply(test_case).await;

        assert!(
            metadata_store
                .read_link(namespace, &digest_link)
                .await
                .is_ok(),
            "the tag's revision record must be recreated"
        );
    })
    .await;
}

#[tokio::test]
async fn tag_targeting_missing_blob_is_removed() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/dangling-tag").unwrap();
        let metadata_store = test_case.metadata_store();

        let ghost_digest = Digest::sha256_of_bytes(b"never-uploaded");
        let key = namespace.tag_entry_path(
            &Tag::new("dangling").unwrap(),
            Utc::now(),
            false,
            &ghost_digest,
        );
        metadata_store
            .object_store()
            .put(&key, Bytes::from_static(b"{}"))
            .await
            .unwrap();

        scrub_apply(test_case).await;

        assert!(
            metadata_store
                .read_link(namespace, &tag("dangling"))
                .await
                .is_err(),
            "a tag targeting missing bytes must be removed"
        );
    })
    .await;
}

/// Regression: with `allow_missing_manifest_references` the push path drops the
/// link and grant for a digest the namespace does not own, so it gains no
/// cross-namespace read access. Scrub re-derives child links from the manifest
/// body and must not hand back what the write path withheld.
#[tokio::test]
async fn withheld_cross_namespace_reference_is_not_regranted() {
    for_each_backend(async |test_case| {
        let owner = &Namespace::new("test-repo/withheld-owner").unwrap();
        let borrower = &Namespace::new("test-repo/withheld-borrower").unwrap();
        let blob_store = test_case.blob_store();
        let metadata_store = test_case.metadata_store();

        // `owner` uploads the layer and config; `borrower` never references
        // either, so it holds no grant on them.
        let (_, config_digest, layer_digest) = push_healthy_image(test_case, owner).await;

        // A permissive push into `borrower` naming the same digests: the pins
        // on them are withheld, the revision record is written.
        let permissive =
            create_test_registry_with(blob_store.clone(), metadata_store.clone(), false);
        let manifest = format!(
            r#"{{
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {{
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "{config_digest}",
                "size": 16
            }},
            "layers": [{{
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": "{layer_digest}",
                "size": 19
            }}]
        }}"#
        );
        permissive
            .handle_put_manifest(
                None,
                PutManifestRequest {
                    namespace: borrower.clone(),
                    reference: Reference::Tag(Tag::new("borrowed").unwrap()),
                    content_type: Some(media_type("application/vnd.oci.image.manifest.v1+json")),
                    tags: vec![],
                    source_ts: None,
                },
                Cursor::new(manifest.into_bytes()),
            )
            .await
            .expect("permissive push");

        for digest in [&layer_digest, &config_digest] {
            assert!(
                metadata_store
                    .read_blob_index_namespace(borrower, digest)
                    .await
                    .is_err(),
                "the permissive push must withhold the grant on '{digest}'"
            );
        }

        scrub_apply(test_case).await;

        for digest in [&layer_digest, &config_digest] {
            assert!(
                metadata_store
                    .read_blob_index_namespace(borrower, digest)
                    .await
                    .is_err(),
                "scrub granted '{borrower}' read access to '{digest}' that the push withheld"
            );
        }
    })
    .await;
}

#[tokio::test]
async fn stale_reference_entry_is_removed() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/stale-entry").unwrap();
        let (_, _, layer_digest) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        // A `Layer` phantom would alias the healthy self-entry and assert
        // nothing, so the phantom names a tag instead.
        let phantom = LinkKind::Tag(Tag::new("phantom-tag").unwrap());
        metadata_store
            .insert_reference(namespace, &layer_digest, &phantom)
            .await
            .unwrap();

        scrub_apply(test_case).await;

        let links = metadata_store
            .read_blob_index_namespace(namespace, &layer_digest)
            .await
            .unwrap();
        assert!(
            !links.contains(&phantom),
            "a reference entry whose referring revision is gone must be removed"
        );
    })
    .await;
}

/// A dangling reference younger than the grace period may belong to a push
/// between its reference wave and its commit, so scrub must leave it alone.
#[tokio::test]
async fn young_dangling_ref_entry_is_kept() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/young-entry").unwrap();
        let (_, _, layer_digest) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        let phantom = LinkKind::Tag(Tag::new("phantom-tag").unwrap());
        metadata_store
            .insert_reference(namespace, &layer_digest, &phantom)
            .await
            .unwrap();

        // Same stores, but a scrub whose grace period is real.
        let graced = Arc::new(MetadataStore::new(
            metadata_store.object_store().clone(),
            Settings {
                gc_grace_secs: 300,
                ..Settings::default()
            },
        ));
        let blob_store = test_case.blob_store();
        let sink: Arc<dyn ActionSink> =
            Arc::new(Executor::new_for_test(blob_store.clone(), graced.clone()));
        run_passes(&blob_store, &graced, sink).await;

        let links = metadata_store
            .read_blob_index_namespace(namespace, &layer_digest)
            .await
            .unwrap();
        assert!(
            links.contains(&phantom),
            "a dangling entry inside the grace period must be kept"
        );
    })
    .await;
}

/// Link repairs derived from a young revision (here: a removed per-referrer
/// entry) may race a push between its waves, so a graced scrub defers them.
#[tokio::test]
async fn young_revision_defers_link_repairs() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/young-revision").unwrap();
        let (manifest_digest, _, layer_digest) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        let entry = LinkKind::ReferencedBy(manifest_digest.clone());
        metadata_store
            .remove_reference(namespace, &layer_digest, &entry)
            .await
            .unwrap();

        let graced = Arc::new(MetadataStore::new(
            metadata_store.object_store().clone(),
            Settings {
                gc_grace_secs: 300,
                ..Settings::default()
            },
        ));
        let blob_store = test_case.blob_store();
        let sink = Arc::new(Mutex::new(Vec::new()));
        run_passes(&blob_store, &graced, sink.clone() as Arc<dyn ActionSink>).await;
        let deferred = sink.lock().unwrap().is_empty();
        assert!(
            deferred,
            "repairs on a revision inside the grace period must wait for the next run"
        );

        // The grace-0 store the fixture built repairs it immediately.
        scrub_apply(test_case).await;
        let links = metadata_store
            .read_blob_index_namespace(namespace, &layer_digest)
            .await
            .unwrap();
        assert!(
            links.contains(&entry),
            "the entry must be re-granted once the grace period is out of scope"
        );
    })
    .await;
}

/// A tag whose entry is inside the grace period can be observed mid-delete
/// (tombstone not yet landed, revision record already gone); a graced scrub
/// must not derive repairs from it.
#[tokio::test]
async fn young_tag_defers_target_repairs() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/young-tag").unwrap();
        let (manifest_digest, _, _) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        let record = namespace.revision_record_path(&manifest_digest);
        metadata_store.object_store().delete(&record).await.unwrap();

        let graced = Arc::new(MetadataStore::new(
            metadata_store.object_store().clone(),
            Settings {
                gc_grace_secs: 300,
                ..Settings::default()
            },
        ));
        let blob_store = test_case.blob_store();
        let sink = Arc::new(Mutex::new(Vec::new()));
        run_passes(&blob_store, &graced, sink.clone() as Arc<dyn ActionSink>).await;
        let deferred = sink.lock().unwrap().is_empty();
        assert!(
            deferred,
            "repairs behind a tag entry inside the grace period must wait"
        );
    })
    .await;
}

/// The run marker is the one key a writer and the collector both consult;
/// a scrub walk must never quarantine a live one.
#[tokio::test]
async fn a_live_gc_marker_survives_the_walk() {
    for_each_backend(async |test_case| {
        let metadata_store = test_case.metadata_store();
        let digest = Digest::sha256_of_bytes(b"marker-covered blob");
        let claim = metadata_store.gc_claim(&digest, &digest).await.unwrap();

        scrub_apply(test_case).await;

        assert!(
            metadata_store.gc_blocked(&[&digest]).await.unwrap(),
            "the live run marker must survive a scrub walk"
        );
        metadata_store.gc_release(claim).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn orphan_blob_is_reclaimed() {
    for_each_backend(async |test_case| {
        let metadata_store = test_case.metadata_store();
        let blob_store = test_case.blob_store();

        let orphan = put_blob_direct(metadata_store.object_store(), b"unreferenced-bytes").await;
        assert!(blob_store.size(&orphan).await.is_ok());

        scrub_apply(test_case).await;

        assert!(
            blob_store.size(&orphan).await.is_err(),
            "a blob with no index entries must be reclaimed"
        );
    })
    .await;
}

/// Blob GC reads one listing of the blob's reference directory, while the
/// reference walk reaches those same keys through a whole-store scan. When the
/// two disagree the listing must not win: a backend that drops a key from a
/// listing would otherwise reclaim the bytes of a referenced blob.
#[tokio::test]
async fn a_blob_the_reference_walk_saw_referenced_is_never_reclaimed() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/ref-witness").unwrap();
        let (_, _, layer_digest) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        let sink = Arc::new(Mutex::new(Vec::new()));
        let validator = Validator::new(
            test_case.blob_store(),
            metadata_store.clone(),
            sink.clone() as Arc<dyn ActionSink>,
            Arc::new(WalkStats::default()),
            false,
        );

        let entry = LinkKind::Blob(layer_digest.clone());
        let ref_key = layer_digest.blob_ref_path(namespace, &entry);
        validator
            .validate_ref(&ref_key, &layer_digest, namespace, entry)
            .await
            .expect("the reference walk must read the layer's references");

        // The per-blob index read now finds nothing, as on a backend that
        // dropped this key from the listing behind it.
        metadata_store
            .object_store()
            .delete_prefix(&layer_digest.blob_ref_dir())
            .await
            .expect("reference removal");

        validator
            .validate_blob(&layer_digest)
            .await
            .expect("blob validation");

        let emitted = match sink.lock() {
            Ok(actions) => actions,
            Err(poisoned) => poisoned.into_inner(),
        };
        assert!(
            !emitted
                .iter()
                .any(|action| matches!(action, Action::DeleteOrphanBlob(d) if d == &layer_digest)),
            "the shard walk saw references, so the bytes must not be reclaimed, got: {:?}",
            emitted.iter().map(ToString::to_string).collect::<Vec<_>>()
        );
    })
    .await;
}

#[tokio::test]
async fn orphan_referrer_record_is_deleted() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/orphan-referrer").unwrap();
        let (manifest_digest, _, _) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        // A referrer entry whose referrer manifest is not a current revision.
        let ghost_referrer = Digest::sha256_of_bytes(b"gone-referrer");
        let link = LinkKind::Referrer {
            subject: manifest_digest.clone(),
            referrer: ghost_referrer.clone(),
        };
        metadata_store
            .object_store()
            .put(
                &namespace.referrer_record_path(&manifest_digest, &ghost_referrer),
                Bytes::from_static(b"{}"),
            )
            .await
            .unwrap();

        scrub_apply(test_case).await;

        assert!(
            metadata_store.read_link(namespace, &link).await.is_err(),
            "a referrer with no revision must be deleted"
        );
    })
    .await;
}

/// The catalog index key of a namespace that holds no revision or tag is
/// reaped, so an emptied namespace leaves the admin listings; the key of a
/// namespace with content is left alone.
#[tokio::test]
async fn catalog_index_key_of_an_emptied_namespace_is_reaped() {
    for_each_backend(async |test_case| {
        let metadata_store = test_case.metadata_store();
        let emptied = &Namespace::new("test-repo/emptied").unwrap();
        let populated = &Namespace::new("test-repo/populated").unwrap();
        push_healthy_image(test_case, populated).await;
        // What a namespace looks like once its last manifest was deleted: the
        // index key its first push wrote, and nothing behind it.
        metadata_store
            .object_store()
            .put(&emptied.catalog_index_path(), Bytes::new())
            .await
            .unwrap();

        scrub_apply(test_case).await;

        assert!(
            !metadata_store
                .object_store()
                .exists(&emptied.catalog_index_path())
                .await
                .unwrap(),
            "an emptied namespace's catalog key must be reaped"
        );
        assert!(
            metadata_store
                .object_store()
                .exists(&populated.catalog_index_path())
                .await
                .unwrap(),
            "a namespace holding content must keep its catalog key"
        );
    })
    .await;
}

#[tokio::test]
async fn unknown_keys_are_quarantined_in_both_stores() {
    for_each_backend(async |test_case| {
        let metadata_store = test_case.metadata_store();
        let blob_store = test_case.blob_store();

        let alien = "totally/alien/key";
        metadata_store
            .object_store()
            .put(alien, Bytes::from_static(b"metadata alien"))
            .await
            .unwrap();
        let blob_alien = "v2/blobs/sha256/aa/not-a-digest/data";
        blob_store
            .object_store()
            .put(blob_alien, Bytes::from_static(b"blob alien"))
            .await
            .unwrap();

        scrub_apply(test_case).await;

        assert!(
            metadata_store.object_store().get(alien).await.is_err(),
            "the alien metadata key must be moved"
        );
        assert_eq!(
            metadata_store
                .object_store()
                .get(&format!("{LOST_AND_FOUND_PREFIX}/{alien}"))
                .await
                .unwrap(),
            b"metadata alien"
        );
        assert!(blob_store.object_store().get(blob_alien).await.is_err());
        assert_eq!(
            blob_store
                .object_store()
                .get(&format!("{LOST_AND_FOUND_PREFIX}/{blob_alien}"))
                .await
                .unwrap(),
            b"blob alien"
        );

        // The quarantined copies are a known category, so a second run emits
        // nothing.
        let actions = scrub_capture(test_case).await;
        assert!(
            actions.is_empty(),
            "quarantined keys must not be re-processed, got: {:?}",
            actions.iter().map(ToString::to_string).collect::<Vec<_>>()
        );
    })
    .await;
}

/// An upload session record that does not parse is deleted, so the session
/// reads as broken and prune's upload sweep reaps it; a parseable one is a
/// live session and must be left alone.
#[tokio::test]
async fn an_unparseable_session_record_is_deleted() {
    for_each_backend(async |test_case| {
        let blob_store = test_case.blob_store();

        let corrupt = format!(
            "v2/repositories/test-repo/_uploads/{}/session.json",
            Uuid::new_v4()
        );
        blob_store
            .object_store()
            .put(&corrupt, Bytes::from_static(b"not a session record"))
            .await
            .unwrap();
        let live = format!(
            "v2/repositories/test-repo/_uploads/{}/session.json",
            Uuid::new_v4()
        );
        let record = format!(
            r#"{{"last_activity":"{}","committed_offset":0,"hash_state":""}}"#,
            Utc::now().to_rfc3339()
        );
        blob_store
            .object_store()
            .put(&live, Bytes::from(record))
            .await
            .unwrap();

        scrub_apply(test_case).await;

        assert!(
            blob_store.object_store().get(&corrupt).await.is_err(),
            "an unparseable session record must be deleted"
        );
        assert!(
            blob_store.object_store().get(&live).await.is_ok(),
            "a live session record must survive the walk"
        );
    })
    .await;
}

/// An upload directory whose name is not a session id is no angos session, so
/// it reaches the unknown-key quarantine like any other unrecognized key.
#[tokio::test]
async fn an_upload_directory_that_is_not_a_session_is_quarantined() {
    for_each_backend(async |test_case| {
        let blob_store = test_case.blob_store();

        let stray = "v2/repositories/test-repo/_uploads/not-a-session/data";
        blob_store
            .object_store()
            .put(stray, Bytes::from_static(b"stray upload"))
            .await
            .unwrap();

        scrub_apply(test_case).await;

        assert!(
            blob_store.object_store().get(stray).await.is_err(),
            "the stray upload directory must be moved"
        );
        assert_eq!(
            blob_store
                .object_store()
                .get(&format!("{LOST_AND_FOUND_PREFIX}/{stray}"))
                .await
                .unwrap(),
            b"stray upload"
        );
    })
    .await;
}

#[tokio::test]
async fn delete_unknown_removes_aliens_without_quarantining() {
    for_each_backend(async |test_case| {
        let metadata_store = test_case.metadata_store();
        let blob_store = test_case.blob_store();

        let alien = "totally/alien/key";
        metadata_store
            .object_store()
            .put(alien, Bytes::from_static(b"metadata alien"))
            .await
            .unwrap();
        let blob_alien = "v2/blobs/sha256/aa/not-a-digest/data";
        blob_store
            .object_store()
            .put(blob_alien, Bytes::from_static(b"blob alien"))
            .await
            .unwrap();

        let sink: Arc<dyn ActionSink> = Arc::new(Executor::new_for_test(
            blob_store.clone(),
            metadata_store.clone(),
        ));
        let stats = run_passes_with(&blob_store, &metadata_store, sink, true).await;

        let meta_objects = metadata_store.object_store();
        assert!(meta_objects.get(alien).await.is_err());
        assert!(
            meta_objects
                .get(&format!("{LOST_AND_FOUND_PREFIX}/{alien}"))
                .await
                .is_err(),
            "a deleted unknown key must not be quarantined"
        );
        assert!(blob_store.object_store().get(blob_alien).await.is_err());
        assert!(
            blob_store
                .object_store()
                .get(&format!("{LOST_AND_FOUND_PREFIX}/{blob_alien}"))
                .await
                .is_err(),
        );
        assert_eq!(
            stats.quarantined.load(Ordering::Relaxed),
            2,
            "deleted unknowns still count in the unknown-key tally"
        );
    })
    .await;
}

#[tokio::test]
async fn corrupt_job_record_is_deleted_and_valid_one_kept() {
    for_each_backend(async |test_case| {
        let metadata_store = test_case.metadata_store();
        let objects = metadata_store.object_store();

        let corrupt = "_jobs/pending/replication/0000000000000000-corrupt.json";
        objects
            .put(corrupt, Bytes::from_static(b"not an envelope"))
            .await
            .unwrap();

        let valid = "_jobs/pending/replication/0000000000000000-valid.json";
        let envelope =
            JobEnvelope::new(Queue::Replication, "replication.push", "lock", &json!({})).unwrap();
        objects
            .put(valid, Bytes::from(serde_json::to_vec(&envelope).unwrap()))
            .await
            .unwrap();

        scrub_apply(test_case).await;

        assert!(
            objects.get(corrupt).await.is_err(),
            "an unparseable job record must be deleted"
        );
        assert!(
            objects.get(valid).await.is_ok(),
            "a parseable job record must survive"
        );
    })
    .await;
}

/// An enqueue crashing between the index write and the pending write leaves a
/// dedup index pointing at nothing. Scrub reclaims it once past the grace
/// period, and never one whose pending job is present or merely in flight.
#[tokio::test]
async fn orphan_job_index_is_reclaimed_age_gated() {
    // The replication queue, so the test registry's in-process cache claim
    // loop does not drain the pending job out from under the walk.
    fn index_key(lock_key: &str) -> String {
        job_lock_key_index_path("replication", &LockKey::new(lock_key).expect("lock key"))
    }

    fn index_body(storage_key: &str) -> Bytes {
        Bytes::from(json!({ "storage_key": storage_key }).to_string())
    }

    // Grace 0 (the shared fixtures): the orphan and the corrupt entry go.
    for_each_backend(async |test_case| {
        let metadata_store = test_case.metadata_store();
        let objects = metadata_store.object_store();

        let orphan = index_key("orphan");
        objects
            .put(&orphan, index_body("0000000000000000-vanished"))
            .await
            .unwrap();

        let live = index_key("live");
        objects
            .put(&live, index_body("0000000000000000-live"))
            .await
            .unwrap();
        let envelope =
            JobEnvelope::new(Queue::Replication, "replication.push", "live", &json!({})).unwrap();
        objects
            .put(
                "_jobs/pending/replication/0000000000000000-live.json",
                Bytes::from(serde_json::to_vec(&envelope).unwrap()),
            )
            .await
            .unwrap();

        let corrupt = index_key("corrupt");
        objects
            .put(&corrupt, Bytes::from_static(b"not an index"))
            .await
            .unwrap();

        scrub_apply(test_case).await;

        assert!(
            objects.head(&orphan).await.is_err(),
            "an orphan job index past the grace period must be reclaimed"
        );
        assert!(
            objects.head(&live).await.is_ok(),
            "a job index whose pending job exists must survive"
        );
        assert!(
            objects.head(&corrupt).await.is_err(),
            "an unparseable job index must be deleted"
        );
    })
    .await;

    // Default grace: the just-written orphan may be an enqueue mid-flight
    // between its two writes, so it is left alone.
    let stack = fs_test_stack();
    let objects = stack.metadata_store.object_store();
    let young = index_key("young");
    objects
        .put(&young, index_body("0000000000000000-vanished"))
        .await
        .unwrap();
    let sink: Arc<dyn ActionSink> = Arc::new(Executor::new_for_test(
        stack.blob_store.clone(),
        stack.metadata_store.clone(),
    ));
    run_passes(&stack.blob_store, &stack.metadata_store, sink).await;
    assert!(
        objects.head(&young).await.is_ok(),
        "a graced scrub must leave a young orphan job index"
    );
}

#[tokio::test]
async fn orphan_namespaces_are_left_alone_and_invalid_names_reclaimed() {
    for_each_backend(async |test_case| {
        // `ghost/app` resolves to no configured repository: scrub must not
        // touch it (config-relative clearing is prune's job).
        let orphan = &Namespace::new("ghost/app").unwrap();
        push_healthy_image(test_case, orphan).await;
        let metadata_store = test_case.metadata_store();

        // An invalid-name namespace directory, by contrast, is structural
        // garbage no API can address; scrub reclaims it unconditionally.
        let invalid_key = "v2/repositories/UPPER-CASE/_manifests/tags/v1/current/link".to_string();
        metadata_store
            .object_store()
            .put(&invalid_key, Bytes::from_static(b"{}"))
            .await
            .unwrap();

        scrub_apply(test_case).await;

        assert!(
            metadata_store.read_link(orphan, &tag("v1")).await.is_ok(),
            "an orphan namespace must survive scrub"
        );
        assert!(
            metadata_store
                .object_store()
                .get(&invalid_key)
                .await
                .is_err(),
            "an invalid-name namespace directory must be reclaimed"
        );
    })
    .await;
}

#[tokio::test]
async fn convergence_second_run_emits_zero_actions() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/converge").unwrap();
        let (manifest_digest, config_digest, layer_digest) =
            push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        // Mixed corruption: a missing pin, a phantom index entry, and an
        // alien key.
        metadata_store
            .remove_reference(
                namespace,
                &config_digest,
                &LinkKind::ReferencedBy(manifest_digest.clone()),
            )
            .await
            .unwrap();
        metadata_store
            .insert_reference(
                namespace,
                &layer_digest,
                &LinkKind::ReferencedBy(Digest::sha256_of_bytes(b"phantom")),
            )
            .await
            .unwrap();
        metadata_store
            .object_store()
            .put("stray/object", Bytes::from_static(b"junk"))
            .await
            .unwrap();
        scrub_apply(test_case).await;

        let actions = scrub_capture(test_case).await;
        assert!(
            actions.is_empty(),
            "the second run must find nothing left to do, got: {:?}",
            actions.iter().map(ToString::to_string).collect::<Vec<_>>()
        );
    })
    .await;
}

/// A grant entry with no backing link is settled damage once the reverify
/// re-reads the same inconsistency.
#[tokio::test]
async fn dangling_grant_entry_is_removed() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/dangling-entry").unwrap();
        push_healthy_image(test_case, namespace).await;
        let blob_store = test_case.blob_store();
        let metadata_store = test_case.metadata_store();

        let dangling = Digest::sha256_of_bytes(b"dangling-layer");
        blob_store
            .object_store()
            .put(&dangling.blob_path(), Bytes::from_static(b"dangling-layer"))
            .await
            .unwrap();
        let phantom = LinkKind::ReferencedBy(dangling.clone());
        metadata_store
            .insert_reference(namespace, &dangling, &phantom)
            .await
            .unwrap();

        scrub_apply(test_case).await;

        assert!(
            metadata_store
                .read_blob_index_namespace(namespace, &dangling)
                .await
                .is_err(),
            "a grant entry with no backing link is settled damage and must be removed"
        );
    })
    .await;
}

/// Write one raw tag entry with the given author timestamp and body,
/// returning its full key.
async fn put_tag_entry(
    metadata_store: &Arc<MetadataStore>,
    namespace: &Namespace,
    tag: &Tag,
    ts_millis: i64,
    deletion: bool,
    digest: &Digest,
    body: &'static [u8],
) -> String {
    let ts = DateTime::from_timestamp_millis(ts_millis).unwrap();
    let key = namespace.tag_entry_path(tag, ts, deletion, digest);
    metadata_store
        .object_store()
        .put(&key, Bytes::from_static(body))
        .await
        .unwrap();
    key
}

/// Three generations of one tag: the two superseded entries are demoted to
/// `!hist/`, resolution still returns the winner, and a second run finds
/// nothing left to do.
#[tokio::test]
async fn superseded_tag_entries_are_demoted_to_hist() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/demote").unwrap();
        let (manifest_digest, _, _) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();
        let tag = Tag::new("v1").unwrap();

        // The push tagged v1 at now; append two older generations.
        let old_a = put_tag_entry(
            &metadata_store,
            namespace,
            &tag,
            1_000_000,
            false,
            &manifest_digest,
            b"{}",
        )
        .await;
        let old_b = put_tag_entry(
            &metadata_store,
            namespace,
            &tag,
            2_000_000,
            false,
            &manifest_digest,
            b"{}",
        )
        .await;

        scrub_apply(test_case).await;

        let store = metadata_store.object_store();
        for old in [&old_a, &old_b] {
            assert!(
                store.head(old).await.is_err(),
                "superseded entry '{old}' must leave the tag prefix"
            );
            let name = old.rsplit_once('/').unwrap().1;
            let hist = namespace.tag_hist_path(&tag, name);
            assert!(
                store.head(&hist).await.is_ok(),
                "demoted entry must exist at '{hist}'"
            );
        }
        let resolved = metadata_store
            .read_link(namespace, &LinkKind::Tag(tag.clone()))
            .await
            .unwrap();
        assert_eq!(resolved.target, manifest_digest);

        let actions = scrub_capture(test_case).await;
        assert!(
            actions.is_empty(),
            "a second run must find nothing left to demote, got: {:?}",
            actions.iter().map(ToString::to_string).collect::<Vec<_>>()
        );
    })
    .await;
}

/// A same-millisecond set+del pair is one complete winner group: neither
/// half is demoted, while a strictly older entry still is.
#[tokio::test]
async fn same_millisecond_tie_group_is_never_split() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/tie-group").unwrap();
        let (manifest_digest, _, _) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();
        let tag = Tag::new("tie").unwrap();

        let ts = 5_000_000;
        let set_key = put_tag_entry(
            &metadata_store,
            namespace,
            &tag,
            ts,
            false,
            &manifest_digest,
            b"{}",
        )
        .await;
        let del_key = put_tag_entry(
            &metadata_store,
            namespace,
            &tag,
            ts,
            true,
            &manifest_digest,
            b"{}",
        )
        .await;
        let older = put_tag_entry(
            &metadata_store,
            namespace,
            &tag,
            ts - 10_000,
            false,
            &manifest_digest,
            b"{}",
        )
        .await;

        scrub_apply(test_case).await;

        let store = metadata_store.object_store();
        assert!(
            store.head(&set_key).await.is_ok(),
            "the winner group's set entry must stay"
        );
        assert!(
            store.head(&del_key).await.is_ok(),
            "the winner group's del entry must stay"
        );
        assert!(
            store.head(&older).await.is_err(),
            "the strictly older entry must be demoted"
        );
    })
    .await;
}

/// A superseded entry whose key is younger than the grace period may be a
/// racing push's write, so a graced scrub must keep it.
#[tokio::test]
async fn young_superseded_entry_is_kept() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/young-demote").unwrap();
        let (manifest_digest, _, _) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();
        let tag = Tag::new("v1").unwrap();

        // Superseded by ordinal, but the key itself was written just now.
        let old_entry = put_tag_entry(
            &metadata_store,
            namespace,
            &tag,
            1_000_000,
            false,
            &manifest_digest,
            b"{}",
        )
        .await;

        // Same stores, but a scrub whose grace period is real.
        let graced = Arc::new(MetadataStore::new(
            metadata_store.object_store().clone(),
            Settings {
                gc_grace_secs: 300,
                ..Settings::default()
            },
        ));
        let blob_store = test_case.blob_store();
        let sink: Arc<dyn ActionSink> =
            Arc::new(Executor::new_for_test(blob_store.clone(), graced.clone()));
        run_passes(&blob_store, &graced, sink).await;

        assert!(
            metadata_store.object_store().head(&old_entry).await.is_ok(),
            "a superseded entry inside the grace period must be kept"
        );
    })
    .await;
}

/// Demoted entries leave the `!tag/` prefix (so tag listings stop paging
/// past them) while the hist keys keep the original bodies.
#[tokio::test]
async fn demoted_entries_leave_the_listing_and_keep_their_bodies() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/demote-list").unwrap();
        let (manifest_digest, _, _) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();
        let tag = Tag::new("v1").unwrap();

        let body_a: &'static [u8] = br#"{"media_type":"application/vnd.gen.a"}"#;
        let body_b: &'static [u8] = br#"{"media_type":"application/vnd.gen.b"}"#;
        let old_a = put_tag_entry(
            &metadata_store,
            namespace,
            &tag,
            1_000_000,
            false,
            &manifest_digest,
            body_a,
        )
        .await;
        let old_b = put_tag_entry(
            &metadata_store,
            namespace,
            &tag,
            2_000_000,
            false,
            &manifest_digest,
            body_b,
        )
        .await;

        scrub_apply(test_case).await;

        let tags = metadata_store
            .list_tags(namespace, 100, None)
            .await
            .unwrap();
        assert_eq!(
            tags.items.iter().filter(|t| t.as_ref() == "v1").count(),
            1,
            "the tag must list exactly once after demotion"
        );
        let dir = namespace.tag_entry_dir(&tag);
        let page = metadata_store
            .object_store()
            .list(&dir, 1000, None)
            .await
            .unwrap();
        assert_eq!(
            page.items.len(),
            1,
            "only the winner may remain under the tag prefix, got {:?}",
            page.items
        );

        for (old, body) in [(&old_a, body_a), (&old_b, body_b)] {
            let name = old.rsplit_once('/').unwrap().1;
            let stored = metadata_store
                .object_store()
                .get(&namespace.tag_hist_path(&tag, name))
                .await
                .unwrap();
            assert_eq!(
                stored.as_slice(),
                body,
                "the demoted body must be preserved verbatim"
            );
        }
    })
    .await;
}

/// A referenced blob is pinned by its per-referrer entry alone; serving,
/// `can_read`, and the blob delete gate run on records and reference keys.
#[tokio::test]
async fn a_tracked_reference_is_pinned_by_its_entry_alone() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/no-link-files").unwrap();
        let (manifest_digest, _, layer_digest) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        // A pull still resolves, the blob stays readable, and the delete gate
        // still refuses, all off the reference entries alone.
        let registry = test_case.registry();
        let resolved = metadata_store
            .read_link(namespace, &tag("v1"))
            .await
            .unwrap();
        assert_eq!(resolved.target, manifest_digest);
        test_case
            .blob_store()
            .read(&resolved.target)
            .await
            .expect("the manifest body must stay readable");
        assert!(
            registry
                .metadata_store()
                .can_read(namespace, &layer_digest)
                .await
                .unwrap(),
            "the layer must be readable through its per-referrer entry"
        );
        let refused = registry
            .handle_delete_blob(DeleteBlobRequest {
                namespace: namespace.clone(),
                digest: layer_digest.clone(),
            })
            .await;
        assert!(
            matches!(refused, Err(RegistryError::BlobReferenced)),
            "the delete gate must refuse a blob a manifest still references"
        );
    })
    .await;
}

/// Craft one access entry at `at` in `dir`, as a replica's stamp would land.
async fn put_atime_entry(
    store: &Arc<MetadataStore>,
    namespace: &Namespace,
    link: &LinkKind,
    client: &str,
    at: DateTime<Utc>,
) {
    let body = serde_json::to_vec(&AccessEntry {
        client: client.to_string(),
        client_ip: None,
        method: None,
        at,
    })
    .expect("entry body");
    store
        .object_store()
        .put(
            &namespace.atime_entry_path(link, at, client).unwrap(),
            Bytes::from(body),
        )
        .await
        .expect("entry put");
}

#[tokio::test]
async fn the_atime_collector_keeps_the_newest_and_prunes_old_superseded_entries() {
    for_each_backend(async |test_case| {
        let namespace = Namespace::new("test-repo/atime").unwrap();
        let (manifest_digest, _, _) = push_healthy_image(test_case, &namespace).await;
        let metadata_store = test_case.metadata_store();
        let tag = Tag::new("v1").unwrap();
        let now = Utc::now();

        // Tag side: a newest entry, a superseded one inside the audit window,
        // and a superseded one past it.
        let tag_link = LinkKind::Tag(tag.clone());
        let dir = namespace.tag_atime_entry_dir(&tag);
        put_atime_entry(&metadata_store, &namespace, &tag_link, "alice", now).await;
        put_atime_entry(
            &metadata_store,
            &namespace,
            &tag_link,
            "bob",
            now - Duration::minutes(10),
        )
        .await;
        put_atime_entry(
            &metadata_store,
            &namespace,
            &tag_link,
            "carol",
            now - Duration::hours(2),
        )
        .await;

        // Revision side: an ancient newest entry alone stays forever.
        let rev_dir = namespace.revision_atime_entry_dir(&manifest_digest);
        put_atime_entry(
            &metadata_store,
            &namespace,
            &LinkKind::Digest(manifest_digest.clone()),
            "alice",
            now - Duration::days(30),
        )
        .await;

        scrub_apply(test_case).await;

        let page = metadata_store
            .object_store()
            .list(&dir, 10, None)
            .await
            .unwrap();
        assert_eq!(
            page.items.len(),
            2,
            "the newest entry and the young superseded one stay; the one past the window goes"
        );
        let rev_page = metadata_store
            .object_store()
            .list(&rev_dir, 10, None)
            .await
            .unwrap();
        assert_eq!(
            rev_page.items.len(),
            1,
            "the newest entry always stays, however old"
        );
    })
    .await;
}

#[tokio::test]
async fn an_undecodable_atime_entry_is_deleted() {
    for_each_backend(async |test_case| {
        let namespace = Namespace::new("test-repo/atime-corrupt").unwrap();
        push_healthy_image(test_case, &namespace).await;
        let metadata_store = test_case.metadata_store();
        let dir = namespace.tag_atime_entry_dir(&Tag::new("v1").unwrap());
        let now = Utc::now();

        let tag_link = LinkKind::Tag(Tag::new("v1").unwrap());
        metadata_store
            .object_store()
            .put(
                &namespace
                    .atime_entry_path(&tag_link, now, "mallory")
                    .unwrap(),
                Bytes::from_static(b"not json"),
            )
            .await
            .unwrap();
        put_atime_entry(
            &metadata_store,
            &namespace,
            &tag_link,
            "alice",
            now - Duration::hours(5),
        )
        .await;

        scrub_apply(test_case).await;

        let page = metadata_store
            .object_store()
            .list(&dir, 10, None)
            .await
            .unwrap();
        assert_eq!(
            page.items.len(),
            1,
            "the undecodable entry goes; the surviving decodable one is the kept newest"
        );
        let corrupt = namespace
            .atime_entry_path(&tag_link, now, "mallory")
            .unwrap();
        let surviving = format!("{dir}/{}", page.items[0]);
        assert_ne!(
            surviving, corrupt,
            "the corrupt entry must be the one deleted"
        );
    })
    .await;
}
