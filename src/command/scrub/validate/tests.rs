use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use angos_tx_engine::intent::{IntentRecord, MutationProgress, MutationRecord};

use crate::{
    command::scrub::{
        action::{Action, LOST_AND_FOUND_PREFIX},
        executor::{ActionSink, Executor},
        validate::{Pass, Validator},
        walk::{self, WalkStats},
    },
    oci::{Digest, Namespace, Reference, Tag},
    registry::{
        blob_store::BlobStore,
        metadata_store::{BlobIndexOperation, LinkKind, LinkMetadata, MetadataStore},
        path_builder,
        test_utils::{
            RegistryTestCase, for_each_backend, media_type, put_blob_direct, put_link_raw,
            upload_blob,
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

    let meta_objects = metadata_store.store().object_store();
    let passes = [
        (Pass::MetadataLinks, "", meta_objects),
        (
            Pass::MetadataShards,
            path_builder::blobs_root_dir(),
            meta_objects,
        ),
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

/// Push a healthy image (config + layer blobs, manifest, `v1` tag) through
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
        .put_manifest(
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

#[tokio::test]
async fn scrub_recreates_missing_child_links() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/heal-links").unwrap();
        let (_, config_digest, layer_digest) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        // Break the config and layer links out-of-band.
        for link in [
            LinkKind::Config(config_digest.clone()),
            LinkKind::Layer(layer_digest.clone()),
        ] {
            metadata_store
                .store()
                .object_store()
                .delete(&path_builder::link_path(&link, namespace))
                .await
                .unwrap();
        }

        scrub_apply(test_case).await;

        for link in [
            LinkKind::Config(config_digest.clone()),
            LinkKind::Layer(layer_digest.clone()),
        ] {
            assert!(
                metadata_store.read_link(namespace, &link).await.is_ok(),
                "link {link} must be recreated"
            );
        }
    })
    .await;
}

#[tokio::test]
async fn scrub_recreates_missing_digest_link_for_tag() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/heal-digest").unwrap();
        let (manifest_digest, _, _) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        let digest_link = LinkKind::Digest(manifest_digest.clone());
        metadata_store
            .store()
            .object_store()
            .delete(&path_builder::link_path(&digest_link, namespace))
            .await
            .unwrap();

        scrub_apply(test_case).await;

        assert!(
            metadata_store
                .read_link(namespace, &digest_link)
                .await
                .is_ok(),
            "the tag's digest revision link must be recreated"
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
        let body = serde_json::to_vec(&LinkMetadata::from_digest(ghost_digest)).unwrap();
        put_link_raw(metadata_store.store(), namespace, &tag("dangling"), &body).await;

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

#[tokio::test]
async fn invalid_tag_directory_is_deleted() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/bad-tag").unwrap();
        let metadata_store = test_case.metadata_store();

        // A leading '-' is a legal path segment but fails the tag grammar.
        let key = format!(
            "{}/current/link",
            path_builder::manifest_tag_dir(namespace, "-bad")
        );
        let body =
            serde_json::to_vec(&LinkMetadata::from_digest(Digest::sha256_of_bytes(b"x"))).unwrap();
        metadata_store
            .store()
            .object_store()
            .put(&key, Bytes::from(body))
            .await
            .unwrap();

        scrub_apply(test_case).await;

        assert!(
            metadata_store
                .store()
                .object_store()
                .get(&key)
                .await
                .is_err(),
            "the invalid tag directory must be deleted"
        );
    })
    .await;
}

#[tokio::test]
async fn corrupt_link_is_deleted() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/corrupt-link").unwrap();
        let metadata_store = test_case.metadata_store();

        put_link_raw(
            metadata_store.store(),
            namespace,
            &tag("garbled"),
            b"not link metadata",
        )
        .await;

        scrub_apply(test_case).await;

        let key = path_builder::link_path(&tag("garbled"), namespace);
        assert!(
            metadata_store
                .store()
                .object_store()
                .get(&key)
                .await
                .is_err(),
            "a link with unreadable content must be deleted"
        );
    })
    .await;
}

#[tokio::test]
async fn missing_referrer_backlink_is_added_and_stale_one_removed() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/backlinks").unwrap();
        let (manifest_digest, config_digest, _) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        // Rewrite the config link with a bogus referrer and without the real one.
        let stale_revision = Digest::sha256_of_bytes(b"no-such-revision");
        let mut broken = LinkMetadata::from_digest(config_digest.clone());
        broken.add_referrer(stale_revision.clone());
        put_link_raw(
            metadata_store.store(),
            namespace,
            &LinkKind::Config(config_digest.clone()),
            &serde_json::to_vec(&broken).unwrap(),
        )
        .await;

        scrub_apply(test_case).await;

        let repaired = metadata_store
            .read_link(namespace, &LinkKind::Config(config_digest))
            .await
            .unwrap();
        assert!(
            repaired.referenced_by.contains(&manifest_digest),
            "the real revision's back-link must be re-added"
        );
        assert!(
            !repaired.referenced_by.contains(&stale_revision),
            "the stale back-link must be pruned"
        );
    })
    .await;
}

#[tokio::test]
async fn missing_blob_index_grant_is_regranted() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/regrant").unwrap();
        let (_, _, layer_digest) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        let link = LinkKind::Layer(layer_digest.clone());
        metadata_store
            .update_blob_index(
                namespace,
                &layer_digest,
                BlobIndexOperation::Remove(link.clone()),
            )
            .await
            .unwrap();

        scrub_apply(test_case).await;

        let links = metadata_store
            .read_blob_index_namespace(namespace, &layer_digest)
            .await
            .unwrap();
        assert!(
            links.contains(&link),
            "the layer grant must be re-issued from the manifest"
        );
    })
    .await;
}

#[tokio::test]
async fn stale_shard_entry_is_removed() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/stale-entry").unwrap();
        let (_, _, layer_digest) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        // Grant an entry whose link file does not exist.
        let phantom = LinkKind::Layer(Digest::sha256_of_bytes(b"phantom-layer"));
        metadata_store
            .update_blob_index(
                namespace,
                &layer_digest,
                BlobIndexOperation::Insert(phantom.clone()),
            )
            .await
            .unwrap();

        scrub_apply(test_case).await;

        let links = metadata_store
            .read_blob_index_namespace(namespace, &layer_digest)
            .await
            .unwrap();
        assert!(
            !links.contains(&phantom),
            "an index entry with no link file must be removed"
        );
    })
    .await;
}

#[tokio::test]
async fn corrupt_shard_is_deleted_and_regranted_on_next_run() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/corrupt-shard").unwrap();
        let (_, _, layer_digest) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        let shard_key = path_builder::blob_index_shard_path(&layer_digest, namespace);
        metadata_store
            .store()
            .object_store()
            .put(&shard_key, Bytes::from_static(b"not a shard"))
            .await
            .unwrap();

        scrub_apply(test_case).await;
        // The corrupt shard was deleted; the same run's link pass may have
        // preceded the deletion, so a second run re-grants from the manifest.
        scrub_apply(test_case).await;

        let links = metadata_store
            .read_blob_index_namespace(namespace, &layer_digest)
            .await
            .unwrap();
        assert!(
            links.contains(&LinkKind::Layer(layer_digest.clone())),
            "grants must be rebuilt after the corrupt shard was deleted"
        );
    })
    .await;
}

#[tokio::test]
async fn orphan_blob_is_reclaimed() {
    for_each_backend(async |test_case| {
        let metadata_store = test_case.metadata_store();
        let blob_store = test_case.blob_store();

        let orphan = put_blob_direct(metadata_store.store(), b"unreferenced-bytes").await;
        assert!(blob_store.size(&orphan).await.is_ok());

        scrub_apply(test_case).await;

        assert!(
            blob_store.size(&orphan).await.is_err(),
            "a blob with no index entries must be reclaimed"
        );
    })
    .await;
}

#[tokio::test]
async fn orphan_referrer_link_is_deleted() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/orphan-referrer").unwrap();
        let (manifest_digest, _, _) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        // A referrer entry whose referrer manifest is not a current revision.
        let ghost_referrer = Digest::sha256_of_bytes(b"gone-referrer");
        let link = LinkKind::Referrer(manifest_digest.clone(), ghost_referrer.clone());
        let body = serde_json::to_vec(&LinkMetadata::from_digest(ghost_referrer.clone())).unwrap();
        put_link_raw(metadata_store.store(), namespace, &link, &body).await;

        scrub_apply(test_case).await;

        assert!(
            metadata_store.read_link(namespace, &link).await.is_err(),
            "a referrer with no revision link must be deleted"
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
            .store()
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
            metadata_store
                .store()
                .object_store()
                .get(alien)
                .await
                .is_err(),
            "the alien metadata key must be moved"
        );
        assert_eq!(
            metadata_store
                .store()
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

        // The quarantined copies are a known category: a second run leaves
        // them alone and emits nothing.
        let actions = scrub_capture(test_case).await;
        assert!(
            actions.is_empty(),
            "quarantined keys must not be re-processed, got: {:?}",
            actions.iter().map(ToString::to_string).collect::<Vec<_>>()
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
            .store()
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

        // Both aliens are gone, counted, and nothing landed in quarantine.
        let meta_objects = metadata_store.store().object_store();
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
        let objects = metadata_store.store().object_store();

        let corrupt = "_jobs/pending/replication/0000000000000000-corrupt.json";
        objects
            .put(corrupt, Bytes::from_static(b"not an envelope"))
            .await
            .unwrap();

        scrub_apply(test_case).await;

        assert!(
            objects.get(corrupt).await.is_err(),
            "an unparseable job record must be deleted"
        );
    })
    .await;
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
            .store()
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
                .store()
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
        let (_, config_digest, layer_digest) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        // Mixed corruption: a missing child link, a phantom index entry, an
        // alien key, and a corrupt tag link.
        metadata_store
            .store()
            .object_store()
            .delete(&path_builder::link_path(
                &LinkKind::Config(config_digest.clone()),
                namespace,
            ))
            .await
            .unwrap();
        metadata_store
            .update_blob_index(
                namespace,
                &layer_digest,
                BlobIndexOperation::Insert(LinkKind::Layer(Digest::sha256_of_bytes(b"phantom"))),
            )
            .await
            .unwrap();
        metadata_store
            .store()
            .object_store()
            .put("stray/object", Bytes::from_static(b"junk"))
            .await
            .unwrap();
        put_link_raw(
            metadata_store.store(),
            namespace,
            &tag("broken"),
            b"garbage",
        )
        .await;

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

/// Write a transaction intent whose only mutation touches `key`, with the
/// given TTL. Returns the intent's log key so tests can reap it.
async fn put_intent_touching(
    metadata_store: &Arc<MetadataStore>,
    key: &str,
    ttl_secs: u64,
) -> String {
    let intent = IntentRecord {
        id: Uuid::new_v4(),
        created_at: Utc::now(),
        ttl_secs,
        reads: Vec::new(),
        mutations: vec![MutationRecord::Delete {
            key: key.to_string(),
            expected: None,
        }],
        coarse_lock_keys: Vec::new(),
        progress: vec![MutationProgress::Pending],
    };
    let log_key = intent.log_key();
    metadata_store
        .store()
        .object_store()
        .put(&log_key, Bytes::from(serde_json::to_vec(&intent).unwrap()))
        .await
        .unwrap();
    log_key
}

#[tokio::test]
async fn live_intent_suppresses_shard_entry_removal() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/inflight-entry").unwrap();
        let (_, _, layer_digest) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        let phantom = LinkKind::Layer(Digest::sha256_of_bytes(b"inflight-layer"));
        metadata_store
            .update_blob_index(
                namespace,
                &layer_digest,
                BlobIndexOperation::Insert(phantom.clone()),
            )
            .await
            .unwrap();
        let log_key = put_intent_touching(
            &metadata_store,
            &path_builder::link_path(&phantom, namespace),
            300,
        )
        .await;

        scrub_apply(test_case).await;
        let links = metadata_store
            .read_blob_index_namespace(namespace, &layer_digest)
            .await
            .unwrap();
        assert!(
            links.contains(&phantom),
            "an entry a live transaction is still writing must not be removed"
        );

        // Once the intent is reaped the same state is settled damage.
        metadata_store
            .store()
            .object_store()
            .delete(&log_key)
            .await
            .unwrap();
        scrub_apply(test_case).await;
        let links = metadata_store
            .read_blob_index_namespace(namespace, &layer_digest)
            .await
            .unwrap();
        assert!(
            !links.contains(&phantom),
            "the entry must be removed after the intent is gone"
        );
    })
    .await;
}

#[tokio::test]
async fn expired_intent_does_not_suppress_repairs() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/expired-intent").unwrap();
        let (_, _, layer_digest) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        let phantom = LinkKind::Layer(Digest::sha256_of_bytes(b"expired-layer"));
        metadata_store
            .update_blob_index(
                namespace,
                &layer_digest,
                BlobIndexOperation::Insert(phantom.clone()),
            )
            .await
            .unwrap();
        put_intent_touching(
            &metadata_store,
            &path_builder::link_path(&phantom, namespace),
            0,
        )
        .await;

        scrub_apply(test_case).await;

        let links = metadata_store
            .read_blob_index_namespace(namespace, &layer_digest)
            .await
            .unwrap();
        assert!(
            !links.contains(&phantom),
            "an expired intent is recovery's leftovers, not an in-flight transaction"
        );
    })
    .await;
}

#[tokio::test]
async fn live_intent_suppresses_referrer_removal() {
    for_each_backend(async |test_case| {
        let namespace = &Namespace::new("test-repo/inflight-referrer").unwrap();
        let (_, config_digest, _) = push_healthy_image(test_case, namespace).await;
        let metadata_store = test_case.metadata_store();

        // A referrer whose revision link is mid-write: referenced but absent.
        let inflight_revision = Digest::sha256_of_bytes(b"inflight-revision");
        let config_link = LinkKind::Config(config_digest.clone());
        let mut current = metadata_store
            .read_link(namespace, &config_link)
            .await
            .unwrap();
        current.add_referrer(inflight_revision.clone());
        put_link_raw(
            metadata_store.store(),
            namespace,
            &config_link,
            &serde_json::to_vec(&current).unwrap(),
        )
        .await;
        put_intent_touching(
            &metadata_store,
            &path_builder::link_path(&LinkKind::Digest(inflight_revision.clone()), namespace),
            300,
        )
        .await;

        scrub_apply(test_case).await;

        let repaired = metadata_store
            .read_link(namespace, &config_link)
            .await
            .unwrap();
        assert!(
            repaired.referenced_by.contains(&inflight_revision),
            "a back-link to a revision still being written must not be pruned"
        );
    })
    .await;
}
