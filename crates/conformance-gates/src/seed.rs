use chrono::Utc;
use serde::Serialize;
use serde_json::json;

use crate::error::GateResult;
use crate::registry::RegistryClient;
use crate::store::{GateStore, sha256_hex};

/// The two seeded images: `gate` is the repair reference, `gate2` the
/// corruption victim.
pub const GATE_NS: &str = "conformance/gate";
pub const GATE2_NS: &str = "conformance/gate2";
pub const GATE_TAG: &str = "gate";
/// `GATE_NS` percent-encoded the way blob-index shard filenames encode it.
const GATE_NS_ENCODED: &str = "conformance%2Fgate";

/// Counters the first scrub run over the seeded store must report. Repairs
/// are a floor, not a pin: the gate2 back-link heal lands on run 2 because
/// its revision validation aborts at the corrupt shard on run 1.
pub const EXPECTED_QUARANTINED: u64 = 5;
pub const EXPECTED_CORRUPT: u64 = 7;
pub const EXPECTED_FAILURES_RUN1: u64 = 1;
pub const EXPECTED_MIN_REPAIRS: u64 = 9;

/// Storage keys of the seeded config-orphan jobs: structurally valid records
/// whose downstream / pull-through repository is not configured. Scrub must
/// leave them alone (structural vs config separation); the always-on prune
/// job sweep must reap them.
pub const ORPHAN_PENDING_JOB_KEY: &str =
    "_jobs/pending/replication/0000000000000002-gate-orphan.json";
pub const ORPHAN_FAILED_JOB_KEY: &str = "_jobs/failed/cache/0000000000000002-gate-orphan.json";

/// Upload container of the seeded orphan multipart (s3 only): a real
/// in-flight multipart with no session marker.
pub const ORPHAN_MULTIPART_KEY: &str =
    "v2/repositories/conformance/gate/_uploads/44444444-0000-4000-8000-000000000000/data";

/// What each quarantined alien must still contain, byte-for-byte, under
/// `_lost_and_found/`.
pub struct Alien {
    pub key: String,
    pub body: &'static str,
}

/// Digests and paths the corruption gate probes after repair. Everything is
/// derived from here so the seeded keys and the checked keys can never drift
/// apart.
#[derive(Serialize)]
pub struct Probes {
    pub gate_layer_digest: String,
    pub gate_config_digest: String,
    pub gate_manifest_digest: String,
    pub gate2_layer_digest: String,
    pub gate2_manifest_digest: String,
    pub orphan_digest: String,
    pub missing_digest: String,
    pub decoy_upload_path: String,
    /// Blob uploaded through the API with no manifest: prune's
    /// retention-grant sweep owns it, and only past the `-u` window.
    pub grant_only_digest: String,
    /// Blob whose bytes were deleted out from under its index entry: prune's
    /// byteless sweep owns it, and only past the `-u` window.
    pub byteless_digest: String,
    /// Whether an orphan multipart was seeded (s3 mode only).
    pub multipart_seeded: bool,
}

impl Probes {
    /// Links a repair run must have recreated from the intact manifest.
    pub fn recreated_links(&self) -> Vec<String> {
        vec![
            format!(
                "v2/repositories/{GATE_NS}/_layers/sha256/{}/link",
                self.gate_layer_digest
            ),
            format!(
                "v2/repositories/{GATE_NS}/_config/sha256/{}/link",
                self.gate_config_digest
            ),
            format!(
                "v2/repositories/{GATE_NS}/_manifests/revisions/sha256/{}/link",
                self.gate_manifest_digest
            ),
        ]
    }

    /// Every seeded artifact that must be gone once the store converges.
    pub fn gone_keys(&self) -> Vec<String> {
        let mut gone = vec![
            format!("v2/repositories/{GATE_NS}/_manifests/tags/dangling/current/link"),
            format!("v2/repositories/{GATE_NS}/_manifests/tags/-bad/current/link"),
            format!("v2/repositories/{GATE_NS}/_manifests/tags/garbled/current/link"),
            format!(
                "v2/repositories/{GATE_NS}/_blobs/sha256/{}/link",
                self.orphan_digest
            ),
            "v2/repositories/UPPER-NS/_manifests/tags/v1/current/link".to_string(),
            "v2/repositories/UPPER-UP/_uploads/00000000-0000-4000-8000-000000000000/data"
                .to_string(),
            format!(
                "v2/blobs/sha256/{}/{}/data",
                &self.orphan_digest[..2],
                self.orphan_digest
            ),
            format!(
                "v2/repositories/{GATE_NS}/_manifests/referrers/sha256/{}",
                self.gate_manifest_digest
            ),
            "_jobs/pending/replication/0000000000000000-gate-junk.json".to_string(),
            "_jobs/failed/cache/0000000000000000-gate-junk.json".to_string(),
            "_jobs/index/replication/gate-junk.json".to_string(),
            format!(
                "v2/repositories/{GATE_NS}/_uploads/11111111-0000-4000-8000-000000000000/startedat"
            ),
        ];
        gone.extend(self.aliens().into_iter().map(|alien| alien.key));
        gone
    }

    /// The five alien keys seeded at different store depths, with the bytes
    /// their quarantined copies must preserve.
    pub fn aliens(&self) -> Vec<Alien> {
        vec![
            Alien {
                key: "zz-alien/at-root".to_string(),
                body: "alien-root",
            },
            Alien {
                key: "v2/zz-alien-under-v2".to_string(),
                body: "alien-v2",
            },
            Alien {
                key: format!("v2/repositories/{GATE_NS}/no-marker-file"),
                body: "alien-ns",
            },
            Alien {
                key: format!(
                    "v2/blobs/sha256/{}/{}/stray",
                    &self.gate_layer_digest[..2],
                    self.gate_layer_digest
                ),
                body: "alien-blobdir",
            },
            Alien {
                key: format!(
                    "v2/repositories/{GATE_NS}/_uploads/22222222-0000-4000-8000-000000000000/stray"
                ),
                body: "alien-upload",
            },
        ]
    }

    /// The gate2 config link whose `referenced_by` set was damaged.
    pub fn gate2_config_link(&self) -> String {
        format!(
            "v2/repositories/{GATE2_NS}/_config/sha256/{}/link",
            self.gate_config_digest
        )
    }

    /// Canonical data key of the gate image's layer, the audit-teeth target.
    pub fn gate_layer_data(&self) -> String {
        blob_data_key(&self.gate_layer_digest)
    }

    /// Canonical data key of the grant-only blob.
    pub fn grant_only_data(&self) -> String {
        blob_data_key(&self.grant_only_digest)
    }

    /// The grant-only blob's per-namespace index shard.
    pub fn grant_only_shard(&self) -> String {
        shard_key(&self.grant_only_digest)
    }

    /// The byteless blob's surviving index shard.
    pub fn byteless_shard(&self) -> String {
        shard_key(&self.byteless_digest)
    }

    /// Key prefixes a repair run may legitimately touch. A snapshot-diff line
    /// outside these means scrub damaged innocent data.
    pub fn blast_prefixes(&self) -> Vec<String> {
        let blob_container = |digest: &str| format!("v2/blobs/sha256/{}/{digest}/", &digest[..2]);
        vec![
            format!("v2/repositories/{GATE_NS}/"),
            format!("v2/repositories/{GATE2_NS}/"),
            "v2/repositories/UPPER-NS/".to_string(),
            "v2/repositories/UPPER-UP/".to_string(),
            blob_container(&self.gate_layer_digest),
            blob_container(&self.gate_config_digest),
            blob_container(&self.gate_manifest_digest),
            blob_container(&self.gate2_layer_digest),
            blob_container(&self.gate2_manifest_digest),
            blob_container(&self.orphan_digest),
            blob_container(&self.grant_only_digest),
            blob_container(&self.byteless_digest),
            "zz-alien/".to_string(),
            "v2/zz-alien-under-v2".to_string(),
            "_jobs/pending/replication/0000000000000000-gate-junk.json".to_string(),
            "_jobs/failed/cache/0000000000000000-gate-junk.json".to_string(),
            "_jobs/index/replication/gate-junk.json".to_string(),
            ORPHAN_PENDING_JOB_KEY.to_string(),
            ORPHAN_FAILED_JOB_KEY.to_string(),
            "_lost_and_found/".to_string(),
            ".tx-".to_string(),
        ]
    }
}

/// Push the two gate images and the decoy upload through the API, then
/// damage the store directly: one defect per failure class the walk claims
/// to handle. Returns the probes every later assertion derives from.
#[allow(clippy::too_many_lines)]
pub async fn seed_defects(store: &GateStore, registry: &RegistryClient) -> GateResult<Probes> {
    let gate = registry
        .push_image(GATE_NS, GATE_TAG, b"scrub-gate-layer")
        .await?;
    let gate2 = registry
        .push_image(GATE2_NS, GATE_TAG, b"scrub-gate2-layer")
        .await?;
    // Decoy: a fresh in-flight upload session every gate must leave alone.
    let decoy_upload_path = registry.start_upload(GATE_NS).await?;

    // Prune fodder, seeded through the real flows so its index state is
    // consistent: a grant-only blob (uploaded, manifest never pushed) and a
    // byteless blob (bytes deleted out from under a live index entry). Scrub
    // must not touch either; prune owns them past the `-u` window.
    let grant_only_digest = registry
        .upload_blob(GATE_NS, b"grant-only-blob-bytes")
        .await?;
    let byteless_digest = registry
        .upload_blob(GATE_NS, b"byteless-shard-blob-bytes")
        .await?;
    store.delete(&blob_data_key(&byteless_digest)).await?;

    // An in-flight multipart with no session marker (s3 only): prune's
    // multipart sweep must abort it past the window, and only then. The pins
    // need `ListMultipartUploads` to work; rustfs accepts multiparts but
    // never lists them, leaving both the sweep and the gate blind, so probe
    // the capability and skip (loudly) instead of passing vacuously.
    let mut multipart_seeded = false;
    if store.is_s3() {
        store.seed_orphan_multipart(ORPHAN_MULTIPART_KEY).await?;
        multipart_seeded = store.multipart_count().await? > 0;
        if !multipart_seeded {
            store.abort_upload(ORPHAN_MULTIPART_KEY).await?;
            println!(
                "backend does not list in-flight multiparts; the multipart sweep pins are skipped"
            );
        }
    }

    let orphan_content = "orphan-blob-bytes";
    let probes = Probes {
        gate_layer_digest: gate.layer_digest,
        gate_config_digest: gate.config_digest,
        gate_manifest_digest: gate.manifest_digest,
        gate2_layer_digest: gate2.layer_digest,
        gate2_manifest_digest: gate2.manifest_digest,
        orphan_digest: sha256_hex(orphan_content.as_bytes()),
        missing_digest: sha256_hex(b"never-uploaded-content"),
        decoy_upload_path,
        grant_only_digest,
        byteless_digest,
        multipart_seeded,
    };
    println!(
        "pushed gate images ({GATE_NS}:{GATE_TAG}, {GATE2_NS}:{GATE_TAG}); decoy upload at {}",
        probes.decoy_upload_path
    );

    // Repair defects: deleted derivable links (layer, config, revision).
    for link in probes.recreated_links() {
        store.delete(&link).await?;
    }

    // A tag whose target blob does not exist, and a tag directory whose name
    // fails the OCI tag grammar.
    let dangling = format!(
        "{{\"target\":\"sha256:{}\",\"created_at\":null}}",
        probes.missing_digest
    );
    store
        .put(
            &format!("v2/repositories/{GATE_NS}/_manifests/tags/dangling/current/link"),
            dangling.clone(),
        )
        .await?;
    store
        .put(
            &format!("v2/repositories/{GATE_NS}/_manifests/tags/-bad/current/link"),
            dangling,
        )
        .await?;

    // Directories whose namespace names fail validation (manifest and upload
    // sides).
    store
        .put(
            "v2/repositories/UPPER-NS/_manifests/tags/v1/current/link",
            "{}",
        )
        .await?;
    store
        .put(
            "v2/repositories/UPPER-UP/_uploads/00000000-0000-4000-8000-000000000000/data",
            "zombie",
        )
        .await?;

    // Orphan blob: bytes at a canonical path with no index references.
    store
        .put(
            &format!(
                "v2/blobs/sha256/{}/{}/data",
                &probes.orphan_digest[..2],
                probes.orphan_digest
            ),
            orphan_content,
        )
        .await?;

    // Orphan referrer: entry whose referrer manifest is not a current
    // revision.
    let ghost = sha256_hex(b"ghost-referrer");
    store
        .put(
            &format!(
                "v2/repositories/{GATE_NS}/_manifests/referrers/sha256/{}/sha256/{ghost}/link",
                probes.gate_manifest_digest
            ),
            format!("{{\"target\":\"sha256:{ghost}\",\"created_at\":null}}"),
        )
        .await?;

    // Back-link damage on gate2's config link: a stale referrer entry and the
    // real revision's entry missing.
    store
        .put(
            &probes.gate2_config_link(),
            format!(
                "{{\"target\":\"sha256:{0}\",\"created_at\":null,\"referenced_by\":[\"sha256:{1}\"]}}",
                probes.gate_config_digest, probes.missing_digest
            ),
        )
        .await?;

    // Corrupt-content defects, deleted outright by the walk.
    let gate2_shard = format!(
        "v2/blobs/sha256/{}/{}/refs/conformance%2Fgate2.json",
        &probes.gate2_layer_digest[..2],
        probes.gate2_layer_digest
    );
    let corrupt: [(String, &str); 7] = [
        (
            format!("v2/repositories/{GATE_NS}/_manifests/tags/garbled/current/link"),
            "not link metadata",
        ),
        (
            format!(
                "v2/repositories/{GATE_NS}/_blobs/sha256/{}/link",
                probes.orphan_digest
            ),
            "not link metadata either",
        ),
        (gate2_shard, "not a shard"),
        (
            format!(
                "v2/repositories/{GATE_NS}/_uploads/11111111-0000-4000-8000-000000000000/startedat"
            ),
            "not a timestamp",
        ),
        (
            "_jobs/pending/replication/0000000000000000-gate-junk.json".to_string(),
            "not an envelope",
        ),
        (
            "_jobs/failed/cache/0000000000000000-gate-junk.json".to_string(),
            "not a dead letter",
        ),
        (
            "_jobs/index/replication/gate-junk.json".to_string(),
            "not an index entry",
        ),
    ];
    for (key, body) in corrupt {
        store.put(&key, body).await?;
    }

    // Alien keys, quarantined with bytes preserved.
    for alien in probes.aliens() {
        store.put(&alien.key, alien.body).await?;
    }

    // Config-orphan jobs: structurally valid records whose targets left the
    // configuration. Scrub must leave them alone; the always-on prune sweep
    // must reap them.
    let pending = json!({
        "id": "gate-orphan-pending",
        "queue": "replication",
        "kind": "replication.push",
        "lock_key": format!("{GATE_NS}:gate"),
        "created_at": Utc::now(),
        "attempts": 0,
        "max_attempts": 5,
        "payload": {
            "downstream": "gate-ghost-downstream",
            "namespace": GATE_NS,
            "tag": GATE_TAG,
            "kind": "replication.push",
        },
    });
    store
        .put(ORPHAN_PENDING_JOB_KEY, serde_json::to_vec(&pending)?)
        .await?;
    let failed = json!({
        "id": "gate-orphan-failed",
        "queue": "cache",
        "kind": "cache.fetch_blob",
        "lock_key": format!("{GATE_NS}:blob"),
        "created_at": Utc::now(),
        "attempts": 5,
        "max_attempts": 5,
        "payload": {
            "namespace": GATE_NS,
            "digest": format!("sha256:{}", probes.missing_digest),
        },
        "last_error": "upstream gone",
        "failed_at": Utc::now(),
    });
    store
        .put(ORPHAN_FAILED_JOB_KEY, serde_json::to_vec(&failed)?)
        .await?;

    let seeded = if multipart_seeded { 27 } else { 26 };
    println!("seeded {seeded} defects across 4 classes plus 2 decoys");
    Ok(probes)
}

/// Canonical `data` key of a blob digest.
fn blob_data_key(digest: &str) -> String {
    format!("v2/blobs/sha256/{}/{digest}/data", &digest[..2])
}

/// The `GATE_NS` blob-index shard key of a blob digest.
fn shard_key(digest: &str) -> String {
    format!(
        "v2/blobs/sha256/{}/{digest}/refs/{GATE_NS_ENCODED}.json",
        &digest[..2]
    )
}
