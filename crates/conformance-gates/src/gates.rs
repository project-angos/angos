use std::fs::{canonicalize, read_to_string, write};
use std::path::PathBuf;
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use reqwest::StatusCode;
use tokio::time::sleep;

use crate::angos::{AngosRunner, ScrubSummary};
use crate::error::{GateResult, ensure, fail};
use crate::registry::RegistryClient;
use crate::seed::{
    EXPECTED_CORRUPT, EXPECTED_FAILURES_RUN1, EXPECTED_MIN_REPAIRS, EXPECTED_QUARANTINED, GATE_NS,
    GATE_TAG, GATE2_NS, ORPHAN_FAILED_JOB_KEY, ORPHAN_PENDING_JOB_KEY, seed_defects,
};
use crate::store::{GateStore, Snapshot, sha256_hex, snapshot_diff, write_snapshot};

/// A healthy-store walk must see at least this many keys, or the listing is
/// broken and the zero-action result is vacuous.
const HEALTHY_WALK_FLOOR: u64 = 100;
/// Minimum manifests the full-catalog digest audit must cover.
const AUDIT_FLOOR: usize = 50;
/// A cascading repair must converge within this many scrub runs.
const FIXPOINT_MAX_RUNS: u32 = 5;
/// How many offending keys a diff-based failure message lists.
const DIFF_REPORT_LIMIT: usize = 20;
/// Pause before the aggressive `prune -u 1s` run, pushing the freshly seeded
/// age-gated fodder past the 1 second window.
const SWEEP_AGE_PAUSE: Duration = Duration::from_secs(2);
/// Images the populate helper pushes; enough to clear every gate floor.
pub const DEFAULT_POPULATE_IMAGES: usize = 60;

/// Everything a gate needs: byte-level store access, the registry API, the
/// angos CLI under test, and a state directory for failure forensics.
pub struct GateContext {
    pub store: GateStore,
    pub registry: RegistryClient,
    pub runner: AngosRunner,
    pub state_dir: PathBuf,
}

impl GateContext {
    fn state_path(&self, name: &str) -> PathBuf {
        self.state_dir.join(name)
    }

    async fn scrub_logged(&self, log_name: &str) -> GateResult<ScrubSummary> {
        let output = self
            .runner
            .run_logged(&["scrub"], &self.state_path(log_name))
            .await?;
        ScrubSummary::parse(&output)
    }

    async fn snapshot_to(&self, name: &str) -> GateResult<Snapshot> {
        let snap = self.store.snapshot().await?;
        write_snapshot(&self.state_path(name), &snap)?;
        Ok(snap)
    }
}

/// Dry-run purity (snapshot-identical), zero-action floors, prune sweep
/// proofs, real-run byte-identity outside engine prefixes, and a
/// full-catalog digest audit.
pub async fn healthy(ctx: &GateContext) -> GateResult<()> {
    // A healthy store carries no quarantined leftovers: anything under
    // `_lost_and_found/` means an earlier walk met data it could not place
    // and nobody looked.
    let quarantined = ctx.store.keys_under("_lost_and_found/").await?;
    ensure(quarantined.is_empty(), || {
        format!(
            "the store carries quarantined keys:\n{}",
            report_keys(&quarantined)
        )
    })?;

    let before_dry = ctx.snapshot_to("snap-before-dry").await?;
    let dry_output = ctx
        .runner
        .run_logged(&["scrub", "--dry-run"], &ctx.state_path("scrub-dry.log"))
        .await?;
    let prune_dry = ctx
        .runner
        .run_logged(&["prune", "--dry-run"], &ctx.state_path("prune-dry.log"))
        .await?;
    let after_dry = ctx.snapshot_to("snap-after-dry").await?;
    let dry_diff = snapshot_diff(&before_dry, &after_dry);
    ensure(dry_diff.is_empty(), || {
        format!("a dry run modified the store:\n{}", report_keys(&dry_diff))
    })?;

    let summary = ScrubSummary::parse(&dry_output)?;
    println!("{summary}");
    ensure(summary.is_all_zero(), || {
        "dry-run scrub proposed changes on a healthy store".to_string()
    })?;
    ensure(summary.walked >= HEALTHY_WALK_FLOOR, || {
        format!(
            "walk saw only {} keys; the listing is broken",
            summary.walked
        )
    })?;

    ensure(!prune_dry.contains("DRY RUN: would"), || {
        "prune proposed actions on a healthy store".to_string()
    })?;
    for proof in [
        "prune: found 0 orphan multipart",
        "found 0 orphan pending and 0 orphan dead-lettered replication",
        "found 0 orphan pending and 0 orphan dead-lettered cache",
    ] {
        ensure(prune_dry.contains(proof), || {
            format!("prune sweep proof missing from dry-run log: '{proof}'")
        })?;
    }

    // A real scrub on a healthy store may touch only engine-owned prefixes
    // (janitor reclaim); everything else must be byte-identical.
    let real = ctx.scrub_logged("scrub-real.log").await?;
    println!("{real}");
    ensure(real.is_all_zero(), || {
        "real scrub acted on a healthy store".to_string()
    })?;
    let after_real = ctx.snapshot_to("snap-after-real").await?;
    let illegal: Vec<String> = snapshot_diff(&after_dry, &after_real)
        .into_iter()
        .filter(|key| !key.starts_with(".tx-"))
        .collect();
    ensure(illegal.is_empty(), || {
        format!(
            "a healthy-store scrub modified non-engine keys:\n{}",
            report_keys(&illegal)
        )
    })?;

    ctx.registry.audit_digests(AUDIT_FLOOR).await?;
    println!(
        "GATE healthy: PASS (walked {}, dry-run byte-pure, real run byte-identical outside .tx-)",
        real.walked
    );
    Ok(())
}

/// Full defect matrix: pinned per-class counters on the first run, fixpoint
/// within bounded runs, per-artifact end states, blast-radius snapshot diff,
/// decoy survival, and the prune window pin.
#[allow(clippy::too_many_lines)]
pub async fn corruption(ctx: &GateContext) -> GateResult<()> {
    let baseline = ctx.snapshot_to("snap-baseline").await?;

    let probes = seed_defects(&ctx.store, &ctx.registry).await?;
    write(
        ctx.state_path("probes.json"),
        serde_json::to_vec_pretty(&probes)?,
    )?;

    // Run 1: every independent defect class must be counted exactly.
    let run1 = ctx.scrub_logged("scrub-1.log").await?;
    println!("{run1}");
    ensure(run1.quarantined == EXPECTED_QUARANTINED, || {
        format!(
            "run 1 quarantined {}, expected {EXPECTED_QUARANTINED}",
            run1.quarantined
        )
    })?;
    ensure(run1.corrupt == EXPECTED_CORRUPT, || {
        format!(
            "run 1 corrupt-deleted {}, expected {EXPECTED_CORRUPT}",
            run1.corrupt
        )
    })?;
    ensure(run1.failures == EXPECTED_FAILURES_RUN1, || {
        format!(
            "run 1 failures {}, expected {EXPECTED_FAILURES_RUN1}",
            run1.failures
        )
    })?;
    ensure(run1.repairs >= EXPECTED_MIN_REPAIRS, || {
        format!(
            "run 1 repairs {}, expected at least {EXPECTED_MIN_REPAIRS}",
            run1.repairs
        )
    })?;

    // Cascading repairs (a recreated link derives more state) must reach a
    // fixpoint within a bounded number of runs.
    let mut attempts = 1;
    loop {
        let fix = ctx.scrub_logged("scrub-fix.log").await?;
        attempts += 1;
        if fix.is_all_zero() {
            break;
        }
        ensure(attempts < FIXPOINT_MAX_RUNS, || {
            format!("no fixpoint after {attempts} runs")
        })?;
    }
    println!("fixpoint after {attempts} runs");

    // Per-artifact end states.
    for link in probes.recreated_links() {
        ensure(ctx.store.exists(&link).await?, || {
            format!("link not recreated: {link}")
        })?;
    }
    for gone in probes.gone_keys() {
        ensure(!ctx.store.exists(&gone).await?, || {
            format!("defect artifact survived: {gone}")
        })?;
    }
    for alien in probes.aliens() {
        let quarantined = format!("_lost_and_found/{}", alien.key);
        let body = ctx.store.body(&quarantined).await?;
        ensure(body.as_deref() == Some(alien.body.as_bytes()), || {
            format!("quarantined bytes wrong for {}", alien.key)
        })?;
    }
    let backlink = ctx
        .store
        .body(&probes.gate2_config_link())
        .await?
        .ok_or_else(|| fail("gate2 config link vanished during repair"))?;
    let backlink = String::from_utf8_lossy(&backlink);
    ensure(backlink.contains(&probes.gate2_manifest_digest), || {
        "missing back-link was not re-added".to_string()
    })?;
    ensure(!backlink.contains(&probes.missing_digest), || {
        "stale back-link was not pruned".to_string()
    })?;

    // Ownership boundary: scrub must have left every age-gated and
    // config-relative artifact for prune, all the way through the fixpoint.
    for (key, what) in [
        (probes.grant_only_data(), "grant-only blob bytes"),
        (probes.grant_only_shard(), "grant-only blob grant"),
        (probes.byteless_shard(), "byteless index entry"),
        (
            ORPHAN_PENDING_JOB_KEY.to_string(),
            "config-orphan pending job",
        ),
        (
            ORPHAN_FAILED_JOB_KEY.to_string(),
            "config-orphan dead letter",
        ),
    ] {
        ensure(ctx.store.exists(&key).await?, || {
            format!("scrub crossed into prune's domain: {what} gone ({key})")
        })?;
    }
    if probes.multipart_seeded {
        ensure(ctx.store.multipart_count().await? >= 1, || {
            "scrub crossed into prune's domain: orphan multipart gone".to_string()
        })?;
    }

    // Blast radius: the diff against the pre-seed baseline may only touch
    // the seeded prefixes; anything else means scrub damaged innocent data.
    let after_repair = ctx.snapshot_to("snap-after-repair").await?;
    let prefixes = probes.blast_prefixes();
    let out_of_radius: Vec<String> = snapshot_diff(&baseline, &after_repair)
        .into_iter()
        .filter(|key| !prefixes.iter().any(|prefix| key.starts_with(prefix)))
        .collect();
    ensure(out_of_radius.is_empty(), || {
        format!(
            "repairs touched keys outside the blast radius:\n{}",
            report_keys(&out_of_radius)
        )
    })?;

    // Decoy survival: the fresh in-flight upload session must still be alive.
    let decoy = ctx
        .registry
        .upload_status(&probes.decoy_upload_path)
        .await?;
    ensure(decoy == StatusCode::NO_CONTENT, || {
        format!("decoy in-flight upload was damaged (status {decoy})")
    })?;

    // Both gate images must be fully pullable, content-verified.
    for (ns, layer_digest) in [
        (GATE_NS, &probes.gate_layer_digest),
        (GATE2_NS, &probes.gate2_layer_digest),
    ] {
        ctx.registry.manifest(ns, GATE_TAG).await?;
        let digest = format!("sha256:{layer_digest}");
        let (status, bytes) = ctx.registry.blob(ns, &digest).await?;
        ensure(status == StatusCode::OK, || {
            format!("{ns} layer GET {status}")
        })?;
        ensure(sha256_hex(&bytes) == **layer_digest, || {
            format!("{ns} layer bytes corrupted")
        })?;
    }

    // Prune window pin: a session backdated past the window is reaped, the
    // fresh decoy survives.
    let old_uuid = "33333333-0000-4000-8000-000000000000";
    let started_at = format!("v2/repositories/{GATE_NS}/_uploads/{old_uuid}/startedat");
    let old_ts = (Utc::now() - TimeDelta::hours(2))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    ctx.store.put(&started_at, old_ts).await?;
    ctx.store
        .put(
            &format!("v2/repositories/{GATE_NS}/_uploads/{old_uuid}/data"),
            "stale upload bytes",
        )
        .await?;
    let prune_log = ctx
        .runner
        .run_logged(&["prune"], &ctx.state_path("prune.log"))
        .await?;
    ensure(!ctx.store.exists(&started_at).await?, || {
        "prune kept an upload past the -u window".to_string()
    })?;
    let decoy = ctx
        .registry
        .upload_status(&probes.decoy_upload_path)
        .await?;
    ensure(decoy == StatusCode::NO_CONTENT, || {
        format!("prune reaped a fresh in-flight upload (status {decoy})")
    })?;

    // The always-on job sweep must have classified and reaped exactly the two
    // config orphans, while the window still shields every age-gated sweep.
    for proof in [
        "found 1 orphan pending and 0 orphan dead-lettered replication",
        "found 0 orphan pending and 1 orphan dead-lettered cache",
    ] {
        ensure(prune_log.contains(proof), || {
            format!("prune job sweep proof missing: '{proof}'")
        })?;
    }
    for (key, what) in [
        (ORPHAN_PENDING_JOB_KEY, "config-orphan pending job"),
        (ORPHAN_FAILED_JOB_KEY, "config-orphan dead letter"),
    ] {
        ensure(!ctx.store.exists(key).await?, || {
            format!("{what} survived the always-on prune sweep")
        })?;
    }
    for (key, what) in [
        (probes.grant_only_data(), "grant-only blob bytes"),
        (probes.grant_only_shard(), "grant-only blob grant"),
        (probes.byteless_shard(), "byteless index entry"),
    ] {
        ensure(ctx.store.exists(&key).await?, || {
            format!("prune reaped a young artifact inside the -u window: {what} ({key})")
        })?;
    }
    if probes.multipart_seeded {
        ensure(ctx.store.multipart_count().await? >= 1, || {
            "prune aborted a young multipart inside the -u window".to_string()
        })?;
    }

    // Grant-only blobs are retention subjects: with a deleting policy but
    // inside the window, the in-flight shield alone must keep the grant.
    let retention_config = write_retention_config(ctx)?;
    ctx.runner
        .run_with_config(
            &retention_config,
            &["prune"],
            &ctx.state_path("prune-retention.log"),
        )
        .await?;
    ensure(ctx.store.exists(&probes.grant_only_data()).await?, || {
        "the -u in-flight shield did not protect a young grant from a deleting retention policy"
            .to_string()
    })?;

    // Past the window everything the shields protected must be reaped: the
    // grant (under the deleting policy), the byteless entry, the multipart.
    // The pause pushes the fodder past a 1s window; this run also reaps the
    // decoy session, which was asserted alive above.
    sleep(SWEEP_AGE_PAUSE).await;
    ctx.runner
        .run_with_config(
            &retention_config,
            &["prune", "-u", "1s"],
            &ctx.state_path("prune-aggressive.log"),
        )
        .await?;
    for (key, what) in [
        (probes.grant_only_data(), "grant-only blob bytes"),
        (probes.grant_only_shard(), "grant-only blob grant"),
        (probes.byteless_shard(), "byteless index entry"),
    ] {
        ensure(!ctx.store.exists(&key).await?, || {
            format!("{what} survived past the -u window ({key})")
        })?;
    }
    if probes.multipart_seeded {
        ensure(ctx.store.multipart_count().await? == 0, || {
            "orphan multipart survived past the -u window".to_string()
        })?;
    }

    // The quarantine's contents were verified above; empty it like the
    // operator would, then require the store to still converge with no
    // actions at all.
    ctx.store.delete_prefix("_lost_and_found").await?;
    let post_cleanup = ctx.scrub_logged("scrub-post-cleanup.log").await?;
    ensure(post_cleanup.is_all_zero(), || {
        format!("store did not stay converged after the prune sweeps: {post_cleanup}")
    })?;

    // Audit self-test: the digest audit must actually detect corruption, or
    // every audit pass in these gates is vacuous. Corrupt one tagged layer in
    // place, expect the audit to fail, restore, expect it to pass.
    let layer_key = probes.gate_layer_data();
    let original = ctx
        .store
        .body(&layer_key)
        .await?
        .ok_or_else(|| fail("gate layer bytes missing before the audit self-test"))?;
    ctx.store.put(&layer_key, "corrupted layer bytes").await?;
    ensure(ctx.registry.audit_digests(1).await.is_err(), || {
        "the digest audit failed to detect corrupted layer bytes".to_string()
    })?;
    ctx.store.put(&layer_key, original).await?;

    ctx.registry.audit_digests(AUDIT_FLOOR).await?;
    println!(
        "GATE corruption: PASS (counters pinned, {} end-states verified, blast radius clean, \
         decoys alive, prune sweeps pinned both sides of the window, audit teeth proven)",
        probes.gone_keys().len()
    );
    Ok(())
}

/// SIGKILL mid-scrub three times, then require convergence and full content
/// integrity.
pub async fn chaos(ctx: &GateContext) -> GateResult<()> {
    for round in 1..=3 {
        ctx.runner.kill_mid_scrub(round).await?;
    }

    // After repeated mid-run kills the store must still converge and every
    // image must still verify.
    ctx.scrub_logged("scrub-post-chaos.log").await?;
    let second = ctx.scrub_logged("scrub-post-chaos-2.log").await?;
    println!("{second}");
    ensure(second.is_all_zero(), || {
        "store did not converge after chaos kills".to_string()
    })?;
    ctx.registry.audit_digests(AUDIT_FLOOR).await?;
    println!("GATE chaos: PASS (3 mid-run kills, converged, content verified)");
    Ok(())
}

/// A manifest link with no stored media type (as a registry seeded before media
/// types were stored carries) serves no Content-Type, which go-containerregistry
/// clients (kaniko, crane) reject. `angos migrate` is the sole fix: it backfills
/// the media type from the manifest body so a later HEAD advertises it.
pub async fn manifest_content_type(ctx: &GateContext) -> GateResult<()> {
    let namespace = "conformance/ct-legacy";
    let pushed = ctx
        .registry
        .push_image(namespace, GATE_TAG, b"content-type-gate-layer")
        .await?;

    // A fresh tag whose link carries no media_type, the state migrate must repair.
    let tag = "legacy-no-media-type";
    let link_key = format!("v2/repositories/{namespace}/_manifests/tags/{tag}/current/link");
    let link = format!(
        r#"{{"target":"sha256:{}","created_at":null,"accessed_at":null}}"#,
        pushed.manifest_digest
    );
    ctx.store.put(&link_key, link).await?;

    // Before migrate the link has no media type, so HEAD serves no Content-Type.
    let (_, before) = ctx.registry.head_manifest(namespace, tag).await?;
    ensure(before.is_empty(), || {
        format!("a media-type-less link already served a Content-Type ('{before}'); test is vacuous")
    })?;

    ctx.runner
        .run_logged(&["migrate"], &ctx.state_path("migrate.log"))
        .await?;

    // Migrate rewrote the link with a media type recovered from the body ...
    let rewritten = ctx
        .store
        .body(&link_key)
        .await?
        .ok_or_else(|| fail("migrate deleted the manifest link"))?;
    ensure(
        String::from_utf8_lossy(&rewritten).contains("media_type"),
        || "migrate did not backfill the link's media type".to_string(),
    )?;

    // ... so HEAD now advertises the Content-Type go-containerregistry requires.
    let (status, content_type) = ctx.registry.head_manifest(namespace, tag).await?;
    ensure(status == StatusCode::OK, || {
        format!("HEAD after migrate returned {status}")
    })?;
    ensure(!content_type.is_empty(), || {
        "HEAD after migrate served no Content-Type".to_string()
    })?;
    println!(
        "GATE manifest-content-type: PASS (migrate backfilled media type; HEAD serves '{content_type}')"
    );
    Ok(())
}

/// Push enough synthetic images through the API to clear the gate walk and
/// audit floors, independently of how many manifests the conformance suite
/// happens to leave. Run before the gates locally and in CI.
pub async fn populate(ctx: &GateContext, images: usize) -> GateResult<()> {
    for index in 0..images {
        let namespace = format!("conformance/pop-{index:02}");
        let layer = format!("populate-layer-{index}");
        ctx.registry
            .push_image(&namespace, GATE_TAG, layer.as_bytes())
            .await?;
    }
    println!("populated {images} images");
    Ok(())
}

/// The gate config plus a keep-tagged-only retention policy on the gate
/// repository: prune retains grant-only blobs by design when no policy is
/// configured, so gating the retention-grant sweep needs one to enforce.
/// Written under the state dir; absolute so the docker mode can mount it.
fn write_retention_config(ctx: &GateContext) -> GateResult<String> {
    let mut config = read_to_string(ctx.runner.config_path())?;
    config.push_str(
        "\n[repository.\"conformance\".retention_policy]\nrules = ['image.tag != null']\n",
    );
    let path = ctx.state_path("config-retention.toml");
    write(&path, config)?;
    let absolute = canonicalize(&path)?;
    Ok(absolute.to_string_lossy().into_owned())
}

fn report_keys(keys: &[String]) -> String {
    let mut lines: Vec<&str> = keys
        .iter()
        .take(DIFF_REPORT_LIMIT)
        .map(String::as_str)
        .collect();
    if keys.len() > lines.len() {
        lines.push("...");
    }
    lines.join("\n")
}
