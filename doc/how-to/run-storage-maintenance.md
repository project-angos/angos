---
displayed_sidebar: howto
sidebar_position: 10
title: "Storage Maintenance"
---

# Run Storage Maintenance

Verify storage integrity with the `scrub` command and enforce retention policies with the `angos policy` command.

## Prerequisites

- Angos installed
- Access to the same configuration and storage as the running registry

## Automatic vs Scheduled Maintenance

Angos performs garbage collection **automatically** during normal operation: unreferenced blobs are cleaned up as part of request handling, without downtime.

The `scrub` command runs as a **separate periodic process** that operates alongside the live server (no shutdown required):
- Rebuilding each revision's derived metadata (links, blob-index grants, referrer back-links, `media_type`) from its current manifest
- Sweeping raw storage to classify every key and reclaim what the rebuild did not account for
- Cleaning up stale uploads and config-drift job queues

Retention enforcement runs under the [`angos policy`](#enforce-policy-angos-policy) command, not under `scrub`.

## What Scrub Does

`scrub` is a **rebuild-and-sweep** operation. It runs the same two passes on every invocation, with no per-checker flags to select:

1. **Rebuild.** For every revision in every namespace, scrub re-derives the derived metadata (digest and tag links, blob-index ownership grants, referrer back-links, and `media_type`) from that revision's current manifest, using the same planner a push uses. This is additive repair: it only writes what is missing or wrong, it runs unconditionally, and its repair writes land only under `--commit` (report-only reports the divergence instead).
2. **Sweep.** A raw enumeration lists every storage key across the owned roots (`v2/blobs`, `v2/repositories`, `_jobs`), classifies each one, and reclaims what the rebuild did not account for: orphan blob bytes, orphan and stale blob-ownership grants, orphan derived links, dangling missing-body revisions, de-configured namespaces, and unrecognized keys.

### Report-Only by Default

**A report-only run performs no storage mutation at all except the best-effort `_scrub-audit/latest.json` run marker.** With no destructive flag scrub classifies everything and every category (deletions, the rebuild repair, the legacy blob-index convergence, the `_registry/` prune) only reports what it *would* do. `--commit` is the global gate for every write (the deprecated `--retention`/`--replicate` aliases gate only their own policy and replication actions, exactly like the dedicated subcommands).

### The Maintenance Grace

Every garbage-collection reap is additionally **age-gated**: a candidate is deleted only once its backend `last_modified` is older than the maintenance grace (`[global] maintenance_grace_secs`, default 48 hours), so in-flight pushes and freshly written state are never swept mid-operation. An object whose `last_modified` the backend does not report is never reaped. Two categories keep their own explicit gates instead: de-configured namespace deletion is config-driven (not age-gated), and the upload cleaners take their age from the `-u`/`-p` flag durations.

Run report-only first, review the classification in the log, then re-run with `--commit`. See [Basic Usage](#basic-usage).

### Flags

| Flag                          | Description                                                                                        |
|-------------------------------|----------------------------------------------------------------------------------------------------|
| `--commit`                    | Global deletion gate: enable destructive actions across every sweep category. Off by default (report-only). Mutually exclusive with `--dry-run`. A mutating configuration is refused when the metadata store's `lock_strategy` is `memory` (no cross-process exclusion) |
| `--delete-unrecognized`       | Also delete unrecognized keys, gated on `--commit`. Without `--commit` the pass classifies and tallies would-delete counts; with `--commit --delete-unrecognized` each unrecognized key older than the maintenance grace is deleted |
| `-u, --uploads <duration>`    | Check upload sessions: remove broken or partial state and uploads older than the given duration    |
| `-p, --multipart <duration>`  | Abort orphan S3 multipart uploads older than the duration (S3 only)                                |
| `--replication-orphans`       | Delete replication jobs (pending and dead-lettered) whose downstream or repository is no longer configured |
| `--cache-orphans`             | Delete cache jobs (pending and dead-lettered) whose repository is no longer configured for pull-through |
| `--jobs`                      | Structural job reconcile: drop dangling `_jobs/index` lock-key entries whose pending envelope has vanished and rebuild the missing or stale dedup entry of a present pending job (a claimed, in-flight job is skipped); with `--prune-unknown`, also remove unknown-named queue directories quiescent for the maintenance grace |
| `--prune-unknown`             | Opt-in **modifier** (off by default): escalate structurally-invalid objects (invalid-named namespaces, unknown job-queue directories) from report-only to deleted. Run report-only first |
| `--reclaim-engine`            | Run one on-demand pass of the transaction engine's pure-delete janitors (orphan `.tx-bodies/` staging older than 1h, expired `.tx-locks/`). Honours report-only/`--commit` |

### Deprecated and Removed Flags

Three flags are deprecated: each still works, emits a one-time deprecation warning, and will be removed in a future release.

| Flag | Status | Replacement |
|----------|--------|-------------|
| `-d, --dry-run`           | deprecated (mutates no registry state) | the report-only default (omit flags); `--commit` to delete |
| `-r, --retention`         | deprecated (still works; mutates retention unless `-d`) | `angos policy --retention` |
| `--replicate`             | deprecated (still works; mutates replication unless `-d`) | `angos replication` |

`-d, --dry-run` writes nothing to registry state; its only write is the best-effort `_scrub-audit/latest.json` run marker. The report-only default (omit all flags) is equally write-free, so the alias survives only for compatibility. `--dry-run` is mutually exclusive with `--commit`.

The 1.3.0 per-checker flags (`-m`/`--manifests`, `-l`/`--links`, `-M`/`--media-types`, `-t`/`--tags`, `-R`/`--referrers`, `-b`/`--blobs`, `-n`/`--orphan-namespaces`, `--orphan-grants`, `--reconcile-blob-index`) are **removed** and fail argument parsing: their work runs unconditionally in the rebuild and sweep. See the [upgrade guide](upgrade.md) for the per-flag mapping.

---

## Enforce Policy (`angos policy`)

**Retention** enforcement is the dedicated `angos policy` subcommand's job. It runs against the same configuration and storage. An operation flag (`--retention`) is required; running `angos policy` with no flag does nothing. Once an operation is selected it mutates by default; pass `-d` to preview. A mutating run holds the shared maintenance lock and is refused on the `memory` lock strategy; a dry run takes no lock:

```bash
# Enforce retention policies
angos -c config.toml policy --retention

# Preview retention enforcement
angos -c config.toml policy --retention --dry-run
```

The matching `scrub` flag (`scrub --retention`) keeps working unchanged as a **deprecated alias**: it still runs but emits a one-time deprecation warning pointing at `angos policy`.

---

## Reconcile Replication (`angos replication`)

**Replication reconcile** is a sync operation, not policy enforcement, so it has its own `angos replication` subcommand. It walks every replicated namespace and enqueues pushes for diverging or downstream-missing tags (and, for a `prune = true` downstream, deletes downstream-only tags), then drains the enqueued jobs in-process. It mutates by default; pass `-d` to preview. A mutating run holds the shared maintenance lock and is refused on the `memory` lock strategy; a dry run takes no lock. For the full semantics, including the backoff-drain caveat and active-active warnings, see [Configure Replication](configure-replication.md) and the `replication` command in the [CLI Reference](../reference/cli.md).

```bash
# Reconcile every replicated namespace with its downstreams
angos -c config.toml replication

# Preview replication reconciliation (enqueues nothing)
angos -c config.toml replication --dry-run
```

The matching `scrub` flag (`scrub --replicate`) keeps working unchanged as a **deprecated alias**: it still runs but emits a one-time deprecation warning pointing at `angos replication`. See [Configure Replication](configure-replication.md).

The rebuild-and-sweep, the upload cleaners (`-u`, `-p`), the config-drift job cleaners (`--replication-orphans`, `--cache-orphans`, `--jobs`, `--prune-unknown`), the unrecognized opt-in (`--delete-unrecognized`), the engine reclaim (`--reclaim-engine`), and the deletion gate (`--commit`) all belong to `scrub`.

---

## Basic Usage

### Report-Only First

Classify everything without deleting anything. This is the default: with no destructive flag scrub runs the additive rebuild repair and reports what each deletion category *would* reclaim.

```bash
./angos -c config.toml scrub
```

Review the classification in the log (see [Run Summary](#run-summary)), especially the would-reap, would-delete, would-revoke, and unrecognized lines, before enabling deletion.

### Then Commit

Once the report-only run looks right, re-run with `--commit` to enable deletion across every sweep category. Each reap requires the candidate to be older than the maintenance grace, and each byte-adjacent reap re-checks its precondition under the per-blob lock via a strongly-consistent point read at delete time:

```bash
./angos -c config.toml scrub --commit
```

Enforce retention separately with `angos policy`:

```bash
./angos -c config.toml policy --retention
```

### Selective Cleanup

`scrub` has no per-checker flags; the rebuild and sweep always run together. What you *can* scope is the upload cleaners and the destructive gate:

```bash
# Enforce only retention policies (`scrub --retention` is a deprecated alias)
./angos -c config.toml policy --retention

# Reap incomplete uploads older than 1 hour
./angos -c config.toml scrub --commit --uploads 1h

# Abort orphan S3 multipart uploads older than 24 hours
./angos -c config.toml scrub --commit --multipart 24h

# Also delete unrecognized keys older than the maintenance grace (needs --commit)
./angos -c config.toml scrub --commit --delete-unrecognized

# Reclaim engine-owned staging/lock keys in the same run
./angos -c config.toml scrub --commit --reclaim-engine
```

### With Logging

```bash
RUST_LOG=info ./angos -c config.toml scrub
```

---

## Scheduling

Schedule `scrub` as a periodic job with a systemd timer (below) or a Kubernetes CronJob. Validate the schedule with a report-only run first, then commit on the recurring job. Schedule retention enforcement (`angos policy --retention`) the same way, with a sibling timer. Mind the [exit codes](#exit-codes): systemd and Kubernetes treat a degraded (2) or aborted (3) run as a failure.

### Recommended Flags for Periodic Maintenance

A committed `scrub --commit` run already reconciles the blob index: the rebuild re-derives every revision's ownership grants, and the sweep removes stale grant entries of every kind (an entry whose backing link file is gone, tag/digest/referrer entries included) and reaps orphan blob bytes. Every reap requires the candidate to be older than the maintenance grace and re-checks its precondition under the blob-data lock via a strongly-consistent point read. The per-namespace grant shard files (`refs/<namespace>.json`) are pruned in the same sweep.

A grant a live manifest still references is never revoked. A bare self-grant (a blob a namespace owns but no manifest references) is kept for at least the maintenance grace, covering the normal push flow where the blob upload precedes its manifest; once older than the grace with still no link referencing the digest, `--commit` revokes it and reclaims the bytes when it was the last reference. The revocation first re-probes, under the blob-data lock, both the link kinds that could reference the digest and the grant's own freshness, so a blob a concurrent upload just re-confirmed (which rewrites the grant without creating a link) is kept. An explicit `DELETE /v2/<name>/blobs/<digest>` reclaims immediately, without waiting for a scrub.

Upload session cleanup is gated behind `-u <duration>`; without it, no obsolete or broken upload state is touched (the config-ownership sweep still clears every in-flight upload of a de-configured namespace once committed, regardless of age). Pass `-u` in schedules that should reclaim that storage.

The `_catalog` listing is derived directly from stored content: a namespace appears when it holds at least one revision or tag and disappears when the last one is deleted. No scrub run or registration step is involved.

### Systemd Timer

Create `/etc/systemd/system/registry-scrub.service`:

```ini
[Unit]
Description=Registry Storage Maintenance

[Service]
Type=oneshot
ExecStart=/usr/bin/angos -c /etc/registry/config.toml scrub --commit
Environment=RUST_LOG=info
```

Create `/etc/systemd/system/registry-scrub.timer`:

```ini
[Unit]
Description=Daily registry storage maintenance

[Timer]
OnCalendar=*-*-* 03:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enforce retention with a sibling `registry-policy.service` (`ExecStart=/usr/bin/angos -c /etc/registry/config.toml policy --retention`) and `registry-policy.timer` (`OnCalendar=*-*-* 03:30:00`). Enable both:

```bash
systemctl enable --now registry-scrub.timer registry-policy.timer
systemctl list-timers 'registry-*'   # confirm the schedule
journalctl -u registry-scrub         # review a run
```

### Kubernetes CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: registry-scrub
  namespace: registry
spec:
  schedule: "0 3 * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: scrub
              image: ghcr.io/project-angos/angos:latest
              args: ["-c", "/config/config.toml", "scrub", "--commit"]
              env:
                - name: RUST_LOG
                  value: info
              volumeMounts:
                - name: config
                  mountPath: /config
                  readOnly: true
          volumes:
            - name: config
              secret:
                secretName: registry-config
          restartPolicy: OnFailure
```

### Docker Compose

```yaml
services:
  scrub:
    image: ghcr.io/project-angos/angos:latest
    command: ["-c", "/config/config.toml", "scrub", "--commit"]
    volumes:
      - ./config:/config:ro
      - ./data:/data
    profiles:
      - maintenance
```

Run manually:

```bash
docker compose --profile maintenance run --rm scrub
```

---

## Retention Policy Configuration

Define what to keep in `config.toml`:

```toml
[global]
update_pull_time = true  # Track pull times

[global.retention_policy]
rules = [
  'image.tag == "latest"',
  'image.pushed_at > now() - days(30)'
]
```

See [Configure Retention Policies](configure-retention-policies.md) for detailed options.

---

## What Gets Deleted

All of the sweep deletions below happen only under `--commit` (retention is a separate `angos policy` operation). Except where noted, a candidate is deleted only once its backend `last_modified` is older than the maintenance grace, and an object without a `last_modified` is never reaped.

| Item              | Condition                        |
|-------------------|----------------------------------|
| Tagged manifest   | Retention only (`angos policy`): doesn't match any retention rule |
| Untagged manifest | Retention only (`angos policy`): doesn't match any retention rule |
| Orphan blob bytes | No namespace references the blob, once past the grace (re-checked under the blob-data lock); hard-deleted (re-pushable) |
| Stale grant entry | A blob-ownership grant entry of any kind (layer/config/manifest, or tag/digest/referrer) whose backing link file is gone, once past the grace (re-checked under the blob-data lock) |
| Bare self-grant   | A blob-ownership grant with no link file referencing the digest, once past the grace (re-checked under the blob-data lock); revoking the last reference reclaims the bytes. An explicit blob DELETE reclaims immediately |
| Orphan link       | A subject referrer or phantom `referenced_by` entry whose referring revision is gone, once past the grace (re-checked under the blob-data lock) |
| Missing-body revision | A revision or tag whose target manifest body is absent from the blob store, once past the grace (re-checked under the blob-data lock) |
| Upload            | With `-u`: broken/incomplete session or older than the flag's timeout (its own age gate, not the grace) |
| Whole namespace   | Not owned by any configured `[repository]` (with a non-empty resolver): revisions, tags, in-flight uploads deleted and blob-ownership grants revoked (see below). Config-driven, not age-gated |
| Unrecognized key      | With `--commit --delete-unrecognized`: an unrecognized key older than the grace is deleted |

### Deletes Are Final

A committed delete removes the key outright; there is no trash or recovery prefix. The report-only default computes the same candidate sets as a committed run, so preview it first and diff the counts. Orphan blob bytes and grant revokes additionally re-check reachability under the blob-data lock at delete time, so a blob a concurrent push just referenced is kept; deleted blob bytes are recoverable only by re-push.

### Clearing De-configured Namespaces

When a `[repository]` is removed from configuration (or data predates it), the namespaces it held no longer resolve to any repository, yet their manifests, tags, and blobs would linger forever. The config-ownership sweep handles this automatically. Under `--commit` it deletes the revisions, tags, and in-flight uploads of every such namespace (including in-flight uploads on the blob backend, so a split blob/metadata deployment is fully covered) and revokes their blob-ownership grants so the freed layer/config blob bytes become reap-eligible by the orphan-blob pass. The blast radius is **every namespace whose owning repository is no longer in your config**, not just empty ones. It is scoped to the namespace's own data: a nested namespace that still resolves to a configured repository (say `[repository."team/app1"]` under a de-configured `team`) is never touched, since child namespaces live as sibling directories inside the parent's.

Safeguards apply:

- **Report-only first.** Run `angos scrub` with no `--commit` and confirm the reported de-configured namespaces only cover namespaces you intend to drop.
- **Config-driven, no age gate.** De-config is a config decision, not a reachability question, so a namespace is deleted the moment it resolves to no configured repository. The deletion is final; the freed blob bytes are hard-deleted under the blob-data lock, which re-checks references and keeps a blob a concurrent push just re-referenced.
- **Run with no writers on orphan namespaces.** A client can still push to a namespace that no longer maps to any repository. Run when no writers target orphan namespaces, or re-run afterward to mop up anything pushed during the clear.
- **Empty-config guard.** If no `[repository]` is configured, every namespace would be de-configured, so the sweep refuses this pass, logs a warning, and deletes nothing. An emptied or mis-loaded config can never wipe the registry.

Cleared namespaces drop out of `_catalog` automatically, since the catalog is derived from stored content.

#### Pruning Unknown Objects (`--prune-unknown`)

`--prune-unknown` is an opt-in modifier, not a scan on its own; passed alone it does nothing. It is off by default, so no scan ever deletes more than before. When set, it escalates structurally-invalid objects from report-only to deleted: invalid-named namespaces (deleted in the metadata rebuild's walk) and unknown-named job-queue directories (alongside `--jobs`). An unknown queue directory is deleted only once quiescent for the maintenance grace, and its quiescence is re-probed immediately before the delete, so a directory a newer replica just started writing is kept. Without `--prune-unknown`, those objects stay report-only and surface in the [run summary](#run-summary). Run report-only first to confirm the listed deletions.

### Protected Items

Never deleted by retention:
- Child manifests of multi-platform indexes
- Manifests with referrers (signatures, SBOMs)

This scope is retention only. The config-ownership sweep clears the whole de-configured namespace's own data, including index children and referrer/signature manifests (nested configured child namespaces excepted).

---

## Safety and Observability

Every run leaves a storage-side record of its outcome, so a silent GC stall or an unexpected reap is diagnosable after the fact.

### Liveness Marker and Logs

- **Liveness marker.** At run end scrub overwrites `_scrub-audit/latest.json` with the run's timestamps, terminal status, exit code, mode, and tallies. The `mode` field (`commit`, `report-only`, or `dry-run`) tells a watcher whether the tallies are performed deletions or would-counts: a recurring job that lost its `--commit` shows `"mode": "report-only"` with ever-present would-counts, not a clean committed run. An out-of-band watcher reads this one object to learn the last run's outcome; an absent or stale marker (`finished_at` older than the timer interval) is the silent-stall signal. The marker lives on the metadata backend; a split blob/metadata deployment lands it on the metadata bucket.
- **Logs.** The log lines are the record of individual deletions: every committed delete, revoke, and reap is logged with its key or digest, and the run ends with the [summary](#run-summary).

### The Maintenance Lock

Each `scrub` run, and each mutating `policy`/`replication` run, holds the single-instance maintenance lock (`maintenance:registry`); a concurrent maintenance command refuses to start. One run may hold the lock for at most `[global] maintenance_lock_max_hold_secs` (default 24 hours); when the max hold fires or ownership is lost mid-run, the run aborts all further deletes (exit 3) and re-runs from scratch next time. Size the knob above your worst-case scrub duration. A mutating run is refused up front when the metadata store's `lock_strategy` is `memory`, since an in-process lock gives no cross-process exclusion; switch to `s3` or `redis`, or run report-only.

### Concurrent Pushes

Manifest pushes hold the per-blob lock for each newly referenced blob, so storage maintenance can never reclaim bytes a concurrent push references. A push racing `scrub --commit` can lose the race the other way: if the sweep reclaims a blob between the client's blob HEAD and its manifest PUT, then with `allow_missing_manifest_references = false` the push receives `MANIFEST_BLOB_UNKNOWN` and the client retry succeeds by re-uploading the blob; the permissive default instead accepts the manifest with the reclaimed reference unowned until it is re-pushed.

### Rebuild-Fatal: Keep Over Reap

The rebuild is result-gated. A namespace whose manifest body fails to parse (or is a newer on-disk format an older scrub cannot read) is marked **rebuild-fatal**: its remaining per-namespace checkers (the deprecated retention alias included) and its destructive sweep passes are skipped (keep over reap), and any fatal namespace also globally skips the digest-keyed byte reap and the stale-grant and self-grant revokes for the whole run (a co-owned blob cannot be gated per-namespace). The config-driven de-configured cleanup still runs, but the bytes its revocations unpin are not reaped until a later clean run. Report-only classification of everything else still proceeds. The fatal namespaces are named in the log and recorded in the marker.

### Exit Codes

| Code | Status   | Meaning |
|------|----------|---------|
| `0`  | clean    | Ran to completion, nothing kept-degraded |
| `1`  | refused  | Refused at entry (lock held, or a mutating configuration on a `memory` lock strategy) |
| `2`  | degraded | Completed with degradation: a rebuild-fatal namespace, an incomplete walk, a failed delete or action, a partial de-configured delete or grant revoke, or a convergence failure |
| `3`  | aborted  | Aborted mid-run (lock lost or max hold elapsed) |

Schedulers such as systemd and Kubernetes treat `2` and `3` as failed runs, so a degraded-but-completed scrub shows up as a job failure; alert on the marker or logs to distinguish the two.

### Engine Reclaim (`--reclaim-engine`)

`--reclaim-engine` runs one pass of the transaction engine's pure-delete janitors: orphan `.tx-bodies/` staging directories older than 1h, and expired `.tx-locks/`. The 1h body age is a safety guard, because a live push stages bodies before writing its intent, so a younger staging directory may belong to an in-flight transaction and is left alone. It honours report-only/`--commit` (lists without deleting unless `--commit`) and is safe alongside a live serving registry. The non-pure-delete recovery replay is intentionally skipped, with a logged reason.

---

## Run Summary

Every run of `scrub`, [`policy`](#enforce-policy-angos-policy), and [`replication`](#reconcile-replication-angos-replication) ends with a structured summary block:

- **Per-category action counts**: orphan manifests, tags, expired uploads, multipart aborts, jobs (orphan-job deletions from `--replication-orphans` / `--cache-orphans`), namespaces pruned, and replication enqueued. These counts are successful mutations only. Zero-count categories are suppressed, so a clean run is a single `total: 0` line. The structural `--jobs` reconcile is not in these counts (it repairs the job store directly, outside the action path); it logs its own retired/re-indexed/found totals on a separate line.
- **Failed applies**: a `failed: N (left in place, see error log)` line appears when some applies returned an error (for example a transient S3 failure). The individual failures are logged as separate `error` lines. The line is omitted when nothing failed.
- **Report-only findings**: observations counted but never acted on: dangling config/layer/child-manifest references, and invalid namespaces skipped because `--prune-unknown` was off. A bounded sample of the offending items is included.

A **report-only** run (the default, or `--dry-run`) reports `would` counts; a **`--commit`** run reports `done` counts (the same categories in both modes), so you can diff a preview against the eventual run. The raw sweep additionally logs per-category `would-reap` / `would-delete` / `would-revoke` lines and a bounded unrecognized sample, so the report-only run tells you exactly what `--commit` would remove.

The summary is a log line (visible at `RUST_LOG=info`), not stdout, so it is captured by the same log stream your systemd/Kubernetes job already collects:

```bash
RUST_LOG=info ./angos -c config.toml scrub
# ... rebuild + sweep logs ...
# SCRUB SUMMARY (mode: report-only)
#   Actions (would):
#     orphan manifests: 12
#     tags: 3
#     total: 15
#   Report-only findings (no action taken):
#     dangling layer blobs: 1
```

---

## Monitoring

Check storage before and after:

```bash
# Filesystem
du -sh /data/registry

# S3
aws s3 ls s3://my-bucket --summarize --recursive
```

Count manifests:

```bash
curl http://localhost:8000/_ext/_repositories | jq
```

---

## Troubleshooting

### Nothing Deleted

- **Did you pass `--commit`?** Scrub is report-only by default and deletes nothing without it.
- A candidate is kept when it is younger than the maintenance grace (`maintenance_grace_secs`, default 48h), when the backend reports no `last_modified` for it (nothing is reaped without an age), when its under-lock point-read re-check finds the reference still live (a concurrent push re-referenced it), or when its namespace is rebuild-fatal (keep over reap). Unrecognized-key deletion additionally needs `--commit --delete-unrecognized`.
- For retention: check retention policies match expected behavior and verify manifests aren't protected (that runs under `angos policy`).
- Use report-only with debug logging to see the full classification:
  ```bash
  RUST_LOG=debug ./angos -c config.toml scrub
  ```

### Storage Not Reduced

- Blobs may be shared across manifests
- Run scrub again after manifest deletion
- Check for incomplete uploads

### Lock Errors

For multi-replica deployments:
- Ensure a shared lock strategy (`s3` or `redis`) is configured; `memory` refuses any mutating maintenance run
- Run one storage-maintenance command at a time across the fleet: every `scrub` run and every mutating `policy`/`replication` run takes the same `maintenance:registry` lock, so a concurrent run refuses with exit 1

### S3 Errors

- Verify S3 credentials have delete permissions
- Check network connectivity
- Review S3 operation timeout settings

### Orphan S3 Multipart Uploads

S3 multipart uploads that were started but never completed can accumulate and consume storage. The `--multipart` flag cleans up orphan multipart uploads that:
- Are older than the specified duration
- Have no corresponding upload container in the registry metadata

This is S3-specific and has no effect on filesystem storage backends.

### Media Types, Links, and Grants Are Rebuilt Automatically

There is no separate media-type backfill, link-repair, or blob-index reconcile step. The unconditional per-revision rebuild re-derives every revision's `media_type`, digest and tag links, referrer back-links, and blob-index ownership grants from its current manifest on every scrub, so a `--commit` run repairs all of them (report-only reports the divergences). The rebuild is additive and idempotent, safe to run repeatedly. The 1.3.0 `-M`, `-l`, `-m`, and `--reconcile-blob-index` flags are removed; see [Deprecated and Removed Flags](#deprecated-and-removed-flags).

### Blob Index Migration

Legacy single-file blob indexes (`index.json`) keep working at runtime on both the S3 and filesystem backends: reads consult the sharded layout first and fall back to the legacy file, and writes apply in place to a legacy file when one is present so the layout never splits mid-blob. Operators are not forced to run scrub just to keep serving traffic.

Convergence to the sharded `refs/{namespace}.json` layout runs automatically on every committed scrub: under `--commit` scrub drains any per-blob `index.json` into shards and prunes the dead pre-1.3 `_registry/` index (report-only counts the legacy files without touching them). Convergence is additive (it only rewrites grants, never deletes blob bytes) and idempotent.

The sweep still consults the legacy read fallback for any un-converged blob, so run `scrub --commit` across every replica after upgrading to converge the whole registry. See [Upgrade Angos](upgrade.md).

---

## Best Practices

1. **Always run report-only first** (omit `--commit`) in production
2. **Run during low-traffic periods** to minimize impact
3. **Monitor storage trends** after scheduled runs
4. **Keep retention policies conservative** initially
5. **Use Redis locking** for multi-replica deployments

## Reference

- [Configure Retention Policies](configure-retention-policies.md) - Policy syntax
- [CLI Reference](../reference/cli.md) - scrub command options
