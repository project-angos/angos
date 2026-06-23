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
- Checking and repairing metadata corruption (links, blob index, referrers)
- Verifying storage consistency
- Cleaning up stale uploads

Retention enforcement no longer runs under `scrub`; it now runs under the [`angos policy`](#enforce-policy-angos-policy) command.

## What Scrub Does

The `scrub` command performs various maintenance operations. Each check must be explicitly enabled:

| Flag                          | Description                                                                                        |
|-------------------------------|----------------------------------------------------------------------------------------------------|
| `-t, --tags`                  | Check and fix tag references; remove tags whose target manifest blob is missing; delete tag directories whose names violate the OCI tag grammar |
| `-m, --manifests`             | Recreate missing/mismatched manifest links; **report** (never delete) references to missing config/layer blobs or index children (pull-through/partial pulls legitimately lack them). Reported items also appear in the [run summary](#run-summary) |
| `-b, --blobs`                 | Remove orphaned (unreferenced) blobs; prune stale blob-index entries for deleted namespaces        |
| `-r, --retention`             | Enforce retention policies (delete expired manifests). **Deprecated:** moved to `angos policy --retention`; the `scrub` flag still works as an alias |
| `-u, --uploads <duration>`    | Check upload sessions: remove broken or partial state and uploads older than the given duration    |
| `-p, --multipart <duration>`  | Cleanup orphan S3 multipart uploads older than duration                                            |
| `-l, --links`                 | Fix links format inconsistencies; remove revisions whose manifest blob is missing; prune phantom referrer back-links. Report-only findings also appear in the [run summary](#run-summary) |
| `--reconcile-blob-index`      | Rebuild blob-index entries missing relative to the manifests that reference each blob; repairs an index corrupted out-of-band. Reads every manifest, so it is expensive |
| `--migrate`                   | Migrate the on-disk storage layout (legacy blob-index files → sharded; prune the pre-1.3 namespace-registry index). Scans every blob; run once after upgrade, not on routine scrubs |
| `-M, --media-types`           | **Deprecated:** manifests record their `media_type` at push time, so new content never needs backfill. `-M` backfills legacy links written before that, still runs unchanged, emits a one-time deprecation warning, and will be removed in a future release |
| `-R, --referrers`             | Check for and remove orphan referrer links whose referrer manifest is no longer a current revision |
| `-n, --orphan-namespaces`     | Delete all content for namespaces not owned by any configured repository (destructive; see below)  |
| `--jobs`                      | Structural job reconcile: drop dangling `_jobs/index` lock-key entries whose pending envelope has vanished (and, with `--prune-unknown`, unknown-named queue directories) |
| `--prune-unknown`             | Opt-in **modifier** (off by default, destructive): escalates structurally-invalid objects from report-only to deleted for whichever scans run alongside it. Run with `-d` first. See [Pruning Unknown Objects](#pruning-unknown-objects---prune-unknown) for details |
| `-d, --dry-run`               | Preview changes without applying them                                                              |

---

## Enforce Policy (`angos policy`)

**Retention** enforcement moved out of `scrub` into the dedicated `angos policy` subcommand. It runs against the same configuration and storage. An operation flag (`--retention`) is required; running `angos policy` with no flag does nothing. Once an operation is selected it mutates by default; pass `-d` to preview:

```bash
# Enforce retention policies
angos -c config.toml policy --retention

# Preview retention enforcement
angos -c config.toml policy --retention --dry-run
```

The matching `scrub` flag (`scrub --retention`) keeps working unchanged as a **deprecated alias**: it still runs but emits a one-time deprecation warning pointing at `angos policy`.

---

## Reconcile Replication (`angos replication`)

**Replication reconcile** is a sync operation, not policy enforcement, so it has its own `angos replication` subcommand (it moved out of `scrub`). It walks every replicated namespace and enqueues pushes for diverging or downstream-missing tags (and, for a `prune = true` downstream, deletes downstream-only tags), then drains the enqueued jobs in-process. It mutates by default; pass `-d` to preview. For the full semantics, including the backoff-drain caveat and active-active warnings, see [Configure Replication](configure-replication.md) and the `replication` command in the [CLI Reference](../reference/cli.md).

```bash
# Reconcile every replicated namespace with its downstreams
angos -c config.toml replication

# Preview replication reconciliation (enqueues nothing)
angos -c config.toml replication --dry-run
```

The matching `scrub` flag (`scrub --replicate`) keeps working unchanged as a **deprecated alias**: it still runs but emits a one-time deprecation warning pointing at `angos replication`. See [Configure Replication](configure-replication.md).

The structural and config-drift cleaners (`-t`, `-m`, `-b`, `-l`, `-R`, `-M`, `-u`, `-p`, `-n`, `--reconcile-blob-index`, `--migrate`, `--orphan-grants`, `--replication-orphans`, `--cache-orphans`, `--jobs`, `--prune-unknown`) stay on `scrub`.

---

## Basic Usage

### Preview Mode (Dry Run)

See what would be deleted without making changes:

```bash
./angos -c config.toml scrub --dry-run --tags --manifests --blobs
```

### Run Full Cleanup

Run the structural checks, then enforce retention (retention moved to `angos policy`):

```bash
./angos -c config.toml scrub --tags --manifests --blobs
./angos -c config.toml policy --retention
```

### Selective Cleanup

Run only specific checks:

```bash
# Enforce only retention policies (moved to `angos policy`; `scrub --retention` is a deprecated alias)
./angos -c config.toml policy --retention

# Clean up orphaned blobs only
./angos -c config.toml scrub --blobs

# Remove incomplete uploads older than 1 hour
./angos -c config.toml scrub --uploads 1h

# Cleanup orphan S3 multipart uploads older than 24 hours
./angos -c config.toml scrub --multipart 24h
```

### With Logging

```bash
RUST_LOG=info ./angos -c config.toml scrub --tags --manifests --blobs
```

---

## Scheduling

These examples schedule the structural `scrub` checks. Retention enforcement moved to `angos policy --retention` (the deprecated `scrub --retention` alias still works); schedule it the same way, as the cron example shows.

### Cron (Linux/macOS)

```bash
# Daily at 3 AM: structural maintenance
0 3 * * * /usr/bin/angos -c /etc/registry/config.toml scrub --tags --manifests --blobs >> /var/log/registry-scrub.log 2>&1

# Daily at 3:30 AM: enforce retention
30 3 * * * /usr/bin/angos -c /etc/registry/config.toml policy --retention >> /var/log/registry-policy.log 2>&1

# Weekly on Sunday at 2 AM
0 2 * * 0 /usr/bin/angos -c /etc/registry/config.toml scrub --tags --manifests --blobs
```

### Recommended Flags for Periodic Maintenance

Always include `-b` (`--blobs`) in scheduled scrub jobs. The blob index records
which namespaces reference each blob; its per-namespace shard files
(`refs/<namespace>.json`) are only pruned when `-b` runs. Without it, shards
for deleted namespaces accumulate indefinitely.

Blob ownership markers are kept until the client issues an explicit
`DELETE /v2/<name>/blobs/<digest>` request; scrub does not remove them when
a namespace's manifests are deleted. This reflects the OCI blob lifecycle and
is not a leak.

Routine upload session cleanup is gated behind `-u <duration>`; without that
flag, neither obsolete nor broken upload state is touched (note that `-n`
separately clears every in-flight upload of an orphan namespace regardless of
age). Pass `-u` in schedules that should reclaim that storage.

The `_catalog` listing is derived directly from stored content: a namespace
appears exactly when it holds at least one revision or tag, and disappears as
soon as the last one is deleted. No scrub run or namespace registration step is
involved.

### Systemd Timer

Create `/etc/systemd/system/registry-scrub.service`:

```ini
[Unit]
Description=Registry Storage Maintenance

[Service]
Type=oneshot
ExecStart=/usr/bin/angos -c /etc/registry/config.toml scrub --tags --manifests --blobs
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

Enable:

```bash
systemctl enable --now registry-scrub.timer
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
              args: ["-c", "/config/config.toml", "scrub", "--tags", "--manifests", "--blobs"]
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
    command: ["-c", "/config/config.toml", "scrub", "--tags", "--manifests", "--blobs"]
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

| Item              | Condition                        |
|-------------------|----------------------------------|
| Tagged manifest   | Doesn't match any retention rule |
| Untagged manifest | Doesn't match any retention rule |
| Blob              | Not referenced by any manifest   |
| Upload            | With `-u`: broken/incomplete session or older than the timeout    |
| Whole namespace   | With `-n`: not owned by any configured `[repository]` (revisions, tags, in-flight uploads, plus the namespace's layer/config blob bytes) |

### Clearing Orphan Namespaces (`-n`)

`-n, --orphan-namespaces` is destructive and opt-in. When a `[repository]` is removed from configuration (or data predates it), the namespaces it held no longer resolve to any repository, yet their manifests, tags, and blobs linger forever. This flag removes the revisions, tags, and in-flight uploads of every such namespace and reclaims their layer/config blob bytes by revoking those blobs' ownership grants when no still-configured namespace shares them, so the blast radius is **every namespace whose owning repository is no longer in your config**, not just empty ones. It does not reclaim the manifest blob bytes (manifest blobs carry no ownership grant); a `--blobs` pass reclaims those once their links are gone.

Safeguards apply:

- **Always dry-run first.** Run `angos scrub --orphan-namespaces --dry-run` and confirm the listed deletions only cover namespaces you intend to drop.
- **Combine with `--blobs` to reclaim manifest bytes.** Run `angos scrub --orphan-namespaces --blobs` to also reclaim the orphan manifest blob bytes in the same pass, once their links are gone.
- **Run with no writers on orphan namespaces.** A client can still push to a namespace that no longer maps to any repository. Run `-n` when no writers target orphan namespaces, or re-run it afterward to mop up anything pushed during the clear.
- **Empty-config guard.** If no `[repository]` is configured, every namespace would be an orphan, so the flag refuses to run, logs a warning, and deletes nothing; an emptied config can never wipe the registry.

Cleared namespaces drop out of `_catalog` automatically, since the catalog is derived from stored content.

#### Pruning Unknown Objects (`--prune-unknown`)

`--prune-unknown` is an opt-in modifier, not a scan on its own; passed alone it does nothing. It is off by default, so no existing flag ever deletes more than before. When set, it escalates structurally-invalid objects from report-only to deleted for whichever scans run in the same invocation: invalid-named namespaces are deleted only alongside a metadata-scoped scan (such as `-m`), and unknown-named job-queue directories only alongside `--jobs`. Without it, those objects stay report-only and surface in the [run summary](#run-summary). Run with `-d` first to confirm the listed deletions.

### Protected Items

Never deleted by retention:
- Child manifests of multi-platform indexes
- Manifests with referrers (signatures, SBOMs)

This scope is retention only. `-n` clears the entire orphan namespace, including index children and referrer/signature manifests.

---

## Run Summary

Every run of `scrub`, [`policy`](#enforce-policy-angos-policy), and [`replication`](#reconcile-replication-angos-replication) ends with a structured summary block:

- **Per-category action counts**: orphan blobs, orphan manifests, tags, referrers, repaired links, blob-index ops, backfilled media types, revoked grants, expired uploads, multipart aborts, jobs (orphan-job deletions from `--replication-orphans` / `--cache-orphans`), migrations, pruned namespaces, and enqueued replications. These counts are successful mutations only. Zero-count categories are suppressed, so a clean run is a single `total: 0` line. The structural `--jobs` reconcile is not in these counts (it repairs the job store directly, outside the action path); it logs its own retired/found totals on a separate line.
- **Failed applies**: a `failed: N (left in place, see error log)` line appears when some applies returned an error (for example a transient S3 failure). Those faults were not resolved, so the line keeps the per-category totals from being mistaken for "all faults resolved"; the individual failures are logged as separate `error` lines. The line is omitted when nothing failed.
- **Report-only findings**: observations that were counted but never acted on: dangling config/layer/index-child references (from `-m`), and invalid-named namespaces skipped because `--prune-unknown` was off (the count points you at `--prune-unknown` to delete them). A bounded sample of the offending items is included.

A **dry-run** (`-d`) reports `would` counts; a **mutate** run reports `done` counts (the same categories in both modes), so you can diff a preview against the eventual run.

The summary is a log line (visible at `RUST_LOG=info`), not stdout, so it is captured by the same log stream your cron/systemd/Kubernetes job already collects:

```bash
RUST_LOG=info ./angos -c config.toml scrub --tags --manifests --blobs
# ... per-check logs ...
# SCRUB SUMMARY (mode: mutate)
#   Actions (done):
#     orphan blobs: 12
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

- Check retention policies match expected behavior
- Verify manifests aren't protected
- Use dry-run with debug logging:
  ```bash
  RUST_LOG=debug ./angos -c config.toml scrub --dry-run --tags --manifests --blobs
  ```

### Storage Not Reduced

- Blobs may be shared across manifests
- Run scrub again after manifest deletion
- Check for incomplete uploads

### Lock Errors

For multi-replica deployments:
- Ensure Redis is configured for locking
- Run only one storage-maintenance command (`scrub` / `policy` / `replication`) at a time across the fleet; they share the same lock domain

### S3 Errors

- Verify S3 credentials have delete permissions
- Check network connectivity
- Review S3 operation timeout settings

### Orphan S3 Multipart Uploads

S3 multipart uploads that were started but never completed can accumulate and consume storage. The `--multipart` flag cleans up orphan multipart uploads that:
- Are older than the specified duration
- Have no corresponding upload container in the registry metadata

This is S3-specific and has no effect on filesystem storage backends.

### Backfill Media Types

Media-type backfill reads each manifest blob and writes its `media_type` into the link metadata. This enables optimizations that avoid full blob reads on HEAD requests and redirect paths. New content records its `media_type` at push time, so backfill (`-M`) is a one-time post-upgrade step for legacy links written before push-time recording landed:

```bash
# Preview what would be backfilled
./angos -c config.toml scrub --media-types --dry-run

# Backfill media types on legacy manifest links
./angos -c config.toml scrub --media-types
```

The `--media-types` (`-M`) flag is **deprecated**: it backfills legacy links only and will be removed in a future release once legacy backfill is no longer needed. It still runs unchanged and emits a one-time deprecation warning.

Setting a `media_type` is idempotent and safe to re-run. The backfill also removes a revision or tag link whose manifest blob is already missing.

`-M` reads every manifest blob (a full content scan), so on a large registry size the maintenance window accordingly. Even once backfill is complete, each `-M` run still performs a metadata read per revision and per tag to check whether `media_type` is already set; on an S3-backed metadata store these are network round-trips.

### Blob Index Migration

Legacy single-file blob indexes (`index.json`) keep working at runtime against both the S3 and filesystem backends. Reads consult the sharded layout first and fall back to the legacy file when no sharded entry exists, and writes are applied in place to a legacy file when one is present so the layout never splits mid-blob. Operators are **not** forced to run scrub just to keep serving traffic.

`scrub --migrate` is the only way to convert the on-disk blob-index layout from legacy to sharded:

- `scrub --migrate` iterates every blob, rewrites each legacy `index.json` into per-namespace shards under `refs/{namespace}.json`, deletes the legacy file once the shards are written, and prunes the pre-1.3 `_registry/` namespace-registry index.

Because it scans every blob, `--migrate` is **not** part of routine maintenance: run it once after an upgrade that introduces the sharded layout, then drop it from scheduled jobs. Running it benefits operators who want the per-namespace shard layout's lock granularity and concurrency characteristics for old blobs; the runtime fallback is correct but does not gain those properties for entries that are still in legacy form. Operators with no legacy data on disk can skip this step.

```bash
# Migrate legacy blob indexes
./angos -c config.toml scrub --migrate
```

This migration is **idempotent**: re-running it is safe and will simply skip data that is already in the sharded layout.

### Fix Links Format

The `--links` flag repairs link format inconsistencies. Use this when:
- Upgrading from an older version that didn't track references
- Repairing corrupted metadata after a storage issue
- Migrating data from another registry

```bash
# Preview what would be fixed
./angos -c config.toml scrub --links --dry-run

# Fix links format for all links
./angos -c config.toml scrub --links
```

This is idempotent and safe to run multiple times.

---

## Best Practices

1. **Always dry-run first** in production
2. **Run during low-traffic periods** to minimize impact
3. **Monitor storage trends** after scheduled runs
4. **Keep retention policies conservative** initially
5. **Use Redis locking** for multi-replica deployments

## Reference

- [Configure Retention Policies](configure-retention-policies.md) - Policy syntax
- [CLI Reference](../reference/cli.md) - scrub command options
