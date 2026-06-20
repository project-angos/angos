---
displayed_sidebar: howto
sidebar_position: 10
title: "Storage Maintenance"
---

# Run Storage Maintenance

Verify storage integrity and enforce retention policies using the `scrub` command.

## Prerequisites

- Angos installed
- Access to the same configuration and storage as the running registry

## Automatic vs Scheduled Maintenance

Angos performs garbage collection **automatically** during normal operation: unreferenced blobs are cleaned up as part of request handling, without downtime.

The `scrub` command runs as a **separate periodic process** that operates alongside the live server (no shutdown required):
- Checking and repairing data corruption
- Verifying storage consistency
- Enforcing retention policies
- Cleaning up stale uploads

## What Scrub Does

The `scrub` command performs various maintenance operations. Each check must be explicitly enabled:

| Flag                          | Description                                                                                        |
|-------------------------------|----------------------------------------------------------------------------------------------------|
| `-t, --tags`                  | Check and fix tag references; remove tags whose target manifest blob is missing; delete tag directories whose names violate the OCI tag grammar |
| `-m, --manifests`             | Check and fix manifest inconsistencies                                                             |
| `-b, --blobs`                 | Check for orphaned or corrupted blobs; prune stale blob-index entries for deleted namespaces       |
| `-r, --retention`             | Enforce retention policies (delete expired manifests)                                              |
| `-u, --uploads <duration>`    | Check upload sessions: remove broken or partial state and uploads older than the given duration    |
| `-p, --multipart <duration>`  | Cleanup orphan S3 multipart uploads older than duration                                            |
| `-l, --links`                 | Fix links format inconsistencies; remove revisions whose manifest blob is missing; prune phantom referrer back-links |
| `-M, --media-types`           | Backfill missing `media_type` on manifest links; remove revisions whose manifest blob is missing   |
| `-R, --referrers`             | Check for and remove orphan referrer links whose referrer manifest is no longer a current revision |
| `-n, --orphan-namespaces`     | Delete all content for namespaces not owned by any configured repository (destructive; see below)  |
| `-d, --dry-run`               | Preview changes without applying them                                                              |

---

## Basic Usage

### Preview Mode (Dry Run)

See what would be deleted without making changes:

```bash
./angos -c config.toml scrub --dry-run --tags --manifests --blobs --retention
```

### Run Full Cleanup

Run all checks (tags, manifests, blobs, and retention policies):

```bash
./angos -c config.toml scrub --tags --manifests --blobs --retention
```

### Selective Cleanup

Run only specific checks:

```bash
# Enforce only retention policies
./angos -c config.toml scrub --retention

# Clean up orphaned blobs only
./angos -c config.toml scrub --blobs

# Remove incomplete uploads older than 1 hour
./angos -c config.toml scrub --uploads 1h

# Cleanup orphan S3 multipart uploads older than 24 hours
./angos -c config.toml scrub --multipart 24h
```

### With Logging

```bash
RUST_LOG=info ./angos -c config.toml scrub --tags --manifests --blobs --retention
```

---

## Scheduling

### Cron (Linux/macOS)

```bash
# Daily at 3 AM - full cleanup
0 3 * * * /usr/bin/angos -c /etc/registry/config.toml scrub --tags --manifests --blobs --retention >> /var/log/registry-scrub.log 2>&1

# Weekly on Sunday at 2 AM
0 2 * * 0 /usr/bin/angos -c /etc/registry/config.toml scrub --tags --manifests --blobs --retention
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
ExecStart=/usr/bin/angos -c /etc/registry/config.toml scrub --tags --manifests --blobs --retention
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
              args: ["-c", "/config/config.toml", "scrub", "--tags", "--manifests", "--blobs", "--retention"]
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
    command: ["-c", "/config/config.toml", "scrub", "--tags", "--manifests", "--blobs", "--retention"]
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

### Protected Items

Never deleted by retention:
- Child manifests of multi-platform indexes
- Manifests with referrers (signatures, SBOMs)

This scope is retention only. `-n` clears the entire orphan namespace, including index children and referrer/signature manifests.

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
  RUST_LOG=debug ./angos -c config.toml scrub --dry-run --tags --manifests --blobs --retention
  ```

### Storage Not Reduced

- Blobs may be shared across manifests
- Run scrub again after manifest deletion
- Check for incomplete uploads

### Lock Errors

For multi-replica deployments:
- Ensure Redis is configured for locking
- Only run one scrub instance at a time

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

The `--media-types` flag reads each manifest blob and writes its `media_type` into the link metadata. This enables optimizations that avoid full blob reads on HEAD requests and redirect paths. Run this once after upgrading to populate existing links:

```bash
# Preview what would be backfilled
./angos -c config.toml scrub --media-types --dry-run

# Backfill media types on all manifest links
./angos -c config.toml scrub --media-types
```

This is idempotent and safe to run multiple times.

### Blob Index Migration

Legacy single-file blob indexes (`index.json`) keep working at runtime against both the S3 and filesystem backends. Reads consult the sharded layout first and fall back to the legacy file when no sharded entry exists, and writes are applied in place to a legacy file when one is present so the layout never splits mid-blob. Operators are **not** forced to run scrub just to keep serving traffic.

`scrub` is the only way to convert the on-disk blob-index layout from legacy to sharded:

- `scrub --blobs` iterates every blob, rewrites each legacy `index.json` into per-namespace shards under `refs/{namespace}.json`, and deletes the legacy file once the shards are written.

Running scrub benefits operators who want the per-namespace shard layout's lock granularity and concurrency characteristics for old blobs; the runtime fallback is correct but does not gain those properties for entries that are still in legacy form. Operators with no legacy data on disk can skip this step.

```bash
# Migrate legacy blob indexes
./angos -c config.toml scrub --blobs
```

This migration is **idempotent**: re-running scrub is safe and will simply skip data that is already in the sharded layout.

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
