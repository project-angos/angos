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

## Online vs Offline Operations

Angos performs garbage collection **online** during normal operation - unreferenced blobs are automatically cleaned up without downtime.

The `scrub` command is for **offline maintenance** tasks:
- Checking and repairing data corruption
- Verifying storage consistency
- Enforcing retention policies
- Cleaning up stale uploads

## What Scrub Does

The `scrub` command performs various maintenance operations. Each check must be explicitly enabled:

| Flag                          | Description                                             |
|-------------------------------|---------------------------------------------------------|
| `-t, --tags`                  | Check and fix invalid tag references                    |
| `-m, --manifests`             | Check and fix manifest inconsistencies                  |
| `-b, --blobs`                 | Check for orphaned or corrupted blobs                   |
| `-r, --retention`             | Enforce retention policies (delete expired manifests)   |
| `-u, --uploads <duration>`    | Remove incomplete uploads older than duration           |
| `-p, --multipart <duration>`  | Cleanup orphan S3 multipart uploads older than duration |
| `-l, --links`                 | Fix links format inconsistencies                        |
| `-M, --media-types`           | Backfill missing `media_type` on manifest links         |
| `-d, --dry-run`               | Preview changes without applying them                   |

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
| Upload            | Incomplete/abandoned             |

### Protected Items

Never deleted:
- Child manifests of multi-platform indexes
- Manifests with referrers (signatures, SBOMs)

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
curl http://localhost:8000/v2/_ext/_repositories | jq
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

### Automatic Blob Index Migration

When the S3 backend reads blob metadata, it automatically migrates legacy single-file blob indexes (`index.json`) to the per-namespace shard format (`refs/{namespace}.json`). This migration is:

- **Transparent** - Requires no operator action
- **Idempotent** - Safe to run multiple times
- **Crash-safe** - Shard files are written before the legacy file is deleted

The `scrub --blobs` command triggers this migration as part of its normal blob iteration, making it an effective way to batch-migrate all legacy indexes:

```bash
# Migrate all legacy blob indexes to the new shard format
./angos -c config.toml scrub --blobs
```

Running this once after upgrading will convert any remaining legacy `index.json` files to the sharded format automatically.

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
