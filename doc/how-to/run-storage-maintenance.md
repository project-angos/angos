---
displayed_sidebar: howto
sidebar_position: 10
title: "Storage Maintenance"
---

# Run Storage Maintenance

Repair storage inconsistencies with the `scrub` command and reclaim aged or policy-expired data with the `prune` command.

## Prerequisites

- Angos installed
- Access to the same configuration and storage as the running registry

## Automatic vs Scheduled Maintenance

Angos performs garbage collection **automatically** during normal operation: unreferenced blobs are cleaned up as part of request handling, without downtime.

The `scrub` and `prune` commands run as **separate periodic processes** that operate alongside the live server (no shutdown required). The split is:

- **scrub is structural.** It walks every object key, validates it, repairs derivable state, and quarantines anything that does not belong in an angos store. It takes no age thresholds and no configuration-relative decisions.
- **prune is config and time.** It enforces retention policies, clears namespaces no configured repository owns, reclaims upload-lifecycle leftovers older than the `-u` window, and deletes queued jobs whose configuration is gone.

## What Scrub Does

Scrub streams every key in both stores (blob and metadata), categorizes it by shape, and validates it concurrently, in three ordered passes: links and job records, then blob-index shards, then blob data. Every check always runs; there are no per-check flags.

Before applying a cross-key repair, scrub confirms the inconsistency is settled damage rather than a transaction caught mid-apply: while a live transaction intent covers the involved keys, the candidate waits briefly and re-checks, and one that never settles is left for the next run. This is what makes scrub safe to run against a live server.

| Concern | Behavior |
|---|---|
| Manifest-derived links | Recreates every link a manifest implies (config, layer, sub-manifest, digest revision) and the `referenced_by` back-links |
| Blob-index grants | Re-issues grants the index is missing relative to the manifests; removes entries whose link file is gone |
| Dangling references | Removes tags and revisions whose manifest blob is missing, and orphan referrer entries |
| Invalid names | Deletes tag, namespace, and upload directories whose names violate the OCI grammar (nothing can address them) |
| Corrupt content | Deletes links, job records, and index shards whose content does not parse |
| Orphan blobs | Reclaims blobs with no index references, re-checked under the blob-data lock at apply time |
| Unrecognized keys | Moves them to `_lost_and_found/` in the same store, preserving their bytes |
| Engine housekeeping | Runs the transaction engine's own janitor sweeps: orphaned `.tx-bodies/` staging and expired `.tx-locks/` objects (age-gated by engine thresholds; skipped in dry-run) |

| Option | Short | Description |
|---|---|---|
| `--dry-run` | `-d` | Preview changes without applying them |
| `--concurrency <N>` | | Keys validated concurrently per pass (default 25) |
| `--delete-unknown` | | Delete unrecognized keys outright instead of quarantining them |

### The lost-and-found prefix

A key that matches no known angos layout is **moved, not deleted**, to `_lost_and_found/<original key>` in the store it was found in. Inspect that prefix after a run; restore a key by moving it back, or delete the prefix once satisfied. Scrub never re-processes quarantined objects.

When the bytes are not worth keeping (a store polluted by a foreign writer, or a quarantine already reviewed), `scrub --delete-unknown` deletes unrecognized keys outright instead. The deletion is unrecoverable, so prefer a `scrub -d` preview first.

Because scrub quarantines (or with `--delete-unknown`, deletes) what it does not recognize, **run it from the same angos version as the server fleet**. After an upgrade, run `scrub -d` first and review the report.

### Convergence

A repair can create new derivable state (a recreated revision link derives its back-links on the next pass), so a heavily damaged store may need more than one run to fully converge. Run scrub until it reports zero changes; every run is safe to repeat.

## What Prune Does

Prune first enforces retention policies (see [Configure Retention Policies](configure-retention-policies.md)), then reclaims everything gated on the `-u` age window (default `1h`):

| Concern | Behavior |
|---|---|
| Upload sessions | Deletes sessions older than the window, or with broken session state |
| Orphan S3 multiparts | Aborts in-flight multipart uploads older than the window whose session marker is gone |
| Grant-only blob ownership | Retention policies decide, like any untagged content (no tag, `pushed_at` = upload time); the `-u` window only shields in-flight pushes, and with no policies configured the grant is retained |
| Byteless index entries | Removes blob-index entries whose blob bytes never landed |
| Orphan namespaces | Clears revisions, tags, in-flight uploads, and blob grants of every namespace not owned by any configured repository (always on; see below) |
| Orphan jobs | Deletes queued replication/cache jobs whose downstream or repository is no longer configured (always on) |

These need an age threshold because a structural check cannot distinguish an in-flight push (blob uploaded, manifest seconds away) from an abandoned one; the `-u` window is exactly that upload-lifecycle age. Run prune against the same configuration file the servers use.

| Option | Short | Description |
|---|---|---|
| `--dry-run` | `-d` | Preview what would be deleted without changes |
| `--uploads <dur>` | `-u` | Age window for upload-lifecycle reclamation (default `1h`) |
| `--concurrency <N>` | | Namespaces, uploads, blobs, or shards checked concurrently per sweep (default 25) |

---

## Basic Usage

```bash
# Preview everything both commands would do
./angos -c config.toml scrub --dry-run
./angos -c config.toml prune --dry-run

# Full maintenance
./angos -c config.toml scrub
./angos -c config.toml prune

# Faster walk on a large store
./angos -c config.toml scrub --concurrency 32

# Keep in-flight uploads alive for up to a day
./angos -c config.toml prune --uploads 24h

# With logging
RUST_LOG=info ./angos -c config.toml scrub
```

---

## Scheduling

Schedule maintenance with a systemd timer (host installs) or a Kubernetes CronJob; both are shown below.

The `_catalog` listing is derived directly from stored content: a namespace appears exactly when it holds at least one revision or tag, and disappears as soon as the last one is deleted. No scrub run or namespace registration step is involved.

Blob ownership markers are kept until the client issues an explicit `DELETE /v2/<name>/blobs/<digest>` request or prune reclaims a grant whose manifest never landed. This reflects the OCI blob lifecycle and is not a leak.

### Systemd Timer

Create `/etc/systemd/system/registry-scrub.service`:

```ini
[Unit]
Description=Registry Storage Maintenance

[Service]
Type=oneshot
ExecStart=/usr/bin/angos -c /etc/registry/config.toml scrub
ExecStart=/usr/bin/angos -c /etc/registry/config.toml prune
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
              args: ["-c", "/config/config.toml", "scrub"]
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

Schedule `angos prune` the same way for retention enforcement and upload reclamation; see [Configure Retention Policies](configure-retention-policies.md) for a complete CronJob example.

### Docker Compose

```yaml
services:
  scrub:
    image: ghcr.io/project-angos/angos:latest
    command: ["-c", "/config/config.toml", "scrub"]
    volumes:
      - ./config:/config:ro
      - ./data:/data
    profiles:
      - maintenance
  prune:
    image: ghcr.io/project-angos/angos:latest
    command: ["-c", "/config/config.toml", "prune"]
    volumes:
      - ./config:/config:ro
      - ./data:/data
    profiles:
      - maintenance
```

Run manually:

```bash
docker compose --profile maintenance run --rm scrub
docker compose --profile maintenance run --rm prune
```

To run them periodically, drive these commands from a systemd timer on the host (see [Systemd Timer](#systemd-timer) above).

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

| Item | Condition |
|---|---|
| Tagged manifest | `prune`: doesn't match any retention rule |
| Untagged manifest | `prune`: doesn't match any retention rule |
| Blob | `scrub`: not referenced by any manifest |
| Upload | `prune`: broken session or older than the `-u` window |
| Grant-only blob | `prune`: no manifest reference, past the `-u` window, and no retention rule keeps it |
| Queued job | `prune`: downstream or repository no longer configured |
| Corrupt object | `scrub`: content does not parse (link, shard, job record) |
| Unrecognized key | `scrub`: quarantined to `_lost_and_found/` |
| Whole namespace | `prune`: not owned by any configured `[repository]` (revisions, tags, in-flight uploads, plus the namespace's blob-ownership grants) |

### Clearing Orphan Namespaces

Orphan-namespace clearing runs on **every prune**. When a `[repository]` is removed from configuration (or data predates it), the namespaces it held no longer resolve to any repository, yet their manifests, tags, and blobs would linger forever. Prune removes the revisions, tags, and in-flight uploads of every such namespace and reclaims their blob bytes by revoking those blobs' ownership grants when no still-configured namespace shares them, so the blast radius is **every namespace whose owning repository is no longer in your config**, not just empty ones.

Safeguards apply:

- **Dry-run after config changes.** Run `angos prune --dry-run` after removing or renaming a `[repository]` and confirm the listed deletions only cover namespaces you intend to drop.
- **Byte reclaim may take a follow-up scrub.** Prune clears the namespace's links and grants; a scrub reclaims blob bytes the cascade freed.
- **Run with no writers on orphan namespaces.** A client can still push to a namespace that no longer maps to any repository; a later prune mops up anything pushed during the clear.
- **Empty-config guard.** If no `[repository]` is configured, every namespace would be an orphan, so the clearing is skipped with a warning and nothing is deleted; an emptied config can never wipe the registry.

Cleared namespaces drop out of `_catalog` automatically, since the catalog is derived from stored content.

### Protected Items

Never deleted by `prune`:
- Child manifests of multi-platform indexes
- Manifests with referrers (signatures, SBOMs)

This scope is retention only. Orphan-namespace clearing removes the entire orphan namespace, including index children and referrer/signature manifests.

---

## Monitoring

Every scrub run ends with a one-line summary: keys walked, repairs, quarantined keys, corrupt deletions, and per-key failures.

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
  RUST_LOG=debug ./angos -c config.toml prune --dry-run
  RUST_LOG=debug ./angos -c config.toml scrub --dry-run
  ```

### Storage Not Reduced

- Blobs may be shared across manifests
- Run scrub again after manifest deletion (repairs converge across runs)
- Check for incomplete uploads (`prune -u`)

### Unexpected Keys in `_lost_and_found/`

- A newer angos version may have written key shapes this scrub does not know; restore them by moving them back and re-run scrub from the matching version
- Anything else under the prefix is junk that never belonged to angos; delete it once inspected

### Lock Errors

For multi-replica deployments:
- Ensure Redis is configured for locking
- Only run one scrub instance at a time

### S3 Errors

- Verify S3 credentials have delete permissions
- Check network connectivity
- Review S3 operation timeout settings

---

## Best Practices

1. **Always dry-run first** in production
2. **Run during low-traffic periods** to minimize impact
3. **Monitor storage trends** after scheduled runs
4. **Keep retention policies conservative** initially
5. **Use Redis locking** for multi-replica deployments
6. **Re-run scrub from the same binary version as the fleet**, especially right after upgrades

## Reference

- [Configure Retention Policies](configure-retention-policies.md) - Policy syntax
- [CLI Reference](../reference/cli.md) - scrub and prune command options
