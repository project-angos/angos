---
displayed_sidebar: howto
sidebar_position: 7
title: "Retention Policies"
---

# Configure Retention Policies

Set up automated cleanup of old container images using CEL-based retention policies.

## Prerequisites

- Angos running
- `update_pull_time = true` if using pull-based retention

## How Retention Works

Retention policies define which images to **keep**. Images not matching any rule are eligible for deletion when running `scrub --retention`.

```mermaid
sequenceDiagram
    participant S as Scrubber
    participant M as Manifest
    participant R as Retention Rules

    S->>M: Evaluate manifest
    M->>M: Check protection status
    alt Index child OR Has referrers
        M-->>S: KEEP (protected)
    else Not protected
        M->>R: Evaluate rules
        alt Any rule matches
            R-->>S: KEEP
        else No rules match
            R-->>S: DELETE
        end
    end
```

**Protected manifests** are never deleted:
- Child manifests of multi-platform indexes
- Manifests with referrers (signatures, SBOMs)

---

## Basic Configuration

### Global Policy

```toml
[global]
update_pull_time = true  # Required for pull-based retention

[global.retention_policy]
rules = [
  'image.tag == "latest"',
  'image.pushed_at > now() - days(30)'
]
```

### Repository Policy

```toml
[repository."production".retention_policy]
rules = [
  'image.tag == "latest"',
  'image.pushed_at > now() - days(90)',
  'top_pushed(20)'
]
```

---

## Common Patterns

### Keep Tagged, Delete Untagged

```toml
rules = [
  'image.tag != null'
]
```

### Time-Based Retention

```toml
rules = [
  'image.pushed_at > now() - days(30)',   # Keep 30 days
  'image.last_pulled_at > now() - days(7)' # Or pulled within 7 days
]
```

### Top-K Retention

```toml
rules = [
  'top_pushed(10)',  # Keep 10 most recently pushed
  'top_pulled(5)'    # Keep 5 most recently pulled
]
```

### Semantic Version Tags

```toml
rules = [
  'image.tag != null && image.tag.matches("^v?[0-9]+\\.[0-9]+\\.[0-9]+$")'
]
```

### Combined Rules

A manifest is **KEPT** if **ANY** rule matches (rules are OR'd):

```toml
rules = [
  # Always keep latest
  'image.tag == "latest"',

  # Keep release tags forever
  'image.tag != null && image.tag.matches("^v[0-9]+\\.[0-9]+\\.[0-9]+$")',

  # Keep other tags for 30 days
  'image.tag != null && image.pushed_at > now() - days(30)',

  # Keep untagged for 7 days
  'image.pushed_at > now() - days(7)',

  # Keep top 10 most pulled
  'top_pulled(10)'
]
```

**Rule Functions:**
- `top_pushed(n)` - Keep the n most recently pushed manifests
- `top_pulled(n)` - Keep the n most recently pulled manifests

---

## Environment-Specific Policies

### Development: Aggressive Cleanup

```toml
[repository."dev".retention_policy]
rules = [
  'image.tag == "latest"',
  'image.pushed_at > now() - days(7)',
  'top_pushed(5)'
]
```

### Staging: Moderate Retention

```toml
[repository."staging".retention_policy]
rules = [
  'image.tag == "latest"',
  'image.pushed_at > now() - days(14)',
  'top_pushed(10)'
]
```

### Production: Conservative

```toml
[repository."production".retention_policy]
rules = [
  'image.tag == "latest"',
  'image.pushed_at > now() - days(90)',
  'image.tag != null && image.tag.matches("^v[0-9]+\\.")',
  'top_pushed(50)'
]
```

---

## Global + Repository Policies

When both are defined, a manifest is kept if **either** policy matches:

```toml
# Global baseline: keep everything for at least 7 days
[global.retention_policy]
rules = [
  'image.pushed_at > now() - days(7)'
]

# Repo-specific: extend for production
[repository."production".retention_policy]
rules = [
  'image.pushed_at > now() - days(365)'
]
```

Result: Production images kept for 365 days, others for 7 days.

---

## Enforcing Retention Policies

Retention policies are enforced by the `scrub` command with the `--retention` flag:

```bash
# Preview what would be deleted
./angos -c config.toml scrub --retention --dry-run

# Enforce retention policies
./angos -c config.toml scrub --retention
```

### Scheduled Enforcement

**Cron:**
```bash
0 3 * * * /usr/bin/angos -c /etc/registry/config.toml scrub --retention
```

**Kubernetes CronJob:**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: registry-retention
spec:
  schedule: "0 3 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: scrub
              image: ghcr.io/project-angos/angos:latest
              args: ["-c", "/config/config.toml", "scrub", "--retention"]
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

---

## Verification

### Check What Would Be Deleted

```bash
RUST_LOG=info ./angos scrub --retention --dry-run
```

### List Current Manifests

```bash
curl http://localhost:8000/v2/_ext/myrepo/myimage/_revisions | jq
```

---

## Troubleshooting

**Images not being deleted:**
- Check if they match any retention rule
- Check if they're protected (index child or has referrers)
- Verify `scrub` command is running

**Pull time not tracked:**
- Enable `update_pull_time = true` in global config
- Pull times are only tracked after enabling

**Rules not matching:**
- Use debug logging: `RUST_LOG=angos::command::scrub=debug`
- Check that `image.tag` is null for untagged manifests

## Reference

- [CEL Expressions Reference](../reference/cel-expressions.md) - Retention variables and functions
- [CLI Reference](../reference/cli.md) - scrub command details
