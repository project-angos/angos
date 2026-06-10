---
displayed_sidebar: howto
sidebar_position: 6
title: "Access Control"
---

# Set Up Access Control

Configure CEL-based access control policies for fine-grained authorization.

## Prerequisites

- Angos running
- Optional: Authentication configured (basic auth, mTLS, or OIDC)

## Basic Concepts

Access control uses CEL (Common Expression Language) to evaluate rules:
- **default = "allow"**: Deny if any rule returns true
- **default = "deny"**: Allow if any rule returns true

Policies can be configured globally or per-repository.

---

## Global Policy

Apply policies to all repositories:

```toml
[global.access_policy]
default = "deny"
rules = [
  "identity.username != null"  # Require authentication
]
```

---

## Repository Policy

Override or supplement global policies for specific repositories:

```toml
[repository."production".access_policy]
default = "deny"
rules = [
  "identity.username == 'admin'",
  "identity.certificate.organizations.contains('Platform')"
]
```

---

## Common Patterns

### Require Authentication

```toml
[global.access_policy]
default = "deny"
rules = [
  "identity.username != null"
]
```

### Read-Only for Everyone, Write for Admins

```toml
[global.access_policy]
default = "deny"
rules = [
  # Anyone can pull images (anonymous read)
  "request.action in ['get-manifest', 'get-blob', 'list-tags']",

  # Only admin can write
  "identity.username == 'admin'"
]
```

:::warning
Do not use `request.action.startsWith('get-')` for anonymous access. This would also allow
`get-api-version` without authentication, which causes Docker to skip sending credentials on
all subsequent requests — even pushes. Always list only the specific read actions you need.
:::

### IP-Based Access

```toml
rules = [
  # Allow from internal network
  "identity.client_ip.startsWith('10.0.')",

  # Allow authenticated users from anywhere
  "identity.username != null"
]
```

### Certificate Organization

```toml
rules = [
  "identity.certificate.organizations.contains('Engineering')"
]
```

### OIDC Claims

```toml
rules = [
  '''identity.oidc != null &&
     identity.oidc.claims["email"].endsWith("@company.com")'''
]
```

---

## Action-Based Policies

Control access to specific operations:

### Push/Pull Separation

```toml
rules = [
  # Everyone can pull
  "request.action == 'get-blob' || request.action == 'get-manifest'",

  # Only deployers can push
  "identity.username == 'deployer' && request.action.startsWith('put-')"
]
```

### Delete Restriction

```toml
rules = [
  # Allow normal operations
  "identity.username != null && !request.action.startsWith('delete-')",

  # Only admin can delete
  "identity.username == 'admin'"
]
```

### Health and Metrics

```toml
rules = [
  # Allow unauthenticated health checks
  "request.action == 'healthz'",

  # Require auth for everything else
  "identity.username != null"
]
```

### Restrict cross-repository blob mount

A cross-repository mount (`POST .../blobs/uploads/?mount={digest}[&from={repository}]`) grants the target
namespace a reference to an existing blob with no upload. Angos grants it only when the caller is
authorized to **read** the blob from a namespace that holds it -- the source repository named by
`{repository}`, or (for a from-less mount) any namespace that references the blob. A caller who cannot read
the source falls back to a normal upload instead, so a mount never hands over a blob the caller could
not otherwise pull -- you do not have to write a policy to close that gap.

Beyond that source-read check, a mount is its own route and CEL action, `mount-blob`, distinct from
`start-upload`, so a policy can restrict who may mount **at all** with a single
`request.action == 'mount-blob'` rule -- independent of the rules governing ordinary uploads. Denying
it rejects the mount request; Angos's own replication transparently falls back to a normal upload when
a downstream denies the mount, so denying `mount-blob` never breaks replication.

For example, restrict every mount to a trusted identity (the replicator), leaving ordinary uploads
untouched. Under a `default = "deny"` policy, add an allow-rule for it:

```toml
[repository."app".access_policy]
default = "deny"
rules = [
  # ...your usual allow-rules for pull, manifest push, start-upload...
  "identity.id == 'replicator' && request.action == 'mount-blob'",
]
```

Under a `default = "allow"` policy, deny it instead:

```toml
[repository."app".access_policy]
default = "allow"
rules = [
  "request.action == 'mount-blob' && identity.id != 'replicator'",
]
```

The `request.from` field (the mount source repository, present only on `mount-blob`) is available if
you need to distinguish a scoped mount from a from-less one.

### Restrict replication writes

A replication write is an ordinary manifest `PUT`/`DELETE` carrying the `X-Angos-Source-Timestamp`
header (see the [API reference](../reference/api-endpoints.md#replication-request-header)). The
receiver persists that timestamp as the tag's creation time, which drives last-writer-wins conflict
resolution and age-based retention. A future-dated timestamp is clamped to the receiver's clock, but
a **backdated** one is accepted from *any* identity allowed to push, letting it weaken a tag in
later LWW races or age a tag straight into a retention window.

:::warning
On every instance that receives replication, restrict manifest pushes and deletes on replicated
repositories to the replicator identity (plus whoever should genuinely push there). There is no
separate replication permission, the CEL `access_policy` on the ordinary write actions is the gate.
:::

Under a `default = "deny"` policy:

```toml
[repository."app".access_policy]
default = "deny"
rules = [
  # pulls for everyone authenticated, writes for the replicator only
  "request.action == 'get-manifest' || request.action == 'get-blob'",
  "identity.id == 'replicator' && request.action in ['put-manifest', 'delete-manifest', 'start-upload', 'update-upload', 'complete-upload', 'mount-blob']",
]
```

The same `identity.id == 'replicator'` credential is the one configured as `username` on the peer's
`[[repository."app".downstream]]` entry.

---

## Web UI Access

Control access to UI-specific actions:

```toml
[global.access_policy]
default = "deny"
rules = [
  # Allow UI assets to load
  "request.action == 'ui-asset' || request.action == 'ui-config'",

  # Require auth for browsing
  "identity.username != null && request.action.startsWith('list-')",

  # Normal registry operations
  "identity.username != null"
]
```

---

## Job Queue Administration

The durable cache-fill job queue is exposed through four extension actions:
`list-jobs` and `list-failed-jobs` (read the pending and dead-letter
partitions) and `retry-job` and `delete-job` (mutate them). These are **not**
covered by the generic `list-*` browsing rule above — they are deliberately
separate action names so you can gate job administration behind higher
privilege than ordinary catalogue browsing.

Grant them only to operators:

```toml
[global.access_policy]
default = "deny"
rules = [
  # Anyone authenticated may browse the catalogue and uploads…
  "identity.username != null && request.action in ['list-repositories', 'list-namespaces', 'list-tags', 'list-uploads']",

  # …but only admins may inspect or mutate the durable job queue.
  "identity.username == 'admin' && request.action in ['list-jobs', 'list-failed-jobs', 'retry-job', 'delete-job']",

  # Normal pull traffic.
  "identity.username != null && request.action in ['get-manifest', 'get-blob']",
]
```

With `default = "deny"`, the job actions are denied for every caller that does
not match the admin rule — including unauthenticated requests and the Web UI's
own browsing identity. The UI's **Jobs** page only renders successfully for
identities the policy admits.

---

## Mixed Authentication

Combine different authentication methods:

```toml
[global.access_policy]
default = "deny"
rules = [
  # Basic auth admin
  "identity.username == 'admin'",

  # mTLS with specific organization
  "identity.certificate.organizations.contains('DevOps')",

  # GitHub Actions from organization
  '''identity.oidc != null &&
     identity.oidc.provider_name == "github-actions" &&
     identity.oidc.claims["repository"].startsWith("myorg/")''',

  # Corporate OIDC users can pull
  '''identity.oidc != null &&
     identity.oidc.provider_name == "corporate" &&
     request.action.startsWith("get-")'''
]
```

---

## Multi-Repository Setup

```toml
# Global baseline
[global.access_policy]
default = "deny"
rules = [
  "request.action == 'healthz'"  # Always allow health checks
]

# Public read-only repo
[repository."public".access_policy]
default = "deny"
rules = [
  "request.action in ['get-manifest', 'get-blob', 'list-tags']"
]

# Development: team access
[repository."dev".access_policy]
default = "deny"
rules = [
  "identity.certificate.organizations.contains('Engineering')"
]

# Production: restricted access
[repository."prod".access_policy]
default = "deny"
rules = [
  "identity.username == 'deployer'",
  '''identity.oidc != null &&
     identity.oidc.claims["ref"] == "refs/heads/main"'''
]
```

---

## Verification

### Test with curl

```bash
# Without auth
curl http://localhost:8000/v2/

# With basic auth
curl -u admin:password http://localhost:8000/v2/

# With bearer token
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/v2/
```

### Debug Logging

```bash
RUST_LOG=angos::registry::access_policy=debug \
  ./angos server
```

---

## Troubleshooting

**All requests denied:**
- Check if any rule can match
- For `default = "deny"`, at least one rule must return true
- Enable debug logging to see rule evaluation

**OIDC rules not matching:**
- Always check `identity.oidc != null` first
- Use bracket notation: `identity.oidc.claims["claim"]`
- Check claim values in debug logs

**Rule evaluation errors:**
- Rules that fail at runtime (e.g., referencing a missing field) or return a non-boolean value immediately deny the request (fail-closed) in both `allow` and `deny` mode; a warning is logged identifying the failing rule
- Check for null access (e.g., `identity.oidc.claims` when not using OIDC)

## Reference

### Available Actions

| Action              | Description                          |
|---------------------|--------------------------------------|
| `healthz`           | Health check endpoint                |
| `readyz`            | Readiness check endpoint             |
| `metrics`           | Prometheus metrics endpoint          |
| `get-api-version`   | API version check                    |
| `start-upload`      | Start blob upload                    |
| `get-upload`        | Get upload status                    |
| `update-upload`     | Continue chunked upload              |
| `complete-upload`   | Complete upload                      |
| `cancel-upload`     | Cancel upload                        |
| `get-blob`          | Download blob                        |
| `delete-blob`       | Delete blob                          |
| `get-manifest`      | Pull manifest                        |
| `put-manifest`      | Push manifest                        |
| `delete-manifest`   | Delete manifest                      |
| `get-referrers`     | Get referrers                        |
| `list-catalog`      | List repositories (OCI catalog)      |
| `list-tags`         | List tags                            |
| `list-repositories` | Extension: list configured repos     |
| `list-namespaces`   | Extension: list namespaces in a repo |
| `list-revisions`    | Extension: list manifest revisions   |
| `list-uploads`      | Extension: list uploads in progress  |
| `list-jobs`         | Extension: list pending/in-flight jobs |
| `list-failed-jobs`  | Extension: list dead-letter jobs     |
| `retry-job`         | Extension: requeue a dead-letter job |
| `delete-job`        | Extension: delete a pending/failed job |
| `ui-asset`          | UI static assets                     |
| `ui-config`         | UI configuration                     |

- [CEL Expressions Reference](../reference/cel-expressions.md) - All variables and functions
- [Configuration Reference](../reference/configuration.md) - Policy configuration options
