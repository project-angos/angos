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
- **default_allow = true**: Deny if any rule returns false
- **default_allow = false**: Allow if any rule returns true

Policies can be configured globally or per-repository.

---

## Global Policy

Apply policies to all repositories:

```toml
[global.access_policy]
default_allow = false
rules = [
  "identity.username != null"  # Require authentication
]
```

---

## Repository Policy

Override or supplement global policies for specific repositories:

```toml
[repository."production".access_policy]
default_allow = false
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
default_allow = false
rules = [
  "identity.username != null"
]
```

### Read-Only for Everyone, Write for Admins

```toml
[global.access_policy]
default_allow = false
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

---

## Web UI Access

Control access to UI-specific actions:

```toml
[global.access_policy]
default_allow = false
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

## Mixed Authentication

Combine different authentication methods:

```toml
[global.access_policy]
default_allow = false
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
default_allow = false
rules = [
  "request.action == 'healthz'"  # Always allow health checks
]

# Public read-only repo
[repository."public".access_policy]
default_allow = false
rules = [
  "request.action in ['get-manifest', 'get-blob', 'list-tags']"
]

# Development: team access
[repository."dev".access_policy]
default_allow = false
rules = [
  "identity.certificate.organizations.contains('Engineering')"
]

# Production: restricted access
[repository."prod".access_policy]
default_allow = false
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
- For `default_allow = false`, at least one rule must return true
- Enable debug logging to see rule evaluation

**OIDC rules not matching:**
- Always check `identity.oidc != null` first
- Use bracket notation: `identity.oidc.claims["claim"]`
- Check claim values in debug logs

**Rule evaluation errors:**
- Failed rules are skipped with a warning
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
| `ui-asset`          | UI static assets                     |
| `ui-config`         | UI configuration                     |

- [CEL Expressions Reference](../reference/cel-expressions.md) - All variables and functions
- [Configuration Reference](../reference/configuration.md) - Policy configuration options
