---
displayed_sidebar: reference
sidebar_position: 3
title: "CEL Expressions"
---

# CEL Expressions Reference

Angos uses [CEL (Common Expression Language)](https://cel.dev/) for access control policies, retention policies, and the scan and index policies. This reference documents all available variables and functions.

---

## Access Control Variables

### Identity (`identity`)

Information about the authenticated client.

| Variable             | Type     | Description                    |
|----------------------|----------|--------------------------------|
| `identity.id`        | string?  | Identity ID from configuration |
| `identity.username`  | string?  | Authenticated username         |
| `identity.client_ip` | string?  | Client IP address (the socket address; a forwarded address is used only when the peer is listed in `global.trusted_proxies`) |

### Certificate (`identity.certificate`)

Available when client presents an mTLS certificate.

| Variable                             | Type     | Description               |
|--------------------------------------|----------|---------------------------|
| `identity.certificate.common_names`  | [string] | Certificate Common Names  |
| `identity.certificate.organizations` | [string] | Certificate Organizations |

### OIDC (`identity.oidc`)

Available when client authenticates with OIDC token. **Always check for null before accessing.**

| Variable                      | Type    | Description                                        |
|-------------------------------|---------|----------------------------------------------------|
| `identity.oidc`               | object? | OIDC context (null if not OIDC)                    |
| `identity.oidc.provider_name` | string  | Configured provider name                           |
| `identity.oidc.claims`        | map     | JWT claims (access with bracket notation)          |

**GitHub Actions Claims:**

| Claim                   | Description                                   |
|-------------------------|-----------------------------------------------|
| `repository`            | Full repository name (e.g., `"myorg/myrepo"`) |
| `ref`                   | Git reference (e.g., `"refs/heads/main"`)     |
| `sha`                   | Commit SHA                                    |
| `workflow`              | Workflow file name                            |
| `workflow_ref`          | Full workflow reference path                  |
| `actor`                 | User who triggered the workflow               |
| `event_name`            | Trigger event (push, pull_request, etc.)      |
| `environment`           | Deployment environment                        |
| `repository_owner`      | Repository owner                              |
| `repository_visibility` | Repository visibility (public/private)        |

**Standard OIDC Claims:**

| Claim    | Description                     |
|----------|---------------------------------|
| `sub`    | Subject identifier              |
| `iss`    | Issuer                          |
| `aud`    | Audience                        |
| `email`  | User email                      |
| `name`   | User name                       |
| `groups` | User groups (provider-specific) |

### Request (`request`)

Information about the current request. Fields are present based on the action type.

| Variable                | Type    | Description                                |
|-------------------------|---------|--------------------------------------------|
| `request.action`        | string  | Action being requested (see Actions table) |
| `request.namespace`     | string? | Repository namespace                       |
| `request.digest`        | string? | Blob or manifest digest (blob actions; by-digest `put-manifest`) |
| `request.reference`     | string? | Manifest tag or digest reference (`get`/`head`/`delete-manifest`) |
| `request.tags`          | [string] | Tags a `put-manifest` creates, empty when none (the target tag, or `?tag=` params on a by-digest push) |
| `request.session_id`    | string? | Upload session id                          |
| `request.from`          | string? | Cross-repository mount source repository (`mount-blob` only) |
| `request.n`             | int?    | Pagination limit                           |
| `request.last`          | string? | Pagination marker                          |
| `request.artifact_type` | string? | Referrer artifact type filter              |
| `request.digest_algorithm` | string? | Digest algorithm a `start-upload` will close with (`?digest-algorithm=`) |
| `request.queue`         | string? | Durable queue for `_jobs` admin actions (`cache`, `replication` or `scan`) |

Optional `request` fields are **omitted** when unset (only `request.action` is always present), so test their presence with the `has()` macro rather than a null comparison: `has(request.from)` is safe, whereas `request.from != null` raises a "no such key" error (evaluated fail-closed) when the field is absent. Optional `identity` fields, by contrast, are present as `null`, so `identity.username != null` is correct there.

### Actions

| Action              | Description                  |
|---------------------|------------------------------|
| `healthz`           | Health check endpoint        |
| `readyz`            | Readiness check endpoint     |
| `metrics`           | Prometheus metrics endpoint  |
| `get-api-version`   | API version check            |
| `start-upload`      | Start blob upload            |
| `mount-blob`        | Cross-repository blob mount (distinct from `start-upload`)            |
| `update-upload`     | Continue chunked upload      |
| `complete-upload`   | Complete upload              |
| `get-upload`        | Get upload status            |
| `cancel-upload`     | Cancel upload                |
| `get-blob`          | Download blob                |
| `delete-blob`       | Delete blob                  |
| `put-manifest`      | Push manifest                |
| `get-manifest`      | Pull manifest                |
| `delete-manifest`   | Delete manifest              |
| `get-referrers`     | Get referrers                |
| `list-catalog`      | List repositories            |
| `list-tags`         | List tags                    |
| `ui-asset`          | UI static assets             |
| `ui-config`         | UI configuration             |
| `get-token`         | Registry token exchange      |
| `list-repositories` | Extension: list repositories |
| `list-namespaces`   | Extension: list namespaces   |
| `list-revisions`    | Extension: list revisions    |
| `list-uploads`      | Extension: list uploads      |
| `list-jobs`         | Extension: list pending jobs |
| `list-failed-jobs`  | Extension: list failed jobs  |
| `retry-job`         | Extension: requeue a job     |
| `delete-job`        | Extension: delete a job      |

---

## Retention Policy Variables

The scan and index policies (`scan.rules`, `index.rules`) share these variables and the retention functions below. Judged as an image lands, `image.pushed_at` is now, `image.last_pulled_at` and `image.scanned_at` are 0, `image.tag` is each pushed tag in turn or null, and the pushed tags are the whole `top_pushed` ranking; `reconcile scan` and `reconcile index` judge the image again with its stored values.

### Image (`image`)

Information about the manifest being evaluated.

| Variable               | Type        | Description                        |
|------------------------|-------------|------------------------------------|
| `image.tag`            | string/null | Tag name, or null if untagged      |
| `image.pushed_at`      | int         | Push time (seconds since epoch)    |
| `image.last_pulled_at` | int         | Last pull time (0 if never pulled) |
| `image.scanned_at`     | int         | Time of the newest scan report (0 if never scanned); set for the scan and index policies, always 0 under retention |

---

## Functions

### Time Functions

| Function     | Description                         |
|--------------|-------------------------------------|
| `now()`      | Current time in seconds since epoch |
| `days(n)`    | Convert days to seconds             |
| `hours(n)`   | Convert hours to seconds            |
| `minutes(n)` | Convert minutes to seconds          |

### Access Control Functions

| Function                  | Description                                                                        |
|---------------------------|------------------------------------------------------------------------------------|
| `has_repository_policy()` | True if a `[repository]` declaring its own `access_policy` covers the request       |

It is false for a namespace no `[repository]` declares and for a repository declaring no `access_policy`: in both cases nothing else would decide, so a global rule granting on it grants outright. Inside a repository policy it is always true.

### Retention Functions

| Function        | Description                               |
|-----------------|-------------------------------------------|
| `top_pushed(n)` | True if among n most recently pushed tags |
| `top_pulled(n)` | True if among n most recently pulled tags |

Rankings are per namespace and contain tag names only: an untagged image has `image.tag == null` and never matches `top_pushed` or `top_pulled`. A ranking lists the tags carrying the time it orders by, so a tag nobody ever pulled never matches `top_pulled(n)` however few tags the namespace holds, and a tag whose push time is unknown never matches `top_pushed(n)`.

`top_pulled` and `image.last_pulled_at` require `update_pull_time = true`; `angos prune` refuses to start otherwise, and a scan or index policy using them fails validation, since pull times would never be recorded.

### String Functions (CEL built-in)

| Function               | Description                 |
|------------------------|-----------------------------|
| `.matches(regex)`      | Match against regex pattern |
| `.startsWith(prefix)`  | Check prefix                |
| `.endsWith(suffix)`    | Check suffix                |
| `.contains(substring)` | Check substring             |
| `in [list]`            | Check membership in list    |

---

## Example Expressions

### Access Control

```cel
// Require authentication
identity.username != null

// Allow specific user
identity.username == 'admin'

// Allow certificate with organization
identity.certificate.organizations.contains('DevOps')

// OIDC: GitHub Actions from specific org
identity.oidc != null && identity.oidc.claims['repository'].startsWith('myorg/')

// OIDC: Allow main branch pushes
identity.oidc != null &&
  identity.oidc.claims['ref'] == 'refs/heads/main' &&
  request.action.startsWith('put-')

// OIDC: Allow specific actors
identity.oidc != null &&
  identity.oidc.claims['actor'] in ['alice', 'bob', 'dependabot[bot]']

// Allow pull operations for everyone (anonymous read)
request.action in ['get-manifest', 'get-blob']

// Restrict delete to admins
identity.username == 'admin' && request.action == 'delete-manifest'
```

### Retention

```cel
// Keep tagged images
image.tag != null

// Keep images pushed within 30 days
image.pushed_at > now() - days(30)

// Keep images pulled within 7 days
image.last_pulled_at > now() - days(7)

// Keep latest tag
image.tag == 'latest'

// Keep semantic version tags
image.tag != null && image.tag.matches('^v?[0-9]+\\.[0-9]+\\.[0-9]+$')

// Keep 10 most recently pushed
top_pushed(10)

// Keep 5 most recently pulled
top_pulled(5)
```

### Scan and Index Policies

A matching rule gives the image the opposite of the table's `default`, which is `skip` unless set.

```cel
// No default: scan every image as it lands, and again monthly
image.scanned_at < now() - days(30)

// No default: refresh weekly the reports of what is still pulled
image.scanned_at < now() - days(7) && image.last_pulled_at > now() - days(30)

// No default: index the tags people open as they land, the rest on first open
image.tag == 'latest' || top_pulled(20)

// default = "scan": skip nightly builds
image.tag == 'nightly'
```

---

## Common Patterns

### Default-Deny with Exceptions

```toml
[global.access_policy]
default = "deny"
rules = [
  "identity.username == 'admin'",
  "identity.certificate.organizations.contains('Platform')",
  "identity.oidc != null && identity.oidc.claims['repository'].startsWith('myorg/')"
]
```

### Defer to Repository Policies

```toml
[global.access_policy]
default = "deny"
rules = [
  "request.action in ['healthz', 'readyz', 'metrics']",
  "has_repository_policy()"
]

[repository."myorg/website".access_policy]
default = "deny"
rules = ["identity.oidc.claims['repository'] == 'myorg/website-frontend'"]
```

The global rules admit a request only when the repository covering it declares rules of its own, which then decide; every other namespace is left to the global default.

Keep a broad global grant off the actions that carry a namespace once repositories deny on their own. A rule such as `request.action.startsWith('list-')` also grants `list-revisions`, `list-uploads` and `list-tags` on namespaces no repository covers, which answer with an empty listing, while the same request under a denying repository answers `401`: the difference names the repositories that exist. Grant the listings that carry no namespace (`list-catalog`, `list-repositories`, `list-namespaces`, `list-jobs`, `list-failed-jobs`) by name instead, and leave the namespaced ones to `has_repository_policy()`, so a hidden repository and an unconfigured path answer alike:

```toml
rules = [
  "request.action in ['list-catalog', 'list-repositories', 'list-namespaces']",
  "has_repository_policy()"
]
```

Those listings need no such grant to stay safe: each filters its own entries by what the caller may read.

### Read-Only for Guests

```toml
[repository."public".access_policy]
default = "deny"
rules = [
  "request.action in ['get-manifest', 'get-blob', 'list-tags']",
  "identity.username == 'admin'"
]
```

:::warning
Avoid `request.action.startsWith('get-')` for anonymous access: it includes `get-api-version`,
which causes Docker clients to skip authentication entirely. List specific actions instead.
:::

### Combined Retention Rules

```toml
[repository."production".retention_policy]
rules = [
  "image.tag == 'latest'",
  "image.pushed_at > now() - days(90)",
  "top_pushed(20)"
]
```

---

## Error Handling

**Access policy rules:**
- A rule that fails to evaluate or returns a non-boolean value stops evaluation immediately and denies the request (fail-closed), with a `warn`-level log
- For default-deny: at least one rule must return true to allow
- For default-allow: the first rule returning true denies
- Always check `identity.oidc != null` before accessing OIDC fields
- Use bracket notation for claims: `identity.oidc.claims['claim_name']`

**Retention policy rules:**
- A rule that returns a non-boolean value or fails to evaluate causes the manifest to be retained and emits a `warn`-level log
- Retention is fail-open by design: unexpected rule outcomes keep the manifest rather than deleting it
- See [Configure Retention Policies, Misconfigured Rules](../how-to/configure-retention-policies.md#misconfigured-rules-and-fail-open-semantics) for details and log examples

**Scan and index policy rules:**
- A rule that returns a non-boolean value or fails to evaluate applies the policy to the image, a scan or an index job, and emits a `warn`-level log: a broken rule costs work rather than hiding a finding
