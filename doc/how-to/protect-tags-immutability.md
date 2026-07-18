---
displayed_sidebar: howto
sidebar_position: 8
title: "Immutable Tags"
---

# Protect Tags with Immutability

Configure immutable tags to prevent overwrites and ensure deployment reproducibility.

## Prerequisites

- Angos running

## Basic Configuration

### Enable Globally

```toml
[global]
immutable_tags = true
```

All tags are now immutable across all repositories.

### Allow Specific Tags to Change

Use exclusion patterns for mutable tags:

```toml
[global]
immutable_tags = true
immutable_tags_exclusions = [
  "^latest$",      # Allow 'latest' to be updated
  "^develop$",     # Allow 'develop' branch tag
  "^main$"         # Allow 'main' branch tag
]
```

---

## Repository-Specific Settings

A repository can add immutability on top of the global setting, but cannot remove it: a tag is immutable when the global flag or the repository flag is true. To keep some repositories mutable, leave the global flag off and enable immutability per repository.

Per-repository `immutable_tags_exclusions` replace the global list when non-empty; otherwise the global list applies.

```toml
# Global default: all tags mutable
[global]
immutable_tags = false

# Development repositories inherit the global default: all tags mutable

# Production: immutable, only 'latest' may change
[repository."production"]
immutable_tags = true
immutable_tags_exclusions = ["^latest$"]

# Staging: immutable, more mutable tags
[repository."staging"]
immutable_tags = true
immutable_tags_exclusions = [
  "^latest$",
  "^staging-.*$",
  "^rc-.*$"
]
```

---

## Exclusion Patterns

Patterns are regular expressions matching the full tag:

| Pattern | Matches |
|---------|---------|
| `^latest$` | Only "latest" |
| `^v\d+\.\d+$` | "v1.0", "v2.3" (minor versions) |
| `^pr-\d+$` | "pr-123", "pr-456" |
| `^nightly-.*$` | "nightly-2024-01-15" |
| `^dev-.*$` | "dev-feature-x" |
| `^.*-SNAPSHOT$` | "1.0-SNAPSHOT" |

---

## Common Patterns

### Protect Semver, Allow Pre-release

```toml
immutable_tags = true
immutable_tags_exclusions = [
  "^latest$",
  "^.*-alpha.*$",
  "^.*-beta.*$",
  "^.*-rc.*$",
  "^.*-SNAPSHOT$"
]
```

### Protect Release Tags Only

The pattern engine does not support negative lookahead, so "everything except vX.Y.Z" cannot be written as a single exclusion. List the mutable tag conventions explicitly instead:

```toml
immutable_tags = true
immutable_tags_exclusions = [
  "^(?:latest|main|develop)$",   # Branch tags stay mutable
  "^(?:dev|feature|nightly)-.*$" # Work-in-progress tags stay mutable
]
```

Release tags such as `v1.2.3` match no exclusion and stay immutable.

### CI/CD Friendly

```toml
immutable_tags = true
immutable_tags_exclusions = [
  "^latest$",
  "^main$",
  "^develop$",
  "^pr-\\d+$",
  "^sha-[a-f0-9]+$"
]
```

---

## Pull-Through Cache Optimization

Immutable tags improve pull-through cache performance:

```toml
[repository."library"]
immutable_tags = true
immutable_tags_exclusions = ["^latest$", "^nightly.*$"]

[[repository."library".upstream]]
url = "https://registry-1.docker.io"
```

Benefits:
- **Immutable tags** (e.g., `nginx:1.25.0`): Served from cache without upstream checks
- **Mutable tags** (e.g., `nginx:latest`): Check upstream for updates

---

## Error Handling

When attempting to overwrite an immutable tag:

**HTTP Response:**
- Status: `409 Conflict`
- Code: `CONFLICT`
- Message: `Tag 'tagname' is immutable and cannot be overwritten`

**Docker CLI:**
```
$ docker push registry.local/myapp:v1.0.0
The push refers to repository [registry.local/myapp]
171a26c7bc56: Layer already exists
unknown: Tag 'v1.0.0' is immutable and cannot be overwritten
```

---

## Verification

### Test Immutability

```bash
# First push succeeds
docker tag alpine:latest localhost:8000/test/image:v1.0
docker push localhost:8000/test/image:v1.0

# Second push with different content fails
docker tag busybox:latest localhost:8000/test/image:v1.0
docker push localhost:8000/test/image:v1.0
# Error: Tag 'v1.0' is immutable and cannot be overwritten

# Excluded tags can be overwritten
docker push localhost:8000/test/image:latest  # Succeeds
docker push localhost:8000/test/image:latest  # Succeeds again
```

### Check Current Configuration

```bash
curl http://localhost:8000/_ext/_repositories | jq
```

Response includes `immutable_tags: true/false` per repository.

---

## Troubleshooting

**Can't overwrite expected mutable tag:**
- Check exclusion patterns match the tag
- Non-empty repository exclusions replace the global list
- A repository cannot disable a global `immutable_tags = true`
- Patterns are regex, escape special characters

**Immutability not enforced:**
- Verify `immutable_tags = true` is set
- Check repository-specific overrides

## Reference

- [Configuration Reference](../reference/configuration.md) - Immutability options
- [Pull-Through Caching](../explanation/pull-through-caching.md) - Cache optimization
