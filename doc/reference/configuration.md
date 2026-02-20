---
displayed_sidebar: reference
sidebar_position: 1
title: "Configuration"
---

# Configuration Reference

Angos is configured via a TOML file (default: `config.toml`). The configuration is automatically reloaded when the file changes.

## Hot Reloading

Most configuration changes take effect immediately without restart. The following options require a restart:

- `server.bind_address`
- `server.port`
- `observability.tracing.sampling_rate`
- Enabling or disabling TLS
- Changing storage backend type (filesystem ↔ S3)

TLS certificate files are also automatically reloaded when they change.

---

## Server (`server`)

| Option                       | Type   | Default  | Description                                        |
|------------------------------|--------|----------|----------------------------------------------------|
| `bind_address`               | string | required | Address to bind (e.g., `"0.0.0.0"`, `"127.0.0.1"`) |
| `port`                       | u16    | `8000`   | Port number                                        |
| `query_timeout`              | u64    | `3600`   | Query timeout in seconds                           |
| `query_timeout_grace_period` | u64    | `60`     | Grace period for queries in seconds                |

### TLS (`server.tls`)

When omitted, the server runs without TLS (insecure).

| Option                      | Type   | Default  | Description                       |
|-----------------------------|--------|----------|-----------------------------------|
| `server_certificate_bundle` | string | required | Path to server certificate (PEM)  |
| `server_private_key`        | string | required | Path to server private key (PEM)  |
| `client_ca_bundle`          | string | -        | Path to client CA bundle for mTLS |

---

## Global Options (`global`)

| Option                      | Type     | Default  | Description                                 |
|-----------------------------|----------|----------|---------------------------------------------|
| `max_concurrent_requests`   | usize    | `64`     | Tokio worker threads (see Performance Tuning) |
| `max_concurrent_cache_jobs` | usize    | `4`      | Maximum concurrent cache jobs               |
| `update_pull_time`          | bool     | `false`  | Track pull times for retention policies     |
| `enable_redirect`           | bool     | `true`   | Allow HTTP 307 redirects for blob downloads |
| `immutable_tags`            | bool     | `false`  | Global immutable tags default               |
| `immutable_tags_exclusions` | [string] | `[]`     | Regex patterns for mutable tags             |
| `authorization_webhook`     | string   | -        | Name of webhook for authorization           |
| `event_webhooks`            | [string] | `[]`     | Event webhook names for all repositories    |

### Global Access Policy (`global.access_policy`)

| Option          | Type     | Default | Description                        |
|-----------------|----------|---------|------------------------------------|
| `default_allow` | bool     | `false` | Default action when no rules match |
| `rules`         | [string] | `[]`    | CEL expressions for access control |

### Global Retention Policy (`global.retention_policy`)

| Option  | Type     | Default | Description                   |
|---------|----------|---------|-------------------------------|
| `rules` | [string] | `[]`    | CEL expressions for retention |

---

## Cache (`cache`)

Token and key cache configuration. Defaults to in-memory (not suitable for multi-replica).

### Redis Cache (`cache.redis`)

| Option       | Type   | Default  | Description                                  |
|--------------|--------|----------|----------------------------------------------|
| `url`        | string | required | Redis URL (e.g., `"redis://localhost:6379"`) |
| `key_prefix` | string | -        | Prefix for cache keys                        |

---

## Blob Storage (`blob_store`)

Choose one: `blob_store.fs` or `blob_store.s3`.

### Filesystem (`blob_store.fs`)

| Option         | Type   | Default  | Description                |
|----------------|--------|----------|----------------------------|
| `root_dir`     | string | required | Directory for blob storage |
| `sync_to_disk` | bool   | `false`  | Force fsync after writes   |

### S3 (`blob_store.s3`)

| Option                           | Type   | Default   | Description                        |
|----------------------------------|--------|-----------|------------------------------------|
| `access_key_id`                  | string | required  | AWS access key ID                  |
| `secret_key`                     | string | required  | AWS secret key                     |
| `endpoint`                       | string | required  | S3 endpoint URL                    |
| `bucket`                         | string | required  | S3 bucket name                     |
| `region`                         | string | required  | AWS region                         |
| `key_prefix`                     | string | -         | Prefix for S3 keys                 |
| `multipart_part_size`            | string | `"50MiB"` | Minimum multipart part size        |
| `multipart_copy_threshold`       | string | `"5GB"`   | Threshold for multipart copy       |
| `multipart_copy_chunk_size`      | string | `"100MB"` | Chunk size for multipart copy      |
| `multipart_copy_jobs`            | usize  | `4`       | Max concurrent multipart copy jobs |
| `max_attempts`                   | u32    | `3`       | Retry attempts for S3 operations   |
| `operation_timeout_secs`         | u64    | `900`     | Total operation timeout            |
| `operation_attempt_timeout_secs` | u64    | `300`     | Per-attempt timeout                |

---

## Metadata Storage (`metadata_store`)

Optional. Defaults to same backend as blob store.

### Filesystem (`metadata_store.fs`)

| Option         | Type   | Default  | Description                                     |
|----------------|--------|----------|-------------------------------------------------|
| `root_dir`     | string | -        | Directory for metadata (defaults to blob store) |
| `sync_to_disk` | bool   | `false`  | Force fsync after writes                        |

### S3 (`metadata_store.s3`)

Same connection options as `blob_store.s3`, plus:

| Option                      | Type | Default | Description                                                                 |
|-----------------------------|------|---------|-----------------------------------------------------------------------------|
| `link_cache_ttl`            | u64  | `30`    | Read-through cache TTL for link metadata, in seconds (0 to disable)         |
| `access_time_debounce_secs` | u64  | `60`    | Buffer access time writes and flush periodically, in seconds (0 to disable) |

The link cache reduces S3 round-trips for repeated tag/layer reads. The access time debounce batches `last_pulled_at` timestamp writes in memory and flushes them periodically, reducing the critical-path operations per manifest pull from 4 (lock, read, write, unlock) to 1 (read).

### Distributed Locking (`metadata_store.*.redis`)

Required for multi-replica deployments.

| Option           | Type   | Default  | Description                  |
|------------------|--------|----------|------------------------------|
| `url`            | string | required | Redis URL                    |
| `ttl`            | usize  | required | Lock TTL in seconds          |
| `key_prefix`     | string | -        | Prefix for lock keys         |
| `max_retries`    | u32    | `100`    | Max lock acquisition retries |
| `retry_delay_ms` | u64    | `10`     | Delay between retries        |

---

## Authentication (`auth`)

### Basic Auth (`auth.identity.<name>`)

| Option     | Type   | Default  | Description          |
|------------|--------|----------|----------------------|
| `username` | string | required | Username             |
| `password` | string | required | Argon2 password hash |

### OIDC (`auth.oidc.<name>`)

#### GitHub Provider

| Option                  | Type   | Default                                                          | Description                     |
|-------------------------|--------|------------------------------------------------------------------|---------------------------------|
| `provider`              | string | required                                                         | Must be `"github"`              |
| `issuer`                | string | `"https://token.actions.githubusercontent.com"`                  | Issuer URL                      |
| `jwks_uri`              | string | `"https://token.actions.githubusercontent.com/.well-known/jwks"` | JWKS URI                        |
| `jwks_refresh_interval` | u64    | `3600`                                                           | JWKS refresh interval (seconds) |
| `required_audience`     | string | -                                                                | Required audience claim         |
| `clock_skew_tolerance`  | u64    | `60`                                                             | Clock skew tolerance (seconds)  |

#### Generic Provider

| Option                  | Type   | Default  | Description                                  |
|-------------------------|--------|----------|----------------------------------------------|
| `provider`              | string | required | Must be `"generic"`                          |
| `issuer`                | string | required | OIDC issuer URL                              |
| `jwks_uri`              | string | -        | Custom JWKS URI (auto-discovered if not set) |
| `jwks_refresh_interval` | u64    | `3600`   | JWKS refresh interval (seconds)              |
| `required_audience`     | string | -        | Required audience claim                      |
| `clock_skew_tolerance`  | u64    | `60`     | Clock skew tolerance (seconds)               |

### Webhooks (`auth.webhook.<name>`)

| Option                      | Type     | Default  | Description                            |
|-----------------------------|----------|----------|----------------------------------------|
| `url`                       | string   | required | Webhook URL                            |
| `timeout_ms`                | u64      | required | Request timeout in milliseconds        |
| `bearer_token`              | string   | -        | Bearer token for authentication        |
| `basic_auth.username`       | string   | -        | Basic auth username                    |
| `basic_auth.password`       | string   | -        | Basic auth password                    |
| `client_certificate_bundle` | string   | -        | Client cert for mTLS                   |
| `client_private_key`        | string   | -        | Client key for mTLS                    |
| `server_ca_bundle`          | string   | -        | CA bundle for server verification      |
| `forward_headers`           | [string] | `[]`     | Headers to forward from client         |
| `cache_ttl`                 | u64      | `60`     | Response cache duration (0 to disable) |

---

## Repository (`repository."<namespace>"`)

| Option                      | Type     | Default  | Description                     |
|-----------------------------|----------|----------|---------------------------------|
| `immutable_tags`            | bool     | inherits | Override global immutable tags  |
| `immutable_tags_exclusions` | [string] | inherits | Override global exclusions      |
| `authorization_webhook`     | string   | inherits | Webhook name (empty to disable) |
| `event_webhooks`            | [string] | inherits | Event webhook names              |

### Upstream (`repository."<namespace>".upstream`)

Array of upstream registries for pull-through cache.

| Option               | Type   | Default  | Description                       |
|----------------------|--------|----------|-----------------------------------|
| `url`                | string | required | Upstream registry URL             |
| `max_redirect`       | u8     | `5`      | Maximum redirects to follow       |
| `server_ca_bundle`   | string | -        | CA bundle for server verification |
| `client_certificate` | string | -        | Client certificate for mTLS       |
| `client_private_key` | string | -        | Client key for mTLS               |
| `username`           | string | -        | Basic auth username               |
| `password`           | string | -        | Basic auth password               |

### Access Policy (`repository."<namespace>".access_policy`)

Same as `global.access_policy`.

### Retention Policy (`repository."<namespace>".retention_policy`)

Same as `global.retention_policy`.

---

## Event Webhooks (`event_webhook.<name>`)

HTTP POST notifications for registry operations. See [Event Webhooks Reference](event-webhooks.md) for full details.

| Option              | Type     | Default  | Description                                      |
|---------------------|----------|----------|--------------------------------------------------|
| `url`               | string   | required | HTTP/HTTPS endpoint URL                          |
| `policy`            | string   | required | Delivery policy: `required`, `optional`, `async` |
| `events`            | [string] | required | Event types to deliver (at least one)            |
| `token`             | string   | -        | Bearer token and HMAC signing secret             |
| `timeout_ms`        | u64      | `5000`   | HTTP request timeout in milliseconds             |
| `max_retries`       | u32      | `0`      | Maximum retry attempts after initial failure     |
| `repository_filter` | [string] | -        | Regex patterns to match repository names         |

Webhooks are enabled by referencing their names:

| Location                   | Option           | Type     | Description                        |
|----------------------------|------------------|----------|------------------------------------|
| `global`                   | `event_webhooks` | [string] | Webhook names for all repositories |
| `repository."<namespace>"` | `event_webhooks` | [string] | Webhook names for this repository  |

---

## Observability

### Tracing (`observability.tracing`)

| Option          | Type   | Default  | Description               |
|-----------------|--------|----------|---------------------------|
| `endpoint`      | string | required | OpenTelemetry endpoint    |
| `sampling_rate` | f64    | required | Sampling rate (0.0 - 1.0) |

---

## Web UI (`ui`)

| Option    | Type   | Default   | Description                |
|-----------|--------|-----------|----------------------------|
| `enabled` | bool   | `false`   | Enable web interface       |
| `name`    | string | `"angos"` | Registry name in UI header |

---

## Performance Tuning

### max_concurrent_requests

Controls the number of Tokio worker threads handling HTTP requests. Default: `64`.

Registry operations are likely I/O-bound (network transfers, storage I/O), so more threads than CPU cores typically improves throughput.

**Rule of thumb:** Start with 8-16x your CPU core count and adjust based on monitoring.

---

## Example Configuration

```toml
[server]
bind_address = "0.0.0.0"
port = 5000

[server.tls]
server_certificate_bundle = "/tls/server.crt"
server_private_key = "/tls/server.key"

[global]
update_pull_time = true
immutable_tags = true
immutable_tags_exclusions = ["^latest$"]

[blob_store.fs]
root_dir = "/var/registry/blobs"

[metadata_store.fs]
root_dir = "/var/registry/metadata"

[metadata_store.fs.redis]
url = "redis://localhost:6379"
ttl = 10

[cache.redis]
url = "redis://localhost:6379"

[auth.identity.admin]
username = "admin"
password = "$argon2id$v=19$m=19456,t=2,p=1$..."

[auth.oidc.github-actions]
provider = "github"

[global.access_policy]
default_allow = false
rules = ["identity.username != ''"]

[repository."docker-io"]
[[repository."docker-io".upstream]]
url = "https://registry-1.docker.io"

[ui]
enabled = true
name = "My Registry"
```
