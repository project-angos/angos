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
- Changing lock strategy

TLS certificate files are also automatically reloaded when they change.

---

## Server (`server`)

| Option                            | Type         | Default  | Description                                        |
|-----------------------------------|--------------|----------|----------------------------------------------------|
| `bind_address`                    | string       | required | Address to bind (e.g., `"0.0.0.0"`, `"127.0.0.1"`) |
| `port`                            | u16          | `8000`   | Port number                                        |
| `query_timeout`                   | non-zero u64 | `3600`   | Query timeout in seconds                           |
| `query_timeout_grace_period`      | non-zero u64 | `60`     | Grace period for queries in seconds                |

Timeout values must be greater than zero.

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
| `max_concurrent_cache_jobs` | usize    | `4`      | Maximum concurrent cache jobs (minimum `1`). With `[global.job_queue]` enabled, also bounds the number of jobs each `angos worker` processes in parallel. |
| `max_concurrent_replication_jobs` | non-zero usize | `4` | Concurrency for replication jobs (minimum `1`). Bounds how many replication pushes are handled in parallel by each `angos worker`, the server's in-process drain, and the `scrub --replicate` end-of-run drain. |
| `max_manifest_size`         | string   | `"5MiB"` | Maximum manifest body size accepted from clients or upstream registries |
| `update_pull_time`          | bool     | `false`  | Track pull times for retention policies     |
| `enable_redirect`           | bool     | -        | **Deprecated.** Fallback for both fields below when unset. |
| `enable_blob_redirect`      | bool     | `true`   | Allow HTTP 307 redirects for blob downloads. |
| `enable_manifest_redirect`  | bool     | `true`   | Allow HTTP 307 redirects for manifest downloads. Manifest bodies served via `response-content-type` to preserve the media type across redirects. |
| `immutable_tags`            | bool     | `false`  | Global immutable tags default               |
| `immutable_tags_exclusions` | [string] | `[]`     | Regex patterns for mutable tags             |
| `allow_missing_manifest_references` | bool | `true` | When `true` (default), accept a manifest push whose referenced blobs or child manifests are not yet present/owned in the namespace (pre-1.2.0 behavior). Set to `false` to reject them with `MANIFEST_BLOB_UNKNOWN`. See note below. |
| `authorization_webhook`     | string   | -        | Name of webhook for authorization           |
| `event_webhooks`            | [string] | `[]`     | Event webhook names for all repositories    |

`max_manifest_size` must be greater than zero.

#### `allow_missing_manifest_references`

This controls whether the live manifest-push path enforces the OCI distribution-spec *option* of rejecting a manifest whose descriptors reference content the registry does not have.

- **`true` (default).** A push is accepted even if a referenced config, layer, or child manifest is absent from or not owned by the target namespace. This maximizes compatibility with clients such as `docker buildx`/`bake`, which push multi-manifest image indexes and provenance/SBOM attestations whose children are not always namespace-local at validation time.
- **`false`.** A push whose references are missing is rejected with `MANIFEST_BLOB_UNKNOWN` (HTTP 404). This is stricter and conformance-oriented, and additionally prevents a caller from minting a namespace-local reference to a blob digest it never uploaded (a cross-namespace read it could not otherwise perform). Inbound replicated manifest pushes are validated the same way; angos-to-angos replication pushes a manifest's children and blobs before the manifest itself, so it stays within strict mode.

`subject` references (referrers) are always accepted regardless of this setting, per the spec. Pull-through cache-fill writes are never validated.

### Durable Job Queue (`global.job_queue`)

Optional. Controls where the job queue is drained. When absent (default),
`angos server` drains the queue itself in-process. When present, `angos server`
enqueues jobs and publishes the queue-depth gauge on `/metrics`, and one or
more `angos worker` processes drain it. Either way, jobs persist under the
`[metadata_store]` backend's `_jobs/` prefix and survive restarts.

The queue does not have its own storage backend. Durable jobs are written to
the **same backend configured for `[metadata_store]`** (filesystem or S3,
whichever metadata uses), under a hardcoded top-level `_jobs/` prefix; the lock
strategy is likewise inherited from `[metadata_store]`. There is therefore no
job-queue-level backend, credential, prefix, or lock-strategy setting: the
section accepts only the two tunables below.

> **A shared lock strategy is required.** The durable queue is drained by
> separate processes, so the per-job execution lock must be shared across them.
> The default in-process `memory` lock cannot coordinate across processes, so
> `[global.job_queue]` combined with a `memory` lock strategy is **rejected at
> startup**. Configure the metadata store with `[metadata_store.s3.lock_strategy.s3]`
> (S3 CAS) or `[metadata_store.s3.lock_strategy.redis]` / `[metadata_store.fs.lock_strategy.redis]`
> (Redis), or omit `[global.job_queue]` to use
> the single-process in-process queue.

| Option | Type | Default | Description |
|---|---|---|---|
| `pending_refresh_interval_secs` | u64 | `15` | How often the server refreshes the `angos_job_queue_pending` gauge. Must be at least `5` (sub-5s ticks induce LIST storms on S3). |
| `pending_ready_horizon_secs` | u64 | `600` | Readiness horizon for the `angos_job_queue_pending` gauge. Only envelopes whose `not_before` falls within `[..., now + horizon]` are counted. Set comfortably larger than your worker pod startup time so KEDA has lead time to scale up before the work becomes claimable. |

> **Per-`lock_key` execution TTL is governed by the lock backend.** A worker that claims a job holds the lock configured under `[metadata_store]` for the duration of execution; the TTL on the lock object (`[metadata_store.fs.lock_strategy.redis].ttl`, `[metadata_store.s3.lock_strategy.s3].ttl_secs`) is what bounds how long another worker has to wait if the holder dies mid-job. Transient heartbeat failures (connect or refresh errors) tolerate a small budget (roughly one TTL of slack) before cancelling the job, so a brief network blip does not waste in-progress work; authoritative signals (ownership loss, max-hold expiry, missing lock object) cancel immediately.

See [Enable Durable Cache Jobs](../how-to/durable-cache-jobs.md) for a full
setup guide including `angos worker` invocation and KEDA autoscaling.

### Global Access Policy (`global.access_policy`)

| Option          | Type     | Default | Description                        |
|-----------------|----------|---------|------------------------------------|
| `default`       | string   | `"deny"` | Default action when no rules match (`"allow"` or `"deny"`). The legacy `default_allow` boolean is still accepted but emits a deprecation warning. |
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
| `multipart_copy_threshold`       | string | `"5GB"`   | Blob size above which S3 upload completion uses multipart copy |
| `multipart_copy_chunk_size`      | string | `"100MB"` | Server-side part size for multipart copy |
| `multipart_copy_jobs`            | usize  | `4`       | Max concurrent multipart copy jobs |
| `multipart_uniform_parts`        | bool   | `false`   | Use uniform multipart upload mode  |
| `max_attempts`                   | u32    | `3`       | Retry attempts for S3 operations   |
| `operation_timeout_secs`         | u64    | `900`     | Total operation timeout            |
| `operation_attempt_timeout_secs` | u64    | `300`     | Per-attempt timeout                |

#### S3 Blob Upload Modes

The registry supports two modes for uploading blobs to S3, controlled by `multipart_uniform_parts`:

**Non-uniform mode (default, `multipart_uniform_parts = false`)**

Each OCI `PATCH` request streams directly into a long-lived S3 multipart upload as an `UploadPart` with known `Content-Length`. Parts are uploaded directly, with no intermediate objects or assembly phase. When the client completes the upload with a `PUT` request, the multipart upload is finalized and the blob is copied to its content-addressed path. This mode works with most S3-compatible providers.

Memory usage per upload: ~8 KiB during each `PATCH` (a single streaming read frame). No data is buffered in memory beyond the current frame.

**Uniform mode (`multipart_uniform_parts = true`)**

A long-lived S3 multipart upload is maintained across all `PATCH` requests. Each committed part is exactly `multipart_part_size` bytes (except the last). The S3 protocol only requires non-final parts to be ≥ 5 MiB; uniform sizing is an additional constraint imposed by some S3 storage providers. Use this mode only if your provider rejects uploads with variable part sizes.

Memory usage per upload: streaming read frames for full parts, plus at most one trailing staged chunk smaller than `multipart_part_size`.

```toml
# Most S3 providers (AWS S3, MinIO, Exoscale, etc.)
[blob_store.s3]
multipart_uniform_parts = false  # Default

# Strict S3 providers (if non-uniform mode fails)
[blob_store.s3]
multipart_uniform_parts = true
```

---

## Metadata Storage (`metadata_store`)

Optional. Defaults to same backend as blob store.

### Lock Strategy Compatibility

The following table shows which lock strategies are supported with each metadata store backend:

| Lock Strategy | S3 metadata store | FS metadata store |
|---------------|-------------------|-------------------|
| memory        | Yes               | Yes               |
| redis         | Yes               | Yes               |
| s3            | Yes               | No                |

### Filesystem (`metadata_store.fs`)

| Option         | Type         | Default    | Description                                     |
|----------------|--------------|------------|-----------------------------------------------|
| `root_dir`     | string       | -          | Directory for metadata (defaults to blob store) |
| `sync_to_disk` | bool         | `false`    | Force fsync after writes                        |
| `lock_strategy` | string/table | `"memory"` | Lock backend: `"memory"` (string), or `[lock_strategy.redis]` (table form). S3 locking not supported. |

> **Note:** The S3 lock strategy is not supported for filesystem metadata stores. Use `"memory"` for single-instance deployments or `[lock_strategy.redis]` for multi-replica deployments.

### S3 (`metadata_store.s3`)

Same connection options as `blob_store.s3`, plus:

| Option                      | Type         | Default    | Description                                                                 |
|-----------------------------|--------------|------------|-----------------------------------------------------------------------------|
| `link_cache_ttl`            | u64          | `30`       | Read-through cache TTL for link metadata, in seconds (0 to disable)         |
| `access_time_debounce_secs` | u64          | `60`       | Buffer access time writes and flush periodically, in seconds (0 to disable) |
| `lock_strategy`             | string/table | `"memory"` | Lock backend: `"memory"` (string), or `[lock_strategy.s3]`/`[lock_strategy.redis]` (table form, see below) |
| `capabilities`              | table        | -          | Optional S3 conditional operation capabilities; see below                    |

The link cache reduces S3 round-trips for repeated tag/layer reads. The access time debounce batches `last_pulled_at` timestamp writes in memory and flushes them periodically, reducing the critical-path operations per manifest pull from 4 (lock, read, write, unlock) to 1 (read).

#### Conditional Capabilities (`metadata_store.s3.capabilities`)

When using S3 as the metadata store, you can declare which conditional write operations your S3-compatible provider supports. This avoids a startup probe and allows Angos to use optimistic blob-index updates when the provider supports them.

| Option              | Type | Default | Description                                                                                                 |
|---------------------|------|---------|-------------------------------------------------------------------------------------------------------------|
| `put_if_none_match` | bool | -       | S3 supports `PutObject` with `If-None-Match: *` (create-only, reject if object exists)                     |
| `put_if_match`      | bool | -       | S3 supports `PutObject` with `If-Match: <etag>` (update-only, reject if ETag mismatch)                     |
| `delete_if_match`   | bool | -       | S3 supports `DeleteObject` with `If-Match: <etag>` (conditional delete, reject if ETag mismatch)          |

**Probe behavior:**
- The probe runs for S3 metadata storage when `capabilities` is omitted.
- When `capabilities` is explicitly configured for an `s3` lock strategy, the startup probe is skipped. Declared values are validated against the lock strategy's requirements (`put_if_none_match` and `put_if_match`).
- When `capabilities` is omitted, the probe runs at startup (and on each config reload that has no cached value); probed values are cached for subsequent reloads.
- With `lock_strategy = "memory"` or `"redis"`, detected conditional writes are used only for blob-index shard updates. Link updates still use the configured lock backend.
- To avoid S3 CAS entirely with `lock_strategy = "memory"` or `"redis"`, explicitly set all three capability flags to `false`.

**Example with explicit capabilities (AWS S3):**
```toml
[metadata_store.s3.capabilities]
put_if_none_match = true
put_if_match = true
delete_if_match = true
```

**Example disabling S3 CAS with memory locking:**
```toml
[metadata_store.s3]
lock_strategy = "memory"

[metadata_store.s3.capabilities]
put_if_none_match = false
put_if_match = false
delete_if_match = false
```

**Example with auto-probe:**
```toml
[metadata_store.s3]
# No capabilities field: probe runs at startup to detect provider support
```

**Performance impact:**
- `lock_strategy` selects the coordinator: `"s3"` selects the CAS coordinator (which uses S3 conditional writes for all coordination); `"redis"` and `"memory"` select the lock coordinator with the corresponding lock backend.
- The CAS coordinator requires `put_if_none_match` and `put_if_match` from the provider; startup fails if either is missing under `lock_strategy = "s3"`.
- `delete_if_match` is optional within the CAS coordinator. When available, its internal S3 lock uses race-free conditional release. When absent, release falls back to plain delete (functional but race-prone under contention).
- Under `lock_strategy = "memory"` or `"redis"`, `put_if_none_match` and `put_if_match` let Angos update blob-index shards with optimistic concurrency while the lock coordinator still protects link metadata.
- Declaring all capability flags as `false` keeps blob-index updates on the configured lock backend. This is valid for `lock_strategy = "memory"` or `"redis"`, but not for `lock_strategy = "s3"`.

> **Warning:** Setting `access_time_debounce_secs = 0` with S3 lock strategy causes every manifest pull to perform a full lock-acquire → read → write → release cycle via S3 API. At scale with many concurrent pulls, this adds significant latency and S3 API costs. Keep the default value of 60 or higher for S3-locked deployments, or disable access time tracking entirely if not needed for retention policies.

### Distributed Locking

Multi-replica deployments require a distributed lock backend. The `lock_strategy` field on the metadata store selects the backend. Three options are available:

**Lock Strategy Compatibility Matrix:**

| Lock Strategy | S3 metadata | FS metadata |
|---|---|---|
| memory | Yes | Yes |
| redis | Yes | Yes |
| s3 | Yes | No |

> **Note:** `lock_strategy = "s3"` selects the CAS-based coordinator and requires the provider to support `put_if_none_match` and `put_if_match`; startup fails fast if either is missing. `lock_strategy = "memory"` and `"redis"` select the lock coordinator, but Angos still uses conditional writes for blob-index shard updates when available.

**Memory** (default) uses in-process locks, suitable for single-instance deployments only:

```toml
[metadata_store.s3]
lock_strategy = "memory"
```

**S3** uses S3 conditional writes (`If-None-Match: *`) for distributed locking without extra infrastructure. The S3 provider must support `put_if_none_match` and `put_if_match`; angos verifies this at startup and fails fast if either is missing. `delete_if_match` is optional: when absent, lock release falls back to plain delete (functional but race-prone under contention):

```toml
# With defaults (empty table body; all fields use defaults)
[metadata_store.s3.lock_strategy.s3]

# With custom settings
[metadata_store.s3.lock_strategy.s3]
ttl_secs = 30
max_retries = 100
retry_delay_ms = 50
```

> **Note:** The bare-string form `lock_strategy = "s3"` is not supported; use the table form `[metadata_store.s3.lock_strategy.s3]` to accept defaults or override individual fields.

| Option                          | Type | Default | Description                  |
|---------------------------------|------|---------|------------------------------|
| `ttl_secs`                      | u64  | `30`    | Lock TTL in seconds (minimum: 9). Heartbeat renews at intervals of `ttl_secs / 3` |
| `max_retries`                   | u32  | `100`   | Max lock acquisition retries |
| `retry_delay_ms`                | u64  | `50`    | Delay between retries (minimum: 1) |
| `max_hold_secs`                 | u64  | `300`   | Maximum lock hold duration in seconds (minimum: 10, must be >= `ttl_secs`). Guard is invalidated if held beyond this duration |
| `operation_timeout_secs`        | u64  | `15`    | Total timeout for lock S3 operations |
| `operation_attempt_timeout_secs`| u64  | `4`     | Per-attempt timeout for lock S3 operations |
| `max_attempts`                  | u32  | `2`     | Maximum retry attempts for lock S3 operations |

> **Lock operation timeouts:** Lock operations use their own S3 client with significantly tighter timeouts than blob/metadata operations. This is intentional: lock operations are small JSON payloads and should fail fast rather than blocking for minutes on a stuck request. The defaults (`operation_timeout_secs = 15`, `operation_attempt_timeout_secs = 4`, `max_attempts = 2`) ensure that a single stuck request cannot consume an entire heartbeat interval (10s with default TTL). Each heartbeat tick is also capped to the heartbeat interval to prevent the slow path (two sequential SDK calls) from exceeding it. A startup warning is emitted if `operation_attempt_timeout_secs × max_attempts >= ttl_secs / 3`. For high-latency S3 scenarios, increase these values but keep `attempt_timeout × max_attempts` below the heartbeat interval.

**Heartbeat Mechanism:**

The S3 lock implementation uses a heartbeat to keep locks alive. Once acquired, a background task automatically renews the lock at regular intervals of `ttl_secs / 3`. For example, with the default `ttl_secs = 30`, the heartbeat runs every 10 seconds. This allows the lock to remain valid beyond the initial TTL as long as the lock-holder remains alive. If a lock-holder crashes, other instances must wait for the full `ttl_secs` duration before the lock becomes available for recovery.

Transient heartbeat failures (connect errors, refresh timeouts, network blips) accumulate up to a small budget (roughly one TTL of slack) before cancelling the in-flight operation. Authoritative signals cancel immediately: S3 reports `ownership_lost`, `etag_unavailable`, `file_disappeared`, or `max_hold_exceeded`; Redis reports `ownership_lost` (refresh script detected the key was overwritten). When the budget is exhausted, the heartbeat emits `heartbeat_failure` for both backends to flag the cancellation as transient-failure-driven rather than authoritative.

Locks are released as part of the operation flow: a successful operation releases its lock before returning. If the surrounding request or task is cancelled mid-operation, a best-effort background release fires on the current Tokio runtime so the remote lock is freed promptly without waiting on TTL. The fallback applies only when a runtime is still available; during process shutdown the lock expires via `ttl_secs`.

> **Contention note:** The first lock acquisition attempt uses parallel PUTs for low latency. If any key is contended, the system falls back to sequential sorted acquisition for all subsequent retries, which eliminates circular wait and prevents livelock. When CAS blob index updates are active (S3 lock strategy), blob digest keys are excluded from locking, avoiding cross-namespace contention on shared layers. Randomized jitter on retry delays desynchronises retrying instances.

> **Clock synchronisation:** The lock implementation uses S3's server-side timestamps for expiry checks, so lock correctness does not depend on synchronised instance clocks. Registry instances should still maintain synchronised clocks (NTP) for logging and other operational reasons.

**Redis** provides distributed locking via Redis, suitable for multi-instance deployments:

```toml
[metadata_store.s3.lock_strategy.redis]
url = "redis://localhost:6379"
ttl = 10
```

| Option           | Type   | Default  | Description                  |
|------------------|--------|----------|------------------------------|
| `url`            | string | required | Redis URL                    |
| `ttl`            | usize  | required | Lock TTL in seconds          |
| `key_prefix`     | string | -        | Prefix for lock keys         |
| `max_retries`    | u32    | `100`    | Max lock acquisition retries |
| `retry_delay_ms` | u64    | `10`     | Initial retry delay in milliseconds. Retries use exponential backoff capped at 1s, plus jitter |

---

## Authentication (`auth`)

### Basic Auth (`auth.identity.<name>`)

| Option     | Type   | Default  | Description          |
|------------|--------|----------|----------------------|
| `username` | string | required | Username             |
| `password` | string | required | Argon2 password hash |

Password hashes are validated when the configuration is parsed. An invalid Argon2 hash causes the server to fail to start with a clear error. Use `angos argon` to generate a valid hash.

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
| `allowed_algorithms`    | array  | `["RS256"]`                                                       | Allowed JWT signing algorithms  |

#### Generic Provider

| Option                  | Type   | Default    | Description                                  |
|-------------------------|--------|------------|----------------------------------------------|
| `provider`              | string | required   | Must be `"generic"`                          |
| `issuer`                | string | required   | OIDC issuer URL                              |
| `jwks_uri`              | string | -          | Custom JWKS URI (auto-discovered if not set) |
| `jwks_refresh_interval` | u64    | `3600`     | JWKS refresh interval (seconds)              |
| `required_audience`     | string | -          | Required audience claim                      |
| `clock_skew_tolerance`  | u64    | `60`       | Clock skew tolerance (seconds)               |
| `allowed_algorithms`    | array  | `["RS256"]` | Allowed JWT signing algorithms              |

`allowed_algorithms` accepts JWT algorithm names such as `"RS256"`, `"RS384"`, `"RS512"`, `"ES256"`, and `"ES384"`. Angos rejects tokens whose header claims an algorithm outside the provider allowlist before signature verification to prevent algorithm-confusion attacks.

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

`url` and `forward_headers` are validated when the configuration is loaded.
If either `client_certificate_bundle` or `client_private_key` is set, both
must be set.

---

## Repository (`repository."<namespace>"`)

Repository namespace keys must not overlap: a key like `team` and a key like `team/app` are considered overlapping because one is a namespace-prefix of the other. The registry rejects this configuration at startup.

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

### Downstream (`repository."<namespace>".downstream`)

Array of downstream registries to which this repository's mutations are replicated. See [Configure Replication](../how-to/configure-replication.md).

| Option                  | Type     | Default            | Description                                                              |
|-------------------------|----------|--------------------|--------------------------------------------------------------------------|
| `name`                  | string   | required           | Local identifier for this downstream (logs, `downstream` metric label)   |
| `url`                   | string   | required           | Downstream registry base URL                                             |
| `mode`                  | string   | `"event+reconcile"` | `"event+reconcile"`, `"event-only"`, or `"reconcile-only"`              |
| `namespace_filter`      | [string] | `[]` (all)         | Regex patterns; a namespace replicates here only if it matches one       |
| `max_concurrent_pushes` | usize    | `4`                | Concurrent blob pushes per manifest for this downstream (positive integer, >= 1) |
| `prune`                 | bool     | `false`            | When `true`, reconciliation also deletes downstream-only tags (authoritative one-way mirror; unsafe for active-active peers) |
| `max_redirect`          | u8       | `5`                | Maximum redirects to follow                                              |
| `username`              | string   | -                  | Basic auth username                                                      |
| `password`              | string   | -                  | Basic auth password                                                      |
| `server_ca_bundle`      | string   | -                  | CA bundle for downstream TLS verification                               |
| `client_certificate`    | string   | -                  | Client certificate for mTLS (requires `client_private_key`)             |
| `client_private_key`    | string   | -                  | Client key for mTLS (requires `client_certificate`)                     |

`mode` values:
- `event+reconcile` (default): push on every local mutation **and** include in `angos scrub --replicate`.
- `event-only`: push on local mutations; excluded from scrub reconciliation.
- `reconcile-only`: excluded from live pushes; mirrored only via `angos scrub --replicate`.

If either `client_certificate` or `client_private_key` is set, both must be set.

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

`url`, `events`, `token`, and `repository_filter` are validated when the
configuration is loaded. If `token` is set, it must not be empty.

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

### Prometheus Metrics

Angos emits Prometheus metrics on the `/metrics` endpoint. The following metrics are available for lock operations:

**Lock Metrics:**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `lock_acquisition_duration_ms` | Histogram | `backend` | Lock acquisition duration in milliseconds |
| `lock_acquisitions_total` | Counter | `backend`, `result` | Total lock acquisition attempts |
| `lock_retries_total` | Counter | `backend` | Total lock acquisition retries |
| `lock_invalidations_total` | Counter | `backend`, `reason` | Total lock invalidations |
| `lock_recoveries_total` | Counter | `backend`, `result` | Total stale lock recovery attempts |

**Label Values:**

- `backend`: `s3`, `redis`, `memory`
- `result` (acquisitions): `success`, `timeout`, `error`
- `result` (recoveries): `acquired`, `not_stale`, `failed`, `error`
- `reason` (invalidations): `ownership_lost`, `max_hold_exceeded`, `heartbeat_failure`, `etag_unavailable`, `file_disappeared` (both S3 and Redis report heartbeat-side failures as `heartbeat_failure`)

---

## Web UI (`ui`)

| Option    | Type   | Default   | Description                |
|-----------|--------|-----------|----------------------------|
| `enabled` | bool   | `false`   | Enable web interface       |
| `name`    | string | `"Angos"` | Registry name in UI header |

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
port = 8000

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

[metadata_store.fs.lock_strategy.redis]
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
default = "deny"
rules = ["identity.username != ''"]

[repository."docker-io"]
[[repository."docker-io".upstream]]
url = "https://registry-1.docker.io"

[ui]
enabled = true
name = "My Registry"
```

### S3-Only Multi-Instance Deployment

This example uses S3 for both blob and metadata storage with S3-based distributed locking, eliminating the need for Redis:

```toml
[server]
bind_address = "0.0.0.0"
port = 8000

[global]
update_pull_time = true

[blob_store.s3]
# Example credentials - replace for production
access_key_id = "minioadmin"
secret_key = "minioadmin"
endpoint = "https://s3.example.com"
bucket = "registry"
region = "us-east-1"

[metadata_store.s3]
# Example credentials - replace for production
access_key_id = "minioadmin"
secret_key = "minioadmin"
endpoint = "https://s3.example.com"
bucket = "registry-metadata"
region = "us-east-1"

[metadata_store.s3.lock_strategy.s3]

[auth.identity.admin]
username = "admin"
password = "$argon2id$v=19$m=19456,t=2,p=1$..."
```
