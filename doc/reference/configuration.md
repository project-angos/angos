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
- Adding or removing `[global.job_queue]`

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

| Option                      | Type   | Default      | Description                       |
|-----------------------------|--------|--------------|-----------------------------------|
| `server_certificate_bundle` | string | required     | Path to server certificate (PEM)  |
| `server_private_key`        | string | required     | Path to server private key (PEM)  |
| `client_ca_bundle`          | string | -            | Path to client CA bundle for mTLS |
| `client_auth`               | string | `"optional"` | `"optional"` accepts both anonymous clients and clients whose certificate validates against `client_ca_bundle`; `"required"` rejects the TLS handshake without a valid client certificate and needs `client_ca_bundle` set. Ignored when `client_ca_bundle` is unset |

---

## Global Options (`global`)

| Option                      | Type     | Default  | Description                                 |
|-----------------------------|----------|----------|---------------------------------------------|
| `max_concurrent_requests`   | usize    | `64`     | Tokio worker threads (see Performance Tuning) |
| `max_concurrent_cache_jobs` | usize    | `4`      | Maximum concurrent cache jobs (minimum `1`). With `[global.job_queue]` enabled, also bounds the number of jobs each `angos worker` processes in parallel. |
| `max_concurrent_replication_jobs` | non-zero usize | `4` | Concurrency for replication jobs (minimum `1`). Bounds how many replication pushes are handled in parallel by each `angos worker`, the server's in-process drain, and the `angos replicate` end-of-run drain. |
| `max_manifest_size`         | string   | `"5MiB"` | Maximum manifest body size accepted from clients or upstream registries |
| `max_blob_size`             | string   | `"100GiB"` | Maximum total size of a single blob upload; a larger upload is rejected with `BLOB_UPLOAD_INVALID` (HTTP 413) |
| `update_pull_time`          | bool     | `false`  | Track pull times for retention policies     |
| `enable_blob_redirect`      | bool     | `true`   | Allow HTTP 307 redirects for blob downloads. |
| `enable_manifest_redirect`  | bool     | `true`   | Allow HTTP 307 redirects for manifest downloads. Manifest bodies served via `response-content-type` to preserve the media type across redirects. |
| `immutable_tags`            | bool     | `false`  | Global immutable tags default               |
| `immutable_tags_exclusions` | [string] | `[]`     | Regex patterns for mutable tags             |
| `allow_missing_manifest_references` | bool | `true` | When `true` (default), accept a manifest push whose referenced blobs or child manifests are not yet present/owned in the namespace; the missing references stay unreadable until their content is pushed. Set to `false` to reject such pushes with `MANIFEST_BLOB_UNKNOWN`. See note below. |
| `authorization_webhook`     | string   | -        | Name of webhook for authorization           |
| `event_webhooks`            | [string] | `[]`     | Event webhook names for all repositories    |
| `trusted_proxies`           | [string] | `[]`     | Proxy IPs or CIDR networks (e.g. `"10.0.0.1"`, `"10.0.0.0/8"`) whose `X-Forwarded-For`/`X-Real-IP` headers are honored as the client IP. From any other peer those headers are ignored and the socket address is used. |

`max_manifest_size` and `max_blob_size` must be greater than zero.

#### `allow_missing_manifest_references`

This controls whether the live manifest-push path enforces the OCI distribution-spec *option* of rejecting a manifest whose descriptors reference content the registry does not have.

- **`true` (default).** A push is accepted even if a referenced config, layer, or child manifest is absent from or not owned by the target namespace. The unowned references are not granted to the namespace: they resolve as unknown on a later pull (`BLOB_UNKNOWN` for a blob, `MANIFEST_UNKNOWN` for a child manifest) until their content is pushed. This maximizes compatibility with clients such as `docker buildx`/`bake`, which push multi-manifest image indexes and provenance/SBOM attestations whose children are not always namespace-local at validation time.
- **`false`.** A push whose references are missing is rejected outright with `MANIFEST_BLOB_UNKNOWN` (HTTP 404). This is stricter and conformance-oriented.

Either setting preserves namespace isolation: a caller never gains read access to a blob digest it never uploaded. Inbound replicated manifest pushes follow the same rule; angos-to-angos replication pushes a manifest's children and blobs before the manifest itself, so its references are always owned. `subject` references (referrers) are always accepted regardless of this setting, per the spec. Pull-through cache-fill writes are trusted, independent of this flag.

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
> On an S3 metadata store whose provider supports conditional operations this
> holds by default: the unset lock strategy resolves to the shared S3 lock.
> The in-process `memory` lock cannot coordinate across processes, so
> `[global.job_queue]` combined with a `memory` lock strategy is **rejected at
> startup**. On providers without conditional operations and on filesystem
> metadata stores, configure `[metadata_store.s3.lock_strategy.redis]` /
> `[metadata_store.fs.lock_strategy.redis]` (Redis), or omit
> `[global.job_queue]` to use the single-process in-process queue.

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
| `default`       | string   | `"deny"` | Default action when no rules match (`"allow"` or `"deny"`). |
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
| `key_prefix` | string | required | Prefix for cache keys                        |

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

Each OCI `PATCH` request streams into a long-lived S3 multipart upload, with no intermediate objects or assembly phase. When the client completes the upload with a `PUT` request, the multipart upload is finalized and the blob is copied to its content-addressed path. This mode works with most S3-compatible providers.

A `PATCH` that carries a `Content-Length` is uploaded directly as an `UploadPart` with that known length. A chunked `PATCH` (no `Content-Length`, as `docker push` sends) is streamed to EOF. When `multipart_part_size` is above the 5 MiB floor, it is coalesced server-side into `part_size` parts via `UploadPartCopy`, buffering at most one 5 MiB sub-part and restaging the trailing remainder. When `multipart_part_size` is exactly 5 MiB, it streams plain 5 MiB parts directly with no coalescing.

Memory usage per upload: for a known-length `PATCH`, up to one ~1 MiB streaming read frame, with no data buffered beyond the current frame. For a coalesced chunked `PATCH` (`multipart_part_size` above the 5 MiB floor), at most one buffered 5 MiB sub-part, at the cost of moving each byte twice within S3 (into a scratch object, then `UploadPartCopy` into the upload).

**Uniform mode (`multipart_uniform_parts = true`)**

A long-lived S3 multipart upload is maintained across all `PATCH` requests. Both a known-length `PATCH` and a chunked `PATCH` (no `Content-Length`, as `docker push` sends) commit non-final parts of exactly `multipart_part_size` bytes; the final part may be smaller. The S3 protocol only requires non-final parts to be ≥ 5 MiB; uniform sizing is an additional constraint imposed by some S3 storage providers. Use this mode only if your provider rejects uploads with variable part sizes.

Memory usage per upload: streaming read frames for full parts, plus at most one trailing staged chunk smaller than `multipart_part_size`. A chunked `PATCH` buffers up to one `multipart_part_size` part, the same as the known-length remainder.

```toml
# Most S3 providers (AWS S3, Exoscale, etc.)
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
| `access_time_debounce_secs` | u64          | `60`       | Buffer access time writes and flush periodically, in seconds (0 to disable). Lock-coordinated deployments only; ignored with CAS |
| `lock_strategy`             | string/table | see below  | Lock backend: `"memory"` (string), or `[lock_strategy.s3]`/`[lock_strategy.redis]` (table form, see below). Unset: `s3` when the provider supports conditional operations, `memory` otherwise |
| `conditional_operations`        | bool         | -          | Declares provider support for the conditional operations Angos coordinates with; omit to probe at startup (see below) |

The link cache reduces S3 round-trips for repeated tag/layer reads. On lock-coordinated deployments, the access time debounce batches `last_pulled_at` timestamp writes in memory and flushes them periodically, reducing the critical-path operations per manifest pull from 4 (lock, read, write, unlock) to 1 (read). With CAS, every pull stamps the access time inline with one conditional write (a lost race is a no-op) and the debounce setting is ignored.

#### Conditional Operations (`metadata_store.s3.conditional_operations`)

When using S3 as the metadata store, you can declare whether your S3-compatible provider supports the conditional operations Angos coordinates with. The set is all-or-nothing: `PutObject` with `If-None-Match: *`, `PutObject` with `If-Match: <etag>`, and `DeleteObject` with `If-Match: <etag>`. Declaring it avoids a startup probe and, when `true`, enables optimistic (CAS) metadata updates.

**Probe behavior:**
- When `conditional_operations` is omitted, the probe runs at startup (and on each config reload that has no cached value); the probed verdict is cached for subsequent reloads. The probe verifies each operation, including that the provider actually enforces the conditions and surfaces ETags.
- When `conditional_operations` is set, the startup probe is skipped. The declared value is validated against the lock strategy's requirements (`lock_strategy = "s3"` requires `true`).
- When `lock_strategy` is unset, the resolved value also selects the default lock backend: the shared S3 lock when `true`, the in-process `memory` lock when `false`.
- With `lock_strategy = "memory"` or `"redis"`, `conditional_operations = true` is used only for blob-index shard updates. Link updates still use the configured lock backend.
- To avoid S3 CAS entirely, set `conditional_operations = false`; combine with `lock_strategy = "redis"` for multi-replica deployments.

**Example with explicit declaration (AWS S3, Exoscale SOS):**
```toml
[metadata_store.s3]
conditional_operations = true
```

**Example disabling S3 CAS with memory locking:**
```toml
[metadata_store.s3]
lock_strategy = "memory"
conditional_operations = false
```

**Example with auto-probe:**
```toml
[metadata_store.s3]
# No conditional_operations field: probe runs at startup to detect provider support
```

**Performance impact:**
- `lock_strategy` selects the coordinator: `"s3"` selects the CAS coordinator (which uses S3 conditional requests for all coordination); `"redis"` and `"memory"` select the lock coordinator with the corresponding lock backend.
- The CAS coordinator requires the full conditional set from the provider; startup fails if any operation is missing under `lock_strategy = "s3"`. Conditional deletes keep lock release and lock reclaim race-free.
- Under `lock_strategy = "memory"` or `"redis"`, `conditional_operations = true` lets Angos update blob-index shards with optimistic concurrency while the lock coordinator still protects link metadata.
- `conditional_operations = false` keeps blob-index updates on the configured lock backend. This is valid for `lock_strategy = "memory"` or `"redis"`, but not for `lock_strategy = "s3"`.

> **Warning:** On lock-coordinated deployments, setting `access_time_debounce_secs = 0` causes every manifest pull to perform a full lock-acquire → read → write → release cycle. At scale with many concurrent pulls, this adds significant latency and API costs. Keep the default value of 60 or higher there, or disable access time tracking entirely if not needed for retention policies. CAS deployments are unaffected: they ignore the knob and stamp inline with a single conditional write.

### Distributed Locking

Multi-replica deployments require a distributed lock backend. The `lock_strategy` field on the metadata store selects the backend. Three options are available; see [Lock Strategy Compatibility](#lock-strategy-compatibility) for the backend support matrix.

> **Note:** `lock_strategy = "s3"` selects the CAS-based coordinator and requires the provider to support the full conditional set (`If-None-Match` and `If-Match` on PUT, `If-Match` on DELETE); startup fails fast if any is missing. `lock_strategy = "memory"` and `"redis"` select the lock coordinator, but Angos still uses conditional writes for blob-index shard updates when available.

**Memory** uses in-process locks, suitable for single-instance deployments only. It is the default for filesystem metadata stores and for S3 providers without conditional-operation support:

```toml
[metadata_store.s3]
lock_strategy = "memory"
```

**S3** uses S3 conditional requests for distributed locking without extra infrastructure: `If-None-Match: *` to acquire, `If-Match` on PUT to refresh, and `If-Match` on DELETE to release, so an instance can only ever remove its own lock object. It is the default when the provider supports the full conditional set; Angos verifies the set at startup and fails fast if the strategy is selected explicitly and any operation is missing:

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
| `ttl_secs`                      | u64  | `30`    | Lock TTL in seconds (9 to 3600). Heartbeat renews at intervals of `ttl_secs / 3` |
| `max_retries`                   | u32  | `100`   | Max lock acquisition retries |
| `retry_delay_ms`                | u64  | `50`    | Delay between retries (minimum: 1) |
| `max_hold_secs`                 | u64  | `300`   | Maximum lock hold duration in seconds (must be >= `ttl_secs`). Guard is invalidated if held beyond this duration |
| `operation_timeout_secs`        | u64  | `15`    | Total timeout for lock S3 operations |
| `operation_attempt_timeout_secs`| u64  | `4`     | Per-attempt timeout for lock S3 operations |
| `max_attempts`                  | u32  | `2`     | Maximum retry attempts for lock S3 operations |

> **Lock operation timeouts:** Lock operations use their own S3 client with significantly tighter timeouts than blob/metadata operations. This is intentional: lock operations are small JSON payloads and should fail fast rather than blocking for minutes on a stuck request. The defaults (`operation_timeout_secs = 15`, `operation_attempt_timeout_secs = 4`, `max_attempts = 2`) ensure that a single stuck request cannot consume an entire heartbeat interval (10s with default TTL). Each heartbeat tick is also capped to the heartbeat interval to prevent the slow path (two sequential SDK calls) from exceeding it. For high-latency S3 scenarios, increase these values but keep `attempt_timeout × max_attempts` below the heartbeat interval.

**Heartbeat Mechanism:**

The S3 lock implementation uses a heartbeat to keep locks alive. Once acquired, a background task automatically renews the lock at regular intervals of `ttl_secs / 3`. For example, with the default `ttl_secs = 30`, the heartbeat runs every 10 seconds. This allows the lock to remain valid beyond the initial TTL as long as the lock-holder remains alive. If a lock-holder crashes, other instances must wait for the full `ttl_secs` duration before the lock becomes available for recovery.

Transient heartbeat failures (connect errors, refresh timeouts, network blips) accumulate up to a small budget (roughly one TTL of slack) before cancelling the in-flight operation. Authoritative signals cancel immediately: S3 reports `ownership_lost`, `file_disappeared`, or `max_hold_exceeded`; Redis reports `ownership_lost` (refresh script detected the key was overwritten). When the budget is exhausted, the heartbeat emits `heartbeat_failure` for both backends to flag the cancellation as transient-failure-driven rather than authoritative.

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

Usernames must be unique across all `auth.identity` entries; a duplicate causes the server to fail to start.

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
| `http_request_timeout_secs` | u64 | `30`                                                          | Timeout for a JWKS or discovery HTTP fetch (seconds) |
| `jwks_refresh_timeout_secs` | u64 | `5`                                                           | Timeout for the forced JWKS refetch on key rotation (seconds) |

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
| `http_request_timeout_secs` | u64 | `30`     | Timeout for a JWKS or discovery HTTP fetch (seconds) |
| `jwks_refresh_timeout_secs` | u64 | `5`      | Timeout for the forced JWKS refetch on key rotation (seconds) |

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
| `immutable_tags`            | bool     | `false`  | Enable immutable tags for this repository. The effective flag is this value OR `global.immutable_tags`, so a repository can add immutability but never opt out of a global `true` |
| `immutable_tags_exclusions` | [string] | inherits | Replaces the global exclusion list when non-empty |
| `authorization_webhook`     | string   | inherits | Webhook name (empty to disable) |
| `event_webhooks`            | [string] | inherits | Event webhook names              |

### Upstream (`repository."<namespace>".upstream`)

Array of upstream registries for pull-through cache.

| Option               | Type   | Default  | Description                       |
|----------------------|--------|----------|-----------------------------------|
| `url`                | string | required | Upstream registry URL. A bare host pulls from the registry root after stripping the repository name (`<repo>/x` → upstream `x`); a path (`https://host/team`) becomes the upstream namespace prefix instead (`<repo>/x` → upstream `team/x`). Pulling the repository root `<repo>` itself maps to `<repo>` for a bare host and to `<path>` for a path URL. Angos talks to the OCI `/v2/` root, so the path is mapped into the namespace, not the HTTP path. |
| `max_redirect`       | u8     | `5`      | Maximum redirects to follow       |
| `connect_timeout_secs` | u64  | `30`     | Timeout for establishing the connection (TCP + TLS handshake) |
| `read_timeout_secs`  | u64    | `300`    | Per-read inactivity timeout during a transfer; not a whole-transfer cap, so a large blob is never limited by total time |
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
| `url`                   | string   | required           | Downstream registry URL. A bare host (`http://host:8000`) mirrors the namespace verbatim; a path (`http://host:8000/team`) becomes the namespace prefix the content lands under, replacing the source repository prefix (`<repo>/x` → `team/x`). The repository root `<repo>` itself maps to `<repo>` for a bare host and to `<path>` for a path URL. Angos serves the OCI API at the root, so the path is mapped into the namespace, not the HTTP path. |
| `mode`                  | string   | `"event+reconcile"` | `"event+reconcile"`, `"event-only"`, or `"reconcile-only"`              |
| `namespace_filter`      | [string] | `[]` (all)         | Regex patterns; a namespace replicates here only if it matches one       |
| `max_concurrent_pushes` | usize    | `4`                | Concurrent blob pushes per manifest for this downstream (positive integer, >= 1) |
| `prune`                 | bool     | `false`            | When `true`, reconciliation also deletes downstream-only tags (authoritative one-way mirror; unsafe for active-active peers) |
| `max_redirect`          | u8       | `5`                | Maximum redirects to follow                                              |
| `connect_timeout_secs`  | u64      | `30`               | Timeout for establishing the connection (TCP + TLS handshake)            |
| `read_timeout_secs`     | u64      | `300`              | Per-read inactivity timeout during a transfer; not a whole-transfer cap, so a large blob push is never limited by total time |
| `username`              | string   | -                  | Basic auth username                                                      |
| `password`              | string   | -                  | Basic auth password                                                      |
| `server_ca_bundle`      | string   | -                  | CA bundle for downstream TLS verification                               |
| `client_certificate`    | string   | -                  | Client certificate for mTLS (requires `client_private_key`)             |
| `client_private_key`    | string   | -                  | Client key for mTLS (requires `client_certificate`)                     |

`mode` values:
- `event+reconcile` (default): push on every local mutation **and** include in `angos replicate`.
- `event-only`: push on local mutations; excluded from `angos replicate` reconciliation.
- `reconcile-only`: excluded from live pushes; mirrored only via `angos replicate`.

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
| `max_retries`       | u32      | policy   | Maximum retry attempts after initial failure (max 16); defaults to `3` for `required`, `0` otherwise |
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

Angos emits Prometheus metrics on the `/metrics` endpoint, including a family of lock metrics for the distributed lock backends. See [Lock Metrics](metrics.md#lock-metrics) for the metric names and label values.

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
key_prefix = "angos"

[auth.identity.admin]
username = "admin"
password = "$argon2id$v=19$m=19456,t=2,p=1$..."

[auth.oidc.github-actions]
provider = "github"

[global.access_policy]
default = "deny"
rules = ["identity.username != null"]

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
access_key_id = "your-access-key-id"
secret_key = "your-secret-key"
endpoint = "https://s3.example.com"
bucket = "registry"
region = "us-east-1"

[metadata_store.s3]
# Example credentials - replace for production
access_key_id = "your-access-key-id"
secret_key = "your-secret-key"
endpoint = "https://s3.example.com"
bucket = "registry-metadata"
region = "us-east-1"

[metadata_store.s3.lock_strategy.s3]

[auth.identity.admin]
username = "admin"
password = "$argon2id$v=19$m=19456,t=2,p=1$..."
```
