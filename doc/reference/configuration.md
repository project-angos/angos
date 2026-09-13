---
displayed_sidebar: reference
sidebar_position: 1
title: "Configuration"
---

# Configuration Reference

Angos is configured via a TOML file (default: `config.toml`). The configuration is automatically reloaded when the file changes.

## Multiple Configuration Files

`-c` is repeatable. Files are merged in the order given, and a later file wins:

```bash
angos -c /etc/angos/config.toml -c /etc/angos/secrets.toml server
```

This keeps credentials out of the file your deployment tooling renders. The base
file carries everything else, and a second file, sourced from a Kubernetes
Secret written by External Secrets Operator, Vault or sealed-secrets, supplies
only the sensitive values:

```toml
# config.toml
[blob_store.s3]
endpoint = "https://s3.example.com"
bucket = "my-registry"
region = "us-east-1"
```

```toml
# secrets.toml
[blob_store.s3]
access_key_id = "AKIA..."
secret_key = "..."
```

Merge rules:

| Value | Behaviour |
|---|---|
| Table | Merged recursively, so a later file can add keys to a table an earlier file opened |
| Scalar | Replaced by the last file that sets it |
| Array | Replaced as a whole, never appended to |

Because arrays are replaced, an array of tables such as
`[[repository."hub".upstream]]` must be declared entirely in one file. To give an
upstream a password from a separate file, repeat the whole entry there.

Only the merged result has to be a valid configuration, so an overriding file
holds just the keys it changes. Every file is watched, so rotating any of them
reloads the merged configuration without a restart.

A syntax error names the file it came from. An error detected after merging
names the offending key path and the files that were merged, because a merged
document has no single source line to quote.

## Hot Reloading

Most configuration changes take effect immediately without restart. The following options require a restart:

- `server.bind_address`
- `server.port`
- `observability.tracing.sampling_rate`
- Enabling or disabling TLS
- Changing storage backend type (filesystem ↔ S3)
- Adding or removing `[global.job_queue]`
- `max_concurrent_cache_jobs` / `max_concurrent_replication_jobs` on a standalone `angos worker`: the worker's pool size is fixed at startup, so a running worker must be restarted to change it (the server's in-process drain applies the change on reload).

TLS certificate files are also automatically reloaded when they change.

---

## Server (`server`)

| Option                            | Type         | Default  | Description                                        |
|-----------------------------------|--------------|----------|----------------------------------------------------|
| `bind_address`                    | string       | required | Address to bind (e.g., `"0.0.0.0"`, `"127.0.0.1"`) |
| `port`                            | u16          | `8000`   | Port number                                        |
| `query_timeout`                   | non-zero u64 | `3600`   | Query timeout in seconds                           |
| `query_timeout_grace_period`      | non-zero u64 | `60`     | Grace period for queries in seconds                |
| `handshake_timeout`               | non-zero u64 | `10`     | Seconds a client may take to finish its handshake  |

Timeout values must be greater than zero.

### TLS (`server.tls`)

When omitted, the server runs without TLS (insecure). A section that is present
but incomplete or invalid fails startup rather than falling back to a plaintext
listener.

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
| `max_concurrent_requests`   | non-zero usize | `64` | Tokio worker threads (minimum `1`, see Performance Tuning) |
| `max_concurrent_cache_jobs` | usize    | `4`      | Maximum concurrent cache jobs (minimum `1`). With `[global.job_queue]` enabled, also bounds the number of jobs each `angos worker` processes in parallel. |
| `max_concurrent_replication_jobs` | non-zero usize | `4` | Concurrency for replication jobs (minimum `1`). Bounds how many replication pushes are handled in parallel by each `angos worker`, the server's in-process drain, and the `angos reconcile replication` end-of-run drain. |
| `max_concurrent_scan_jobs` | usize | `2` | Worker concurrency for the scan queue |
| `max_concurrent_index_jobs` | usize | `1` | Worker concurrency for the layer index queue; a job inflates one layer, CPU-bound |
| `max_manifest_size`         | string   | `"5MiB"` | Maximum manifest body size accepted from clients or upstream registries |
| `max_blob_size`             | string   | `"100GiB"` | Maximum total size of a single blob upload; a larger upload is rejected with `BLOB_UPLOAD_INVALID` (HTTP 413) |
| `blob_stream_frame_size`    | string   | `"128KiB"` | Read buffer each frame of a streamed blob response is filled from; larger frames cost fewer allocations and body writes per blob served, at one buffer per in-flight response |
| `update_pull_time`          | bool     | `false`  | Track pull times for retention policies     |
| `enable_blob_redirect`      | bool     | `true`   | Allow HTTP 307 redirects for blob downloads. |
| `enable_manifest_redirect`  | bool     | `true`   | Allow HTTP 307 redirects for manifest downloads. Manifest bodies served via `response-content-type` to preserve the media type across redirects. |
| `immutable_tags`            | bool     | `false`  | Global immutable tags default               |
| `immutable_tags_exclusions` | [string] | `[]`     | Regex patterns for mutable tags             |
| `allow_missing_manifest_references` | bool | `true` | When `true` (default), accept a manifest push whose referenced blobs or child manifests are not yet present/owned in the namespace; the missing references stay unreadable until their content is pushed. Set to `false` to reject such pushes with `MANIFEST_BLOB_UNKNOWN`. See note below. |
| `authorization_webhook`     | string   | -        | Name of webhook for authorization           |
| `event_webhooks`            | [string] | `[]`     | Event webhook names for all repositories    |
| `shutdown_drain_secs`       | u64      | `30`     | Seconds to keep draining in-flight work on shutdown before forcing exit: the server stops accepting, lets the requests in flight finish, then drains the queued webhook deliveries. |
| `namespace_walk_concurrency`| usize    | `128`    | Concurrent directory scans a catalog / upload-namespace walk keeps in flight, hiding per-request backend latency on S3. |
| `gc_grace_secs`             | u64      | `300`    | Reclamation grace period, used by the serving process and scrub alike: young keys read as live, the push path re-checks the collector after this long, and gc run markers derive their TTL from it. Lower it only in a maintenance config for offline runs against a store with no live traffic; the serving processes must keep a value that exceeds clock skew plus the longest write stall. |
| `atime_audit_window_secs`   | u64      | `3600`   | How long superseded access entries are retained as pull history, which the admin pull-history endpoint serves and scrub collects past. Raising it grows the number of keys under `!atime/` in proportion to pull volume. |
| `trusted_proxies`           | [string] | `[]`     | Proxy IPs or CIDR networks (e.g. `"10.0.0.1"`, `"10.0.0.0/8"`) whose `X-Forwarded-For`/`X-Real-IP` headers are honored as the client IP. From any other peer those headers are ignored and the socket address is used. The set must name proxies only: a range that also covers clients lets them spoof the forwarded header. |

`max_manifest_size`, `max_blob_size` and `blob_stream_frame_size` must be greater than zero.

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
whichever metadata uses), under a hardcoded top-level `_jobs/` prefix. There is
no job-queue-level backend, credential, or prefix setting: the section accepts
only the tunables below.

> **An honest atomic create is preferred and probed at startup.** The durable
> queue is drained by separate processes that serialise on leased claim keys
> created atomically (`link(2)` on FS, `If-None-Match: *` on S3). A startup
> probe creates a scratch claim key twice; a backend where the second create
> succeeds cannot enforce the atomic create and degrades to advisory claims
> with a logged warning, where a claim race may run an idempotent job more
> than once. Correctness is unaffected either way.

| Option | Type | Default | Description |
|---|---|---|---|
| `pending_refresh_interval_secs` | u64 | `15` | How often the server refreshes the `angos_job_queue_pending` gauge. Must be at least `5` (sub-5s ticks induce LIST storms on S3). |
| `pending_ready_horizon_secs` | u64 | `600` | Readiness horizon for the `angos_job_queue_pending` gauge. Only envelopes whose `not_before` falls within `[..., now + horizon]` are counted. Set comfortably larger than your worker pod startup time so KEDA has lead time to scale up before the work becomes claimable. |
| `max_attempts` | u32 | `5` | Times a failing job is retried before it is dead-lettered. |
| `retry_backoff_min_ms` | u64 | `100` | First retry backoff delay; the exponential schedule grows from here. |
| `retry_backoff_max_ms` | u64 | `10000` | Ceiling on the exponential retry backoff. |
| `claim_ttl_secs` | u64 | `60` | Lease on a job claim, in seconds. A crashed worker's jobs are taken over after this long; the holder refreshes the lease at a third of it. Minimum `3`. |

> **Crash takeover is bounded by `claim_ttl_secs`.** A worker that claims a job leases the job's claim key for `claim_ttl_secs` and refreshes the lease at a third of it. If the holder dies mid-job, another worker takes the claim over once the lease lapses and re-runs the job; handlers are idempotent, so the re-run duplicates work at worst. Transient refresh errors are tolerated while the last verified lease still covers the holder.

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

Required. Choose one: `blob_store.fs` or `blob_store.s3`. A configuration
naming neither fails to load, and so does an `fs` backend whose `root_dir` is
empty, which would otherwise resolve every object against the process working
directory.

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
| `multipart_part_size`            | string | `"50MiB"` | Minimum multipart part size (5 MiB to 5 GiB) |
| `multipart_copy_threshold`       | string | `"5GB"`   | Blob size above which S3 upload completion uses multipart copy |
| `multipart_copy_chunk_size`      | string | `"100MB"` | Server-side part size for multipart copy |
| `multipart_copy_jobs`            | usize  | `4`       | Max concurrent multipart copy jobs |
| `multipart_uniform_parts`        | bool   | `false`   | Use uniform multipart upload mode  |
| `max_attempts`                   | u32    | `3`       | Retry attempts for S3 operations   |
| `operation_timeout_secs`         | u64    | `900`     | Total operation timeout; a streamed upload body is exempt, since the pushing client paces it |
| `operation_attempt_timeout_secs` | u64    | `300`     | Per-attempt timeout, and the read timeout that cuts a stalled transfer short; a streamed upload body is exempt |
| `circuit_breaker_threshold`      | u32    | `5`       | Consecutive failures that trip the circuit breaker open |
| `circuit_breaker_cooldown_secs`  | u64    | `10`      | Seconds the tripped breaker stays open before a half-open probe |
| `children_scan_concurrency`      | usize  | `16`      | Concurrent range chains a truncated children/flat scan fans out to |
| `presign_ttl_secs`               | u64    | `1800`    | Lifetime of a generated presigned download URL |

#### S3 Blob Upload Modes

The registry supports two modes for uploading blobs to S3, controlled by `multipart_uniform_parts`:

**Non-uniform mode (default, `multipart_uniform_parts = false`)**

Each OCI `PATCH` request streams into a long-lived S3 multipart upload, with no intermediate objects or assembly phase. When the client completes the upload with a `PUT` request, the multipart upload is finalized and the blob is copied to its content-addressed path. This mode works with most S3-compatible providers.

A `PATCH` that carries a `Content-Length` is uploaded directly as an `UploadPart` with that known length, split into equal parts only where it would breach S3's 5 GiB per-part ceiling. A chunked `PATCH` (no `Content-Length`, as `docker push` sends) is drained to EOF, flushing an `UploadPart` each time `multipart_part_size` bytes accumulate and restaging the trailing remainder.

Memory usage per upload: for a known-length `PATCH`, up to one ~1 MiB streaming read frame, with no data buffered beyond the current frame. For a chunked `PATCH`, one `multipart_part_size` part while it fills; the remainder the previous `PATCH` staged streams back into it, so the bound stays one part size per in-flight request.

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

### Unknown Keys

Unknown keys under any section are ignored, so configs carrying knobs of
removed subsystems (`lock_strategy`, `conditional_operations`,
`access_time_debounce_secs`) keep loading. Remove them at your convenience.

### Filesystem (`metadata_store.fs`)

| Option         | Type         | Default    | Description                                     |
|----------------|--------------|------------|-----------------------------------------------|
| `root_dir`     | string       | -          | Directory for metadata (defaults to blob store) |
| `sync_to_disk` | bool         | `false`    | Force fsync after writes                        |

### S3 (`metadata_store.s3`)

Same connection options as `blob_store.s3`, plus:

| Option                      | Type         | Default    | Description                                                                 |
|-----------------------------|--------------|------------|-----------------------------------------------------------------------------|
| `link_cache_ttl`            | u64          | `30`       | Read-through cache TTL for link metadata, in seconds (0 to disable)         |

The link cache reduces S3 round-trips for repeated tag/layer reads.

> **Warning:** With `update_pull_time` enabled, every stamped manifest pull adds one storage write (the append-only access entry). At scale with many concurrent pulls this adds latency and API costs; disable access time tracking if it is not needed for retention policies.

### Distributed Locking

There is no lock backend: reads and writes are lock-free, blob reclamation is
fenced by the `v2/gc/` marker protocol, and the durable job queue serialises
workers with atomically created claim keys. `lock_strategy` tables are
ignored like any unknown key (see [Unknown Keys](#unknown-keys)).

---

## Authentication (`auth`)

### Basic Auth (`auth.identity.<name>`)

| Option     | Type   | Default  | Description          |
|------------|--------|----------|----------------------|
| `username` | string | required | Username             |
| `password` | string | required | Argon2 password hash |

Password hashes are validated when the configuration is parsed. An invalid Argon2 hash causes the server to fail to start with a clear error. Use `angos argon` to generate a valid hash.

Usernames must be unique across all `auth.identity` entries, and none may match
an `auth.oidc` provider name: a Basic credential naming a provider is read as
that provider's token. Either collision causes the server to fail to start.

### OIDC (`auth.oidc.<name>`)

Every provider takes the same options: a provider is an issuer plus how its
tokens are validated, so there is no provider type to select.

| Option                  | Type   | Default    | Description                                  |
|-------------------------|--------|------------|----------------------------------------------|
| `issuer`                | string | required   | OIDC issuer URL                              |
| `jwks_uri`              | string | -          | Custom JWKS URI (auto-discovered if not set) |
| `server_ca_bundle`      | string | -          | PEM CA bundle trusted for this provider's HTTPS fetches |
| `client_certificate_bundle` | string | -      | PEM client certificate presented on those fetches, requires `client_private_key` |
| `client_private_key`    | string | -          | PEM key for `client_certificate_bundle`      |
| `bearer_token_file`     | string | -          | File holding a bearer token sent on those fetches, read per fetch |
| `required_claims`       | array  | `[]`       | Claims a token must carry; a missing or null one is rejected |
| `jwks_refresh_interval` | u64    | `3600`     | JWKS refresh interval (seconds)              |
| `required_audience`     | string | -          | Audience the token must carry and match; unset accepts any `aud`, including none |
| `clock_skew_tolerance`  | u64    | `60`       | Clock skew tolerance (seconds)               |
| `allowed_algorithms`    | array  | `["RS256"]` | Allowed JWT signing algorithms              |
| `http_request_timeout_secs` | u64 | `30`     | Timeout for a JWKS or discovery HTTP fetch (seconds) |
| `jwks_refresh_timeout_secs` | u64 | `5`      | Timeout for the forced JWKS refetch on key rotation (seconds) |

GitHub Actions, for example, is one such entry:

```toml
[auth.oidc.github-actions]
issuer = "https://token.actions.githubusercontent.com"
jwks_uri = "https://token.actions.githubusercontent.com/.well-known/jwks"
required_claims = ["repository", "actor"]
```

Set `server_ca_bundle` for an issuer whose certificate the system roots do not
cover, such as a kube-apiserver signed by the cluster CA. It applies to the
discovery and JWKS fetches for that provider alone; other providers keep the
system roots.

Set `client_certificate_bundle` and `client_private_key` for an issuer that
refuses an anonymous caller on those endpoints, a kube-apiserver serving
discovery to authenticated users only being the usual case. Configuring one
without the other fails startup rather than fetching anonymously.

Set `bearer_token_file` instead for an issuer that authenticates callers with a
token, such as the same kube-apiserver reached with angos's own projected
service-account token. The file is read on every fetch, so a token the kubelet
rotates in place stays current, and an unreadable path fails startup. The token
is sent only to URLs on the issuer's own origin: `jwks_uri` comes out of the
discovery document, and an issuer naming another host is not handed it.

`required_claims` checks presence only. Predicates over claim *values* belong in
the access policy, which sees the whole claim map.

`allowed_algorithms` accepts JWT algorithm names such as `"RS256"`, `"RS384"`, `"RS512"`, `"ES256"`, and `"ES384"`. Angos rejects tokens whose header claims an algorithm outside the provider allowlist before signature verification to prevent algorithm-confusion attacks.

### Token Service (`auth.token_service`)

Issues registry-signed bearer tokens at `GET /token`, so a client holding a
short-lived credential can exchange it once and keep pushing after that
credential expires. Present the section to enable it.

| Option       | Type   | Default  | Description                                                     |
|--------------|--------|----------|-----------------------------------------------------------------|
| `secret_key` | string | required | Base64 HMAC signing key, at least 32 bytes decoded               |
| `realm`      | string | -        | Absolute token URL advertised to clients, path must end with `/token` |
| `ttl_secs`   | u64    | `3600`   | Token lifetime in seconds, at most `86400`                      |

With the section present, a `401` carries `WWW-Authenticate: Bearer` instead of
`Basic`, for every client rather than only OIDC ones. Left unset, the challenge
is built from each request's own `Host`, which is what a registry serving
several hostnames wants; behind a TLS-terminating proxy, list the proxy in
`global.trusted_proxies` so its `X-Forwarded-Proto` decides the scheme. Set
`realm` when anything in front of the registry caches responses, so the
challenge cannot follow a `Host` a client chose, and when a proxy strips a path
prefix, so the advertised URL is the prefixed one clients must call.

Generate `secret_key` with `openssl rand -base64 32`: it is decoded before use,
so its strength is the randomness of those bytes and not the length of a
passphrase. Rotating it invalidates every outstanding token; clients recover by
fetching a new one. An issued token freezes the identity it was minted from but
not its permissions: access policies are still evaluated per request. A token
cannot otherwise be revoked before it expires, so `ttl_secs` is the window a
stolen one stays usable. Removing or renaming an `auth.oidc` entry invalidates
outstanding tokens minted from that provider. The section reloads without a
restart, `secret_key` included, so rotating the key during an incident costs no
downtime.

`GET /token` is subject to the access policy like any other route. Under
`default = "deny"`, add a rule for it:

```toml
[global.access_policy]
default = "deny"
rules = [
  "request.action == 'get-token' && identity.oidc != null",
]
```

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

Repository namespace keys must not overlap: a key like `team` and a key like `team/app` are considered overlapping because one is a namespace-prefix of the other. The registry rejects this configuration at startup, as it does two repositories declaring the same `namespace`.

| Option                      | Type     | Default  | Description                     |
|-----------------------------|----------|----------|---------------------------------|
| `namespace`                 | string   | none     | Registry namespace this repository mirrors (`docker.io`), as a client names it in the `?ns=` proxy parameter. A request naming it is served from this repository whatever path it asks for; see [Upstream Selection](../explanation/pull-through-caching.md#upstream-selection-and-the-ns-parameter) |
| `immutable_tags`            | bool     | `false`  | Enable immutable tags for this repository. The effective flag is this value OR `global.immutable_tags`, so a repository can add immutability but never opt out of a global `true` |
| `immutable_tags_exclusions` | [string] | inherits | Replaces the global exclusion list when non-empty |
| `scan` | bool | `false` | Send each image manifest pushed here, or stored by a cache miss in a pull-through repository, to the scanner service |
| `index` | bool | `false` | Index the filesystem of each image manifest pushed here, or stored by a cache miss, as it lands, so the web UI browses it at once; without the flag an image is indexed the first time someone opens its filesystem |
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
- `event+reconcile` (default): push on every local mutation **and** include in `angos reconcile replication`.
- `event-only`: push on local mutations; excluded from `angos reconcile replication` reconciliation.
- `reconcile-only`: excluded from live pushes; mirrored only via `angos reconcile replication`.

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

## Scanning (`global.scan`)

The scanner service each `scan = true` repository sends its image pushes to.

| Option         | Type   | Default | Description                                                  |
|----------------|--------|---------|--------------------------------------------------------------|
| `url`          | string | required | Base URL of the scanner service; the job posts to its `/scan` |
| `token`        | string | -       | Bearer token the service expects, when it checks one         |
| `timeout_secs` | u64    | `600`   | Bound on one scan request, pull and analysis included        |

A repository opts in with `scan = true`; setting it without `[global.scan]` fails validation. `max_concurrent_scan_jobs` in `[global]` sizes the scan queue's worker pool.

## Scanner Service (`scanner`)

Read by `angos scanner` alone, so a scanner host's configuration can hold this section and nothing else; the registry ignores it.

| Option                 | Type   | Default   | Description                                                  |
|------------------------|--------|-----------|--------------------------------------------------------------|
| `bind_address`         | string | `0.0.0.0` | Address the service listens on                               |
| `port`                 | u16    | `8766`    | Port the service listens on                                  |
| `token`                | string | -         | Bearer token a scan request must carry; unset accepts any    |
| `max_concurrent_scans` | usize  | `2`       | Scanner processes run at once; further requests wait. Trivy locks its cache and runs one at a time whatever the value |
| `registry.url`         | string | required  | Registry the scanner pulls images from                       |
| `registry.username`    | string | -         | Identity the scanner pulls with                              |
| `registry.password`    | string | -         | Its password                                                 |

---

## Observability

### Tracing (`observability.tracing`)

| Option          | Type   | Default  | Description               |
|-----------------|--------|----------|---------------------------|
| `endpoint`      | string | required | OpenTelemetry endpoint    |
| `sampling_rate` | f64    | required | Sampling rate (0.0 - 1.0) |

### Prometheus Metrics

Angos emits Prometheus metrics on the `/metrics` endpoint. See the [Metrics Reference](metrics.md) for the metric names and label values.

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

[cache.redis]
url = "redis://localhost:6379"
key_prefix = "angos"

[auth.identity.admin]
username = "admin"
password = "$argon2id$v=19$m=19456,t=2,p=1$..."

[auth.oidc.github-actions]
issuer = "https://token.actions.githubusercontent.com"
required_claims = ["repository", "actor"]

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

This example uses S3 for both blob and metadata storage; multiple instances need no coordination infrastructure:

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

[auth.identity.admin]
username = "admin"
password = "$argon2id$v=19$m=19456,t=2,p=1$..."
```
