---
displayed_sidebar: reference
sidebar_position: 4
title: "API Endpoints"
---

# API Endpoints Reference

Angos implements the [OCI Distribution Specification v1.1](https://github.com/opencontainers/distribution-spec/releases/tag/v1.1.0) plus extension endpoints.

---

## OCI Distribution API

Base path: `/v2/`

Comma-separated `Accept` header values are parsed and ordered by quality (`q`) before Angos uses them for upstream pull-through requests.

The optional `ns` query parameter ([Registry Proxying](https://github.com/opencontainers/distribution-spec/blob/main/spec.md#registry-proxying)) names the registry namespace a mirroring client believes it is addressing. On a pull, a request naming one a `[repository]` declares (`namespace = "docker.io"`) is served from that repository whatever path it asks for, and the response echoes `OCI-Namespace`. A namespace no repository declares is ignored, as is `ns` on a write, and no header is sent. See [Upstream Selection and the `ns` Parameter](../explanation/pull-through-caching.md#upstream-selection-and-the-ns-parameter).

### API Version Check

```
GET /v2/
HEAD /v2/
```

Returns `200 OK` if the registry is available. Used for authentication challenges.

### Blobs

```
HEAD /v2/{namespace}/blobs/{digest}
GET  /v2/{namespace}/blobs/{digest}
```

Check existence or download a blob by digest. A blob is visible only within namespaces that own it;
a digest that exists in storage but is not linked to the requested namespace returns `BLOB_UNKNOWN`.

`GET` supports a single byte range through the `Range` header:

- `Range: bytes=<start>-<end>` returns `206 Partial Content`.
- `Range: bytes=<start>-` returns from `<start>` through the end of the blob.
- `Range: bytes=-<suffix-length>` returns the final `<suffix-length>` bytes.
- If `<end>` is beyond the blob length, Angos clamps it to the final byte.
- If `<suffix-length>` is longer than the blob, Angos returns the full blob as `206 Partial Content`.
- Multiple ranges are not supported; Angos ignores them and returns the normal full `200 OK` response.
- A `Range` Angos cannot read is ignored the same way, returning `200 OK`: an unknown unit (`items=0-5`), a missing one (`100-200`), a non-numeric bound, or an inverted window (`bytes=500-499`).
- A range that reads correctly but cannot be met returns `416 Range Not Satisfiable`: a start at or beyond the blob length, or a zero-length suffix range.
- For an empty blob, Angos ignores a syntactically valid range and returns the normal `200 OK` empty body.

On a pull-through repository, a range over a blob that is not cached yet is forwarded to the upstream: its `206` is passed through with the upstream's `Content-Range`, and an upstream that ignores the range answers the full `200 OK` body. The blob is still queued for caching, so later ranges are served locally.

When blob redirects are enabled (`global.enable_blob_redirect`, default `true`) and the blob store supports presigned URLs, `GET` may answer with a `307` redirect. Sending an `X-Angos-No-Redirect` header with any non-empty value other than `0` or `false` forces an inline body instead. The web UI sends it because a browser `fetch` cannot follow the cross-origin presigned redirect; OCI clients never send it.

```
DELETE /v2/{namespace}/blobs/{digest}
```

Delete a blob owned by the namespace. If the digest is still referenced by manifest metadata in
that namespace, Angos returns `DENIED` (HTTP 405, the status the spec lists for a refused blob
delete) and leaves the blob unchanged; the message names the reason, which the client resolves by
deleting the manifest first. After those references are removed, deleting the blob removes that
namespace's ownership and answers `202 Accepted`; when no namespace references the digest, the
blob's bytes are reclaimed by the next `angos scrub` sweep.

### Blob Upload

```
POST /v2/{namespace}/blobs/uploads/
```

Start a new blob upload. Returns `202 Accepted` with a `Location` header pointing at the upload
session, or `201 Created` when the blob is already available or was uploaded by this request
(see `digest` / `mount` below).

Query parameters:
- `digest` - Return the existing blob (returns `201 Created`) when the requested namespace already
  owns it. Otherwise, a request declaring a `Content-Length` carries the blob as its body and
  completes the upload here, returning `201 Created`; a declared `0` is the empty blob, not an
  absent body. A request declaring no length at all (a chunked body) starts an upload session the
  client then closes with a `PUT`. A body that does not hash to `digest` is rejected with
  `DIGEST_INVALID` and stores nothing.
- `digest-algorithm` - The algorithm (`sha256` or `sha512`) the upload will be closed with. The
  session then hashes each chunk under that one alone instead of every supported algorithm, which is
  what a session must do when it cannot know the algorithm before the closing `PUT`. Completing a
  hinted session with a digest of another algorithm returns `DIGEST_INVALID`, and an unsupported
  value returns `400 Bad Request`.
- `mount` (with optional `from`) - Cross-repository blob mount. `?mount={digest}` requests that an
  existing blob be referenced by the target namespace with no body transfer:
  - With `from`: the mount succeeds when the blob exists, is held by `{repository}`, and the caller is
    authorized to read it from there.
  - Without `from` (automatic content discovery): the mount succeeds when the blob exists and the
    caller is authorized to read it from a namespace that already references it.

  On success the server returns `201 Created` with the blob `Location`. When the blob cannot be
  mounted (absent, not held by the named source, or not readable by the caller), the server falls
  back to a normal upload session (`202 Accepted`). A mount request never fails for this reason.
  The session fall-back covers *unsatisfiable* mounts only: a syntactically malformed `?digest=`,
  `?mount=`, or `?from=` value returns `400 Bad Request`.

  **Authorization.** A mount only grants a reference to a blob the caller could already read: the
  server evaluates the caller's read access (`get-blob`) against the source namespace (the `from`
  repository, or for a `from`-less mount a namespace that references the blob) and falls back to a
  normal upload session when none is readable, so a mount never hands over a blob the caller could
  not otherwise pull. A mount is also its own route and CEL action, `mount-blob`, distinct from
  `start-upload`, so you can additionally restrict who may mount at all with a
  `request.action == 'mount-blob'` rule (denying it rejects the mount; Angos's replication falls back
  to a normal upload). Container clients send `?mount=` opportunistically on push, so a default-deny
  policy should grant `mount-blob` alongside `start-upload` or those pushes fail. See
  [Restrict cross-repository blob mount](../how-to/set-up-access-control.md#restrict-cross-repository-blob-mount).

```
GET /v2/{namespace}/blobs/uploads/{uuid}
```

Get upload status.

```
PATCH /v2/{namespace}/blobs/uploads/{uuid}
```

Upload a chunk. Use `Content-Range` for the chunk range, spelled `<start>-<end>` as the specification defines it, and `Content-Length` for the exact chunk size. The `bytes=` unit belongs to the `Range` header and is rejected here. A `Content-Range` must resume at the committed offset and, when it names a last byte, must agree with the `Content-Length` sent with it; either mismatch returns `416 Range Not Satisfiable` before a byte is committed. A missing `Content-Length` is accepted as a chunked upload streamed to EOF, and a chunked body that does not fill the window it announced returns `416` and cancels the session; an invalid `Content-Length` returns `400 Bad Request`. A body shorter than the `Content-Length` it declared commits what arrived and answers `202` with the bytes actually committed in `Range`, leaving the closing `PUT` to refuse the digest. An upload whose cumulative size exceeds `global.max_blob_size` is rejected with `BLOB_UPLOAD_INVALID` (HTTP 413).

```
PUT /v2/{namespace}/blobs/uploads/{uuid}?digest={digest}
```

Complete the upload with final digest. A final chunk sent with a `Content-Length` uses that chunk size; a final chunk sent chunked (a body without `Content-Length`) is streamed to EOF. With no body, the upload is treated as zero length.

```
DELETE /v2/{namespace}/blobs/uploads/{uuid}
```

Cancel an upload.

### Manifests

```
HEAD /v2/{namespace}/manifests/{reference}
GET  /v2/{namespace}/manifests/{reference}
```

Check existence or download a manifest. `{reference}` can be a tag or digest.

When manifest redirects are enabled (`global.enable_manifest_redirect`, default `true`) and the blob store supports presigned URLs, `GET` may answer with a `307` redirect. As for blobs, an `X-Angos-No-Redirect` header with any non-empty value other than `0` or `false` forces an inline body instead.

```
PUT /v2/{namespace}/manifests/{reference}
```

Push a manifest. Manifest bodies larger than `global.max_manifest_size` are rejected with
`MANIFEST_INVALID` (HTTP 413). When `global.allow_missing_manifest_references = false`, config, layer, and
child manifest digests referenced by the manifest must already exist and be readable in the
namespace, and missing references are rejected with `MANIFEST_BLOB_UNKNOWN`. By default
(`allow_missing_manifest_references = true`) the push is accepted, but a referenced digest the
namespace does not already own is not made readable: it resolves as unknown on a later pull
(`BLOB_UNKNOWN` for a blob, `MANIFEST_UNKNOWN` for a child manifest) until its content is pushed.
Subject digests used for referrers are not required to exist in either mode.

A `Content-Type` that disagrees with the body's own `mediaType` is rejected with `MANIFEST_INVALID`.
Parameters on the header (`; charset=utf-8`) take no part in that comparison and are not stored, so a
pull serves the bare media type whatever the push sent.

When pushing by digest, one or more `?tag=` query parameters create the listed tags pointing at the
pushed manifest, for example `PUT /v2/{namespace}/manifests/{digest}?tag=1.2.3&tag=latest`. Each
value must be a valid tag; a value that is not a valid tag is rejected at routing with a generic
`400`, like any other malformed reference. The response carries an `OCI-Tag` header listing the
accepted tags, comma and space separated. Tag query parameters on a by-tag push are ignored.

```
DELETE /v2/{namespace}/manifests/{reference}
```

Delete a manifest by tag or digest, answering `202 Accepted`. Deleting by tag removes only that
tag. Deleting by digest also removes tags pointing at the digest; the manifest body's bytes are
reclaimed by the next `angos scrub` sweep once no namespace references it. Config and layer blobs
remain owned by the namespace until they are deleted through the blob endpoint or scrubbed as
orphans.

#### Replication request header

Manifest `PUT` and `DELETE` accept an optional replication header, set automatically by Angos when
mirroring a change to a configured downstream (it is not used by ordinary clients):

| Header                     | Value                          | Purpose                                                                 |
|----------------------------|--------------------------------|-------------------------------------------------------------------------|
| `X-Angos-Source-Timestamp` | event timestamp (RFC 3339)     | Last-writer-wins: the receiver compares it against the creation time of the affected tags and rejects the write with `409 REPLICATION_SUPERSEDED` when the local copy is strictly newer. |

Last-writer-wins applies only when `X-Angos-Source-Timestamp` is present and parses as RFC 3339,
and is always evaluated against tag creation times. A tag `PUT` or `DELETE` is compared against the
local tag's recorded creation time. A `DELETE` by digest cascades to every tag pointing at the
revision, so it is guarded through those tags: when any pointing tag is strictly newer than the
incoming timestamp, the whole delete is rejected with `409 REPLICATION_SUPERSEDED`: the newer tag,
and the revision it still references, must not be dropped by the older delete. A `PUT` by digest is
content-addressed (there is nothing to resolve) and is not LWW-guarded. A missing, empty, or
malformed timestamp simply disables LWW for that request: the write is applied as an ordinary
client write rather than failing. A local tag with no recorded creation time is treated as oldest
and never blocks the incoming write.

A future-dated timestamp is **clamped to the receiver's current time**, so a client cannot pin a
permanent last-writer-wins victory. A *backdated* timestamp is accepted and persisted verbatim as
the tag's creation time, where it weakens that write in later LWW races and feeds age-based
retention and the `top_pushed` ranking with the supplied date. The header is honored from **any
identity allowed to push** and cannot be gated separately from `put-manifest`, so a push-capable
identity can backdate a tag far enough to make it eligible for pruning on the next `angos
prune` run, an indirect delete even without a `delete-manifest` grant. Treat push on a
replicated repository as trust over `created_at`, and restrict it via the CEL `access_policy` (see
[Restrict replication writes](../how-to/set-up-access-control.md#restrict-replication-writes)).

A `409 REPLICATION_SUPERSEDED` is convergence, not failure: the sender treats it as success and
completes the replication job. It is distinct on the wire from the immutable-tag `409 DENIED`, which
surfaces so the job retries or dead-letters.

### Tags

```
GET /v2/{namespace}/tags/list
```

List tags for a namespace.

Query parameters:
- `n` - Maximum number of results
- `last` - Pagination marker

A namespace holding no manifest content at all returns `NAME_UNKNOWN` (HTTP 404), so a client can probe existence here. A namespace whose tags were all deleted still holds its revisions and returns `200` with an empty list, until those are deleted too.

On a pull-through repository the listing reports the tags this registry has cached, not the upstream's catalogue: enumerating the upstream would walk its whole tag list on every request. Pull a tag to cache it, or list tags against the upstream directly.

### Catalog

```
GET /v2/_catalog
```

List repositories. A Docker Registry V2 endpoint the OCI distribution specification does not define; angos serves it at its long-standing path.

Query parameters:
- `n` - Maximum number of results
- `last` - Pagination marker

The returned names are derived directly from stored content: a namespace is listed exactly when it holds at least one revision or tag, and stops being listed as soon as the last one is deleted.

The listing is filtered by access policy: a namespace is included only when the caller could list its tags under the global policy and the covering repository's policy, so a repository a caller cannot read is hidden from its catalog. Filtering runs per page, so a page may return fewer than `n` names while more remain; follow the `Link` header until it is absent. The authorization webhook is not consulted for this filtering.

### Referrers

```
GET /v2/{namespace}/referrers/{digest}
```

List manifests that reference a subject digest.

Query parameters:
- `artifactType` - Filter by artifact type
- `last` - Pagination marker. The specification paginates this endpoint through the `Link` header alone, so this is the cursor Angos puts in the `Link` it advertises rather than one a client composes; it takes no page-size parameter, and Angos sizes the page at 100

A malformed request is rejected with `DIGEST_INVALID` (HTTP 400): a digest that is not valid syntax, or an `artifactType` that is not a media type. A registry serving this endpoint must never answer `404` to it, so an unreadable request is a bad one rather than an unserved path, unlike the other listings.

Referrers a client recorded under the fallback tag (`<algorithm>-<hex>`, an index of referring descriptors) before this registry served the API are folded into the listing, as the spec's "Enabling the Referrers API" procedure requires, so a repository imported from such a registry keeps them.

On a pull-through repository the listing merges the upstream's referrers with the cached ones, since nothing fills a referrer index on its own and an uncached subject would otherwise report none. An upstream that cannot be reached is left out rather than failing the request, so the cached referrers are still served. The `artifactType` filter is applied to both.

A subject with more referrers than the page size is served one page at a time, with the next page advertised in a `Link` header carrying `rel="next"`; the link repeats the `artifactType` filter so following it keeps the listing filtered, percent-encoding it so a `+json` suffix survives the round trip. A filtered page may hold fewer than 100 entries, since the filter is applied after the page is cut.

Merging a pull-through listing needs both sides whole, so each page re-reads the upstream's referrers in full: paginating a widely referenced subject on a mirror costs one upstream enumeration per page.

---

## Extension API (not part of the OCI specification)

Base path: `/v2/_angos/` for the registry, `/v2/<name>/_angos/` for one namespace (`<name>` being the OCI repository name).

These sit in the extension namespace the distribution spec reserves, whose shape is `_<extension>/<component>/<module>`; `angos` is the extension name. See the spec's [extensions document](https://github.com/opencontainers/distribution-spec/blob/main/extensions/README.md).

> **Note:** the top-level `/_ext/` prefix is not served; clients must use the `/v2/_angos/...` equivalents. [Upgrade Angos](../how-to/upgrade.md#extension-api-moved-into-the-reserved-namespace-breaking-change) maps each pre-1.5.0 path to its replacement.

### List Repositories

```
GET /v2/_angos/repositories/list
```

List all configured repositories with their namespace counts.

**Response:**
```json
{
  "repositories": [
    {
      "name": "library",
      "namespace_count": 15,
      "pull_through_cache": true,
      "immutable_tags": true
    }
  ]
}
```

### List Namespaces

```
GET /v2/_angos/namespaces/list?repository={repository}
```

List namespaces within a repository, with the repository's effective configuration.

Names are served from the `v2/cat` index alone, reading only the repository's own key range. A
namespace emptied since its last write can therefore still be listed, with zero counts, until
`angos scrub` reaps its index key.

Each namespace's counts are a full enumeration of its tags, revisions, and upload sessions. The
three are issued together per namespace, and the namespaces themselves are counted concurrently, so
the listing costs one round of enumerations rather than one after another.

**Response:**
```json
{
  "repository": "library",
  "namespaces": [
    {
      "name": "library/nginx",
      "tag_count": 12,
      "manifest_count": 25,
      "upload_count": 0
    }
  ],
  "pull_through_cache": true,
  "upstream_urls": ["https://registry-1.docker.io"],
  "immutable_tags": false,
  "immutable_tags_exclusions": []
}
```

### List Revisions

```
GET /v2/{namespace}/_angos/revisions/list
```

List all manifest revisions with tags, parent relationships, and referrers.

**Response:**
```json
{
  "name": "library/nginx",
  "manifests": [
    {
      "digest": "sha256:abc123...",
      "tags": ["latest", "1.25.0"],
      "parents": [
        {
          "digest": "sha256:def456...",
          "tags": ["latest"],
          "platform": {"os": "linux", "architecture": "amd64"}
        }
      ],
      "referrers": [
        {
          "digest": "sha256:789abc...",
          "artifactType": "application/vnd.dev.cosign.artifact.sig.v1+json"
        }
      ],
      "pushed_at": "2026-01-01T12:00:00Z",
      "last_pulled_at": "2026-01-02T08:30:00Z"
    }
  ]
}
```

`parents` and `referrers` are omitted when empty; `pushed_at` and `last_pulled_at` are omitted when not recorded.

### List Pulls

```
GET /v2/{namespace}/_angos/pulls/list?tag={tag}
GET /v2/{namespace}/_angos/pulls/list?digest={algorithm}:{hex}
```

List the recorded pulls of one manifest target, newest first. Exactly one of `tag` or `digest` is
required: a missing, empty, ambiguous, or unparseable target returns `404` rather than silently
narrowing to one of the two.

Pulls are only recorded when [`update_pull_time`](configuration.md#global-options-global) is enabled, which is off by
default; with it disabled every target reports an empty list.

**Response:**
```json
{
  "target": "1.25.0",
  "window_secs": 3600,
  "entries": [
    {
      "client": "alice",
      "at": "2026-01-02T08:30:00Z"
    }
  ]
}
```

`target` echoes the requested reference and `window_secs` reports the configured
[`atime_audit_window_secs`](configuration.md#global-options-global) retention. Anonymous pulls are recorded as
`anonymous`. A target with no recorded pulls returns an empty `entries` list rather than `404`;
the endpoint does not check that the target exists.

At most 100 entries are returned and the endpoint does not paginate. Only the newest entry is
retained indefinitely — scrub collects superseded entries once they age past `window_secs` — so
this is a bounded audit window, not a complete pull history.

This endpoint is gated by the same `list-revisions` CEL action as
[List Revisions](#list-revisions).

### Layer Entries

```
GET /v2/{namespace}/_angos/layers/{algorithm}:{hex}/entries
```

The filesystem listing of one layer: every tar entry in order, with its kind, size, mode,
owner, modification time, link target and the offset of its data in the uncompressed stream.
The first request for a layer that was never indexed enqueues the index job and answers
`202 Accepted` with `{"status": "indexing"}`; ask again once it ran. A layer the namespace
does not own is `404`, like the blob itself.

**Response:**
```json
{
  "compressed": true,
  "uncompressed_size": 7340032,
  "entries": [
    { "path": "etc", "kind": "dir", "size": 0, "mode": 493, "uid": 0, "gid": 0, "mtime": 1700000000, "offset": 512 },
    { "path": "etc/os-release", "kind": "file", "size": 164, "mode": 420, "uid": 0, "gid": 0, "mtime": 1700000000, "offset": 1536 },
    { "path": "etc/motd", "kind": "whiteout", "size": 0, "mode": 420, "uid": 0, "gid": 0, "mtime": 1700000000, "offset": 2560 }
  ]
}
```

`kind` is one of `file`, `dir`, `symlink`, `hardlink`, `whiteout`, `opaque` or `other`; a
whiteout names the path the layer removes from the ones below it, an opaque marker the
directory it empties. Paths are normalised without a leading `./` or trailing `/`.

### Layer File

```
GET /v2/{namespace}/_angos/layers/{algorithm}:{hex}/file?path={path}
GET /v2/{namespace}/_angos/layers/{algorithm}:{hex}/file?path={path}&download
```

The bytes of one file of the layer, `path` as the listing spells it; a hard link serves its
target. The content type is guessed from the name, and `download` adds a
`Content-Disposition: attachment` header. A gzipped layer is decoded from the nearest of the
checkpoints the index job recorded, every 4 MiB of output, rather than from its start. A path
that is not a file, or a layer not yet indexed, is `404`.

Both endpoints are gated by the `get-blob` CEL action of the layer's namespace.

### List Uploads

```
GET /v2/{namespace}/_angos/uploads/list
```

List blob uploads in progress.

**Response:**
```json
{
  "name": "library/nginx",
  "uploads": [
    {
      "uuid": "123e4567-e89b-12d3-a456-426614174000",
      "size": 1048576,
      "started_at": "2026-01-01T12:00:00Z"
    }
  ]
}
```

### List Jobs

```
GET /v2/_angos/jobs/list
```

List pending and in-flight jobs on a durable job queue (see
[Enable Durable Cache Jobs](../how-to/durable-cache-jobs.md), which introduces the queues and this
admin API).

Query parameters:
- `n` - Maximum number of results (default 100)
- `after` - Pagination cursor: the `next` value from the previous page
- `queue` - Queue to administer: `cache` (default) or `replication`

An unknown `queue` value, or any malformed query value (for example a non-numeric `n`), rejects the
request rather than silently falling back to the default `cache` queue: the `GET` listings return
`404` and the retry/delete mutations return `400`.

Like the cross-repository blob mount, job administration uses its own CEL actions (`list-jobs`,
`list-failed-jobs`, `retry-job`, and `delete-job`) so it can be gated behind higher privilege than
registry reads; `queue` is exposed to CEL so the `replication` queue can be gated separately.

**Response:**
```json
{
  "jobs": [
    {
      "storage_key": "0000019700a1b2c3-123e4567-e89b-12d3-a456-426614174000",
      "id": "123e4567-e89b-12d3-a456-426614174000",
      "kind": "cache.fetch_blob",
      "lock_key": "cache.library/nginx:sha256:abc123...",
      "attempts": 1,
      "max_attempts": 5,
      "created_at": "2026-01-01T12:00:00Z",
      "not_before": "2026-01-01T12:05:00Z"
    }
  ],
  "next": "0000019700a1b2c3-123e4567-e89b-12d3-a456-426614174000"
}
```

`not_before` is the earliest instant a worker may pick the job up, decoded from the storage key's
time prefix. `next` is present only when another page follows; pass it back as `after`.

### List Failed Jobs

```
GET /v2/_angos/jobs/failed
```

List dead-lettered jobs, i.e. jobs that exhausted their retry budget. Same query parameters and
rejection rules as `GET /v2/_angos/jobs/list`.

**Response:**
```json
{
  "failed": [
    {
      "storage_key": "0000019700a1b2c3-123e4567-e89b-12d3-a456-426614174000",
      "id": "123e4567-e89b-12d3-a456-426614174000",
      "kind": "replication.push_manifest",
      "lock_key": "replication.push.backup:library/nginx:latest",
      "attempts": 5,
      "max_attempts": 5,
      "created_at": "2026-01-01T12:00:00Z",
      "failed_at": "2026-01-01T12:30:00Z",
      "last_error": "..."
    }
  ],
  "next": "0000019700a1b2c3-123e4567-e89b-12d3-a456-426614174000"
}
```

### Retry Failed Job

```
POST /v2/_angos/jobs/failed?key={key}
```

Requeue a dead-lettered job with its attempt counter reset to zero. `{key}` is the job's
`storage_key` from the failed listing. Accepts `?queue=` like the listings. Returns
`204 No Content` on success, or `404` when the key does not exist.

### Delete Job

```
DELETE /v2/_angos/jobs/failed?key={key}
DELETE /v2/_angos/jobs/pending?key={key}
```

Delete a dead-lettered or pending job by `storage_key`. Accepts `?queue=` like the listings.
Returns `204 No Content` on success, or `404` when the key does not exist.

---

## Health and Metrics

### Health Check (Liveness)

```
GET /healthz
```

Returns `200 OK` if the service is running. Use this for Kubernetes liveness probes to detect hung processes.

### Readiness Check

```
GET /readyz
```

Returns `200 OK` when the metadata store answers a single bounded listing. The check does not walk the namespace tree and does not probe the blob store.

Use this for Kubernetes readiness probes to detect when a replica is unable to serve traffic.

**Success Response:**
```json
{"status":"ready"}
```

**Service Unavailable Response (503):**
```json
{"status":"not_ready","error":"storage backend not ready: ..."}
```

### Prometheus Metrics

```
GET /metrics
```

Returns metrics in Prometheus exposition format.

---

## Web UI

When the UI is enabled, non-API paths serve the web interface.

### UI Routes

| Route                                | Description                |
|--------------------------------------|----------------------------|
| `/`                                  | Repository list            |
| `/{repository}`                      | Namespace list             |
| `/{repository}/{namespace}`          | Manifest list              |
| `/{repository}/{namespace}:{tag}`    | Manifest details by tag    |
| `/{repository}/{namespace}@{digest}` | Manifest details by digest |

### UI Configuration

```
GET /v2/_angos/ui/config
```

Returns UI configuration.

**Response:**
```json
{
  "name": "My Container Registry"
}
```

---

## Authentication

Every route passes through the access policy, including `/healthz`, `/readyz` and `/token` (actions `healthz`, `readyz` and `get-token`). A default-deny policy must allow those actions or health probes and token exchange fail.

### Token Service

```
GET /token
```

Exchanges the credential that authenticated the request for a registry-signed bearer token, so a client holding a short-lived credential can keep working after it expires. Returns `404` unless [`auth.token_service`](configuration.md#token-service-authtoken_service) is configured.

The endpoint is advertised in the `WWW-Authenticate` header of a `401`, and OCI clients follow it on their own. A request with no credentials gets a token carrying no identity, which the access policy then evaluates as anonymous.

`GET` is the only method it serves. The OAuth2 form of the token endpoint, `POST /token`, is not implemented: it answers `405` with `Allow: GET`, which is what lets a client that probes the OAuth2 form first — containerd does whenever it holds a password — fall back to the plain form and complete the exchange.

**Success Response:**
```json
{"token":"<jwt>","expires_in":3600}
```

Present it as `Authorization: Bearer <token>` on subsequent requests. The token declares its own type, `angos+jwt`, which is what tells it apart from a provider's bearer on the same header. The token carries the identity, never the client certificate or IP: those are read from the live request, so an mTLS client keeps its certificate identity while using a token.

The response is `Cache-Control: no-store`, and presenting a registry token here is refused with a `401`: renewing one would let a token outlive the credential it was minted from for as long as a client kept asking, and `ttl_secs` would bound nothing.

### Methods

**Basic Authentication:**
```
Authorization: Basic base64(username:password)
```

**Bearer Token (OIDC):**
```
Authorization: Bearer <jwt-token>
```

**Bearer Token (registry):**
```
Authorization: Bearer <token from GET /token>
```

Both bearers share the header. Angos tells them apart by the type each declares,
so a token it did not issue is left for the OIDC providers to validate.

**OIDC via Basic Auth (Docker compatibility):**
```
Authorization: Basic base64(provider-name:jwt-token)
```

Authentication schemes are parsed case-insensitively, so `basic` and `bearer` are accepted the same as `Basic` and `Bearer`.

When the username matches an OIDC provider name, the password is validated as a JWT token. This enables Docker clients to authenticate with OIDC tokens:

```bash
echo "$OIDC_TOKEN" | docker login registry.example.com \
  --username github-actions --password-stdin
```

**mTLS:**

Present a client certificate during TLS handshake.

### Authentication Flow

1. Client makes unauthenticated request
2. Server returns `401 Unauthorized` with `WWW-Authenticate` header
3. Client retries with credentials, or, when the header names a `Bearer` realm,
   exchanges them at that realm for a registry token and retries with it
4. Server validates and processes request

---

## Error Responses

Errors follow OCI Distribution error format:

```json
{
  "errors": [
    {
      "code": "MANIFEST_UNKNOWN",
      "message": "manifest unknown",
      "detail": "sha256:abc123..."
    }
  ]
}
```

### Error Codes

| Code                  | HTTP Status  | Description               |
|-----------------------|--------------|---------------------------|
| `BLOB_UNKNOWN`        | 404          | Blob does not exist       |
| `BLOB_UPLOAD_INVALID` | 413          | Blob upload exceeds `global.max_blob_size` |
| `BLOB_UPLOAD_UNKNOWN` | 404          | Upload session not found  |
| `DIGEST_INVALID`      | 400          | Invalid digest format     |
| `MANIFEST_BLOB_UNKNOWN` | 404        | Manifest reference is missing |
| `MANIFEST_INVALID`    | 400, or 413 when the body exceeds `global.max_manifest_size` | Invalid manifest content  |
| `MANIFEST_UNKNOWN`    | 404          | Manifest does not exist   |
| `NAME_INVALID`        | 400          | Invalid repository name   |
| `NAME_UNKNOWN`        | 404          | Repository not found      |
| `SIZE_INVALID`        | 416          | Requested range not satisfiable |
| `UNAUTHORIZED`        | 401          | Authentication required   |
| `DENIED`              | 403, 405, or 409 | Access denied by policy, a still-referenced blob (405), or a write rejected such as an immutable tag that cannot be overwritten (409) |
| `UNSUPPORTED`         | 400          | Unsupported operation, and the code carried by any other malformed request |
| `REPLICATION_SUPERSEDED` | 409       | Replication write rejected by last-writer-wins (the local copy is strictly newer) |

The spec fixes the `code` of a 4XX body to the identifiers above; `REPLICATION_SUPERSEDED` is the one exception, and only ever answers a replication write (a manifest `PUT` or `DELETE` carrying `X-Angos-Source-Timestamp`). A 5XX body carries `INTERNAL_ERROR`, `PROVIDER_UNAVAILABLE`, or `RECLAMATION_IN_PROGRESS` (a `503` with `Retry-After`, answering a write that races an active blob reclamation), which the rule does not cover.
