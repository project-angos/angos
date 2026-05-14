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

### API Version Check

```
GET /v2/
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
- A range whose start is at or beyond the blob length, or a zero-length suffix range, returns `416 Range Not Satisfiable`.
- For an empty blob, Angos ignores a syntactically valid range and returns the normal `200 OK` empty body.

Range requests for pull-through repositories are supported only after the blob is available locally. A range request for an uncached pull-through blob returns `416`; request the full blob first to populate the cache.

```
DELETE /v2/{namespace}/blobs/{digest}
```

Delete a blob owned by the namespace. If the digest is still referenced by manifest metadata in
that namespace, Angos returns `DENIED` and leaves the blob unchanged. After those references are
removed, deleting the blob removes that namespace's ownership; the underlying blob data is removed
only when no namespace references the digest.

### Blob Upload

```
POST /v2/{namespace}/blobs/uploads/
```

Start a new blob upload. Returns `202 Accepted` with `Location` header.

Query parameters:
- `digest` - Return the existing blob only when the requested namespace already owns it; otherwise
  start a new upload session.
- `mount` - Mount blob from another repository

```
GET /v2/{namespace}/blobs/uploads/{uuid}
```

Get upload status.

```
PATCH /v2/{namespace}/blobs/uploads/{uuid}
```

Upload a chunk. Use `Content-Range` for the chunk range and `Content-Length` for the exact chunk size. Missing or invalid `Content-Length` returns `400 Bad Request`.

```
PUT /v2/{namespace}/blobs/uploads/{uuid}?digest={digest}
```

Complete the upload with final digest. If a final chunk is included, `Content-Length` must contain that chunk size. Without a final chunk, missing `Content-Length` is treated as zero.

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

```
PUT /v2/{namespace}/manifests/{reference}
```

Push a manifest. Manifest bodies larger than `global.max_manifest_size` are rejected with `MANIFEST_INVALID`.

```
DELETE /v2/{namespace}/manifests/{reference}
```

Delete a manifest by tag or digest. Deleting by tag removes only that tag. Deleting by digest also
removes tags pointing at the digest and removes the manifest body when no remaining namespace
references it. Config and layer blobs remain owned by the namespace until they are deleted through
the blob endpoint or scrubbed as orphans.

### Tags

```
GET /v2/{namespace}/tags/list
```

List tags for a namespace.

Query parameters:
- `n` - Maximum number of results
- `last` - Pagination marker

### Catalog

```
GET /v2/_catalog
```

List repositories.

Query parameters:
- `n` - Maximum number of results
- `last` - Pagination marker

### Referrers

```
GET /v2/{namespace}/referrers/{digest}
```

List manifests that reference a subject digest.

Query parameters:
- `artifactType` - Filter by artifact type

---

## Extension API (not part of the OCI specification)

Base path: `/v2/_ext/`

### List Repositories

```
GET /v2/_ext/_repositories
```

List all configured repositories with namespace counts.

**Response:**
```json
{
  "repositories": [
    {
      "name": "library",
      "namespaces": 15,
      "is_pull_through": true,
      "immutable_tags": true
    }
  ]
}
```

### List Namespaces

```
GET /v2/_ext/{repository}/_namespaces
```

List namespaces within a repository.

**Response:**
```json
{
  "namespaces": [
    {
      "name": "nginx",
      "manifests": 25,
      "uploads": 0
    }
  ]
}
```

### List Revisions

```
GET /v2/_ext/{namespace}/_revisions
```

List all manifest revisions with tags and parent relationships.

**Response:**
```json
{
  "revisions": [
    {
      "digest": "sha256:abc123...",
      "media_type": "application/vnd.oci.image.index.v1+json",
      "tags": ["latest", "1.25.0"],
      "parent": null,
      "pushed_at": 1703123456,
      "last_pulled_at": 1703200000
    }
  ]
}
```

### List Uploads

```
GET /v2/_ext/{namespace}/_uploads
```

List blob uploads in progress.

**Response:**
```json
{
  "uploads": [
    {
      "uuid": "123e4567-e89b-12d3-a456-426614174000",
      "size": 1048576,
      "started_at": 1703123456
    }
  ]
}
```

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

Returns `200 OK` if the storage backend is healthy and ready to handle requests. Checks accessibility of the blob store, metadata store, and lock backend.

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
GET /_ui/config
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

All endpoints (except `/healthz` and `/readyz`) require authentication when access policies are configured.

### Methods

**Basic Authentication:**
```
Authorization: Basic base64(username:password)
```

**Bearer Token (OIDC):**
```
Authorization: Bearer <jwt-token>
```

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
3. Client retries with credentials
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
| `BLOB_UPLOAD_INVALID` | 400          | Invalid upload            |
| `BLOB_UPLOAD_UNKNOWN` | 404          | Upload session not found  |
| `DIGEST_INVALID`      | 400          | Invalid digest format     |
| `MANIFEST_INVALID`    | 400          | Invalid manifest content  |
| `MANIFEST_UNKNOWN`    | 404          | Manifest does not exist   |
| `NAME_INVALID`        | 400          | Invalid repository name   |
| `NAME_UNKNOWN`        | 404          | Repository not found      |
| `SIZE_INVALID`        | 400          | Size mismatch             |
| `TAG_INVALID`         | 400          | Invalid tag               |
| `TAG_IMMUTABLE`       | 409          | Tag cannot be overwritten |
| `UNAUTHORIZED`        | 401          | Authentication required   |
| `DENIED`              | 403 or 405   | Access denied by policy, or blob is still referenced |
| `UNSUPPORTED`         | 415          | Unsupported operation     |
