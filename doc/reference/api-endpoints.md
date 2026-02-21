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

Check existence or download a blob by digest.

```
DELETE /v2/{namespace}/blobs/{digest}
```

Delete a blob.

### Blob Upload

```
POST /v2/{namespace}/blobs/uploads/
```

Start a new blob upload. Returns `202 Accepted` with `Location` header.

Query parameters:
- `digest` - Complete upload in single request (monolithic)
- `mount` - Mount blob from another repository

```
GET /v2/{namespace}/blobs/uploads/{uuid}
```

Get upload status.

```
PATCH /v2/{namespace}/blobs/uploads/{uuid}
```

Upload a chunk. Use `Content-Range` header for chunked uploads.

```
PUT /v2/{namespace}/blobs/uploads/{uuid}?digest={digest}
```

Complete the upload with final digest.

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

Push a manifest.

```
DELETE /v2/{namespace}/manifests/{reference}
```

Delete a manifest by tag or digest.

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

### Health Check

```
GET /healthz
```

Returns `200 OK` if the service is healthy.

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

All endpoints (except `/healthz`) require authentication when access policies are configured.

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
| `DENIED`              | 403          | Access denied by policy   |
| `UNSUPPORTED`         | 415          | Unsupported operation     |
