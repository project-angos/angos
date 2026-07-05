---
displayed_sidebar: reference
sidebar_position: 7
title: "Event Webhooks"
---

# Event Webhooks Reference

Event webhooks deliver HTTP POST notifications when registry operations occur.

---

## Configuration

Webhooks are defined in the `[event_webhook.<name>]` section of the configuration file.

### Webhook Definition (`event_webhook.<name>`)

| Option              | Type     | Default  | Description                                      |
|---------------------|----------|----------|--------------------------------------------------|
| `url`               | string   | required | HTTP/HTTPS endpoint URL                          |
| `policy`            | string   | required | Delivery policy: `required`, `optional`, `async` |
| `events`            | [string] | required | Event types to deliver (at least one)            |
| `token`             | string   | -        | Bearer token and HMAC signing secret             |
| `timeout_ms`        | u64      | `5000`   | HTTP request timeout in milliseconds             |
| `max_retries`       | u32      | policy   | Maximum retry attempts after initial failure (max 16); defaults to `3` for `required`, `0` otherwise |
| `repository_filter` | [string] | -        | Regex patterns to match repository names         |

### Webhook References

Webhooks are enabled by referencing their names in global or repository configuration:

| Location                              | Option           | Type     | Description                        |
|---------------------------------------|------------------|----------|------------------------------------|
| `global`                              | `event_webhooks` | [string] | Webhook names for all repositories |
| `repository."<namespace>"`            | `event_webhooks` | [string] | Webhook names for this repository  |

---

## Delivery Semantics

Events are at-least-once notifications of intent: the registry fires them
just before performing the operation. A performed operation is therefore
never left unnotified, while an operation that is rejected or fails after
emission leaves a false-positive event behind. Duplicates are expected, for
example when a client replays an idempotent push or when replication
re-applies a converged write in a multi-replica mesh. Each emission carries
a fresh event `id`, so consumers needing idempotency must key on the payload
(kind, namespace, digest, tag) rather than on the `id`.

---

## Delivery Policies

| Policy     | Behavior                                                                                 |
|------------|------------------------------------------------------------------------------------------|
| `required` | Synchronous. Waits for response. Non-2xx or network failure fails the client operation before it is performed. |
| `optional` | Synchronous. Waits for response. Failure is logged but does not affect the client.       |
| `async`    | Asynchronous. Dispatched in background. Client receives response immediately.            |

### Retry Behavior

Retries apply to `required`, `optional`, and `async` policies when
`max_retries > 0`. Unless set explicitly, `required` webhooks retry 3 times
(a transient endpoint hiccup should not fail the client operation) and the
other policies do not retry.

Backoff formula: `100ms * 2^(attempt - 1)`, capped at 10 seconds

| `max_retries` | Total attempts | Delays               |
|---------------|----------------|----------------------|
| 0             | 1              | -                    |
| 1             | 2              | 100ms                |
| 2             | 3              | 100ms, 200ms         |
| 3             | 4              | 100ms, 200ms, 400ms  |

---

## Event Types

| Event             | Trigger                                          |
|-------------------|--------------------------------------------------|
| `manifest.push`   | Manifest push (client, replication or cache fill) |
| `manifest.pull`   | Manifest served to a client via `GET`            |
| `manifest.delete` | Manifest delete (client or retention)            |
| `blob.push`       | Blob upload completion, a cross-repo mount, or a cache fill |
| `blob.pull`       | Blob served to a client via `GET`                |
| `tag.create`      | Tag created (part of manifest push with tag ref) |
| `tag.delete`      | Tag deleted (part of manifest delete by tag)     |

Pull events fire on successful `GET` requests only (`HEAD` probes are not
eventful) and include responses answered with a `307` redirect to presigned
storage URLs. `manifest.pull` carries the resolved digest, the requested
reference, and the tag when the request was by tag. Pulls are high-volume:
prefer the `async` policy for pull subscriptions, since `required` and
`optional` put a webhook round-trip on the critical path of every pull, and
`required` fails the pull when delivery fails.

---

## Event Payload

Events are delivered as JSON via HTTP POST.

### Schema

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-01-15T10:30:00.123456Z",
  "kind": "manifest.push",
  "namespace": "library/nginx",
  "repository": "docker-hub",
  "digest": "sha256:abc123def456...",
  "reference": "sha256:abc123def456...",
  "tag": "latest",
  "actor": {
    "id": "user-123",
    "username": "alice",
    "client_ip": "192.168.1.100"
  }
}
```

### Fields

| Field       | Type   | Always present | Description                                     |
|-------------|--------|----------------|-------------------------------------------------|
| `id`        | string | yes            | Unique event ID (UUID v4)                       |
| `timestamp` | string | yes            | ISO 8601 timestamp (UTC)                        |
| `kind`      | string | yes            | Event type (see Event Types)                    |
| `namespace` | string | yes            | Image namespace (e.g., `library/nginx`)         |
| `repository`| string | yes            | Repository name                                 |
| `digest`    | string | no             | Content digest (sha256/sha512)                  |
| `reference` | string | no             | Tag or digest reference used in the request     |
| `tag`       | string | no             | Tag name (present for tag operations)           |
| `actor`     | object | no             | Client identity (present when authenticated)    |

### Actor Fields

| Field       | Type   | Description                        |
|-------------|--------|------------------------------------|
| `id`        | string | Identity identifier                |
| `username`  | string | Basic auth or OIDC subject         |
| `client_ip` | string | Client IP address                  |
| `internal`  | string | Name of the internal process that performed the operation (`prune`, `cache`); absent on client-initiated operations |

All actor fields are optional and omitted when not available.

Operations performed by angos itself carry the `internal` field instead of a
client identity: retention enforcement emits `manifest.delete` / `tag.delete`
with `internal = "prune"`, and pull-through cache fills emit `manifest.push` /
`blob.push` with `internal = "cache"`.

---

## HTTP Request

### Method

`POST`

### Headers

| Header                     | Always sent | Description                             |
|----------------------------|-------------|-----------------------------------------|
| `Content-Type`             | yes         | `application/json`                      |
| `X-Registry-Event`         | yes         | Event type (e.g., `manifest.push`)      |
| `Authorization`            | when token set | `Bearer <token>`                     |
| `X-Registry-Signature-256` | when token set | `sha256=<hmac-hex-digest>`           |

### HMAC Signature

When `token` is configured, the payload is signed with HMAC-SHA256:

- **Algorithm**: HMAC-SHA256
- **Key**: The `token` value (UTF-8 encoded)
- **Message**: The raw JSON request body
- **Format**: `sha256=` followed by the hex-encoded digest

The signature is sent in the `X-Registry-Signature-256` header.

**Test vector:**

```
HMAC-SHA256("test-secret", "hello world")
= 046e2496e13e0bfd8dbef84244dd188311a48086646355161bc4ad0769a49cf4
```

Header value: `sha256=046e2496e13e0bfd8dbef84244dd188311a48086646355161bc4ad0769a49cf4`

### Response

Any `2xx` status code is considered success. All other status codes are treated as failure.

---

## Repository Filters

When `repository_filter` is set, events are only delivered if the event's repository name matches at least one regex pattern. Patterns use Rust regex syntax.

| Pattern          | Matches                            |
|------------------|------------------------------------|
| `^production/.*` | `production/api`, `production/web` |
| `^library/.*`    | `library/nginx`, `library/redis`.  |
| `.*`             | Everything                         |

Without `repository_filter`, all repositories match.

---

## Metrics

### event_webhook_deliveries_total

Total event webhook delivery attempts.

| Type    | Labels                       |
|---------|------------------------------|
| Counter | `webhook`, `event`, `result` |

**Labels:**
- `webhook`: Webhook name from configuration
- `event`: Event type (e.g., `manifest.push`)
- `result`: `success` or `error`

### event_webhook_delivery_duration_seconds

Event webhook delivery duration.

| Type      | Labels             |
|-----------|--------------------|
| Histogram | `webhook`, `event` |

**Labels:**
- `webhook`: Webhook name from configuration
- `event`: Event type (e.g., `manifest.push`)

---

## Validation

The following conditions are validated at configuration load time:

- `url` must be a valid URI
- `events` must contain at least one event type
- `token`, when set, must not be empty
- `repository_filter` patterns must be valid regex
- Webhook names referenced in `global.event_webhooks` and `repository.*.event_webhooks` must exist in `[event_webhook.*]`

Invalid configuration is rejected with a descriptive error message.

---

## Hot Reloading

Event webhook configuration is hot-reloaded when the configuration file changes. The new dispatcher replaces the old one atomically. In-flight deliveries on the old dispatcher continue to completion.

Changes that do **not** require restart:
- Adding, removing, or modifying webhook definitions
- Changing webhook references in global or repository configuration

---

## Graceful Shutdown

On shutdown, the dispatcher:
1. Stops accepting new async deliveries
2. Drains in-flight async deliveries to completion

The drain is bounded, not open-ended: every delivery is limited by its own
`timeout_ms`, its capped retry count, and the 10-second backoff ceiling, so an
unresponsive endpoint cannot hang the shutdown. A crash (as opposed to a
graceful shutdown) still loses in-flight async deliveries.
