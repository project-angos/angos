---
displayed_sidebar: reference
sidebar_position: 6
title: "Metrics"
---

# Metrics Reference

Angos exposes Prometheus metrics at the `/metrics` endpoint. The endpoint is authenticated
and authorized like every other route, so with the default deny policy a scraper is refused
until a rule allows the `metrics` action, anonymously or for the identity the scraper presents:

```toml
[global.access_policy]
rules = ["request.action == 'metrics'"]
```

See [Set Up Access Control](../how-to/set-up-access-control.md#health-and-metrics).

---

## HTTP Metrics

### http_requests_total

Total number of HTTP requests.

| Type    | Labels                      |
|---------|-----------------------------|
| Counter | `method`, `route`, `status` |

**Labels:**
- `method`: HTTP method (`GET`, `POST`, `PUT`, `DELETE`, etc.); any other method is `other`
- `route`: Route action (e.g., `get-manifest`, `get-blob`, `list-tags`)
- `status`: HTTP status code (`200`, `404`, `500`, etc.)

**Example:**
```promql
# Request rate over 5 minutes
rate(http_requests_total[5m])

# Error rate (5xx responses)
rate(http_requests_total{status=~"5.."}[5m])

# Requests by route
sum by (route) (rate(http_requests_total[5m]))

# GET requests for manifests
rate(http_requests_total{method="GET", route="get-manifest"}[5m])
```

### http_request_duration_ms

HTTP request latency in milliseconds.

| Type      | Labels            |
|-----------|-------------------|
| Histogram | `method`, `route` |

Buckets: 1, 5, 10, 25, 50, 100, 250 and 500 ms, then 1, 2.5, 5, 10, 30 and 60 s.

**Example:**
```promql
# 95th percentile latency
histogram_quantile(0.95, rate(http_request_duration_ms_bucket[5m]))

# Average latency
rate(http_request_duration_ms_sum[5m]) / rate(http_request_duration_ms_count[5m])

# Latency by route
histogram_quantile(0.99, sum by (route, le) (rate(http_request_duration_ms_bucket[5m])))

# Manifest pull latency
histogram_quantile(0.95, rate(http_request_duration_ms_bucket{route="get-manifest"}[5m]))
```

### http_requests_in_flight

Current number of open HTTP connections, counted from the completed handshake to the close. An
idle keep-alive connection counts, so the gauge tracks connections, not requests.

| Type  | Labels |
|-------|--------|
| Gauge | none   |

**Example:**
```promql
# Current open connections
http_requests_in_flight

# Max open connections over time
max_over_time(http_requests_in_flight[1h])
```

### Route Values

The `route` label is the endpoint name. It is close to the action name an access policy sees (see [CEL Expressions](cel-expressions.md)) but not identical: the version check, `HEAD` requests and the upload steps are told apart here.

| Route                | Description |
|----------------------|-------------|
| `healthz`            | Health check |
| `readyz`             | Readiness check |
| `metrics`            | Prometheus metrics |
| `check-version`      | API version check, `GET /v2/` |
| `get-manifest`       | Pull manifest |
| `head-manifest`      | Manifest existence check |
| `put-manifest`       | Push manifest |
| `delete-manifest`    | Delete manifest |
| `get-blob`           | Download blob |
| `head-blob`          | Blob existence check |
| `delete-blob`        | Delete blob |
| `start-upload`       | Start blob upload |
| `mount-blob`         | Cross-repo blob mount |
| `get-upload`         | Upload status |
| `patch-upload`       | Chunk upload |
| `put-upload`         | Complete upload |
| `delete-upload`      | Cancel upload |
| `list-tags`          | List tags |
| `get-referrers`      | Get referrers |
| `list-catalog`       | List repositories, `/v2/_catalog` |
| `ui-asset`           | UI static files |
| `ui-config`          | UI configuration |
| `get-token`          | Token service |
| `list-repositories`  | Extension API |
| `list-namespaces`    | Extension API |
| `list-revisions`     | Extension API |
| `list-uploads`       | Extension API |
| `list-pulls`         | Extension API, the pull-history endpoint |
| `list-layer-entries` | Extension API, the entries of a layer |
| `get-layer-file`     | Extension API, one file out of a layer |
| `list-jobs`          | List pending jobs |
| `list-failed-jobs`   | List dead-letter jobs |
| `retry-job`          | Requeue dead-letter job |
| `delete-job`         | Delete queued job |
| `unknown`            | Unrecognized route |
---

## Authentication Metrics

### auth_attempts_total

Total number of authentication attempts.

| Type    | Labels             |
|---------|--------------------|
| Counter | `method`, `result` |

**Labels:**
- `method`: `basic`, `mtls`, `oidc`, `token`
- `result`: `success`, `failed`

**Example:**
```promql
# Authentication success rate
sum(rate(auth_attempts_total{result="success"}[5m])) /
sum(rate(auth_attempts_total[5m]))

# Failed auth attempts by method
sum by (method) (rate(auth_attempts_total{result="failed"}[5m]))
```

---

## Webhook Metrics

### webhook_authorization_requests_total

Total webhook authorization requests.

| Type    | Labels              |
|---------|---------------------|
| Counter | `webhook`, `result` |

**Labels:**
- `webhook`: Name of the webhook
- `result`: `allow` and `deny` for a `2xx` or a `401`/`403` answer; `unavailable` when the
  webhook answered any other status and `transport_error` when it could not be reached. Both
  of the last two deny the request. Every request is authorized by the webhook, so this counter
  also gives the webhook's request rate.

**Example:**
```promql
# Webhook hit rate
sum by (webhook) (rate(webhook_authorization_requests_total[5m]))

# Denial rate by webhook
sum by (webhook) (rate(webhook_authorization_requests_total{result="deny"}[5m]))

# Fail-closed denials: the webhook is down or misbehaving
sum by (webhook) (rate(webhook_authorization_requests_total{result=~"unavailable|transport_error"}[5m]))
```

### webhook_authorization_duration_seconds

Webhook authorization request duration.

| Type      | Labels    |
|-----------|-----------|
| Histogram | `webhook` |

**Example:**
```promql
# 95th percentile webhook latency
histogram_quantile(0.95, rate(webhook_authorization_duration_seconds_bucket[5m]))

# Slow webhook detection (> 1s)
rate(webhook_authorization_duration_seconds_bucket{le="1"}[5m])
```

---

## Event Webhook Metrics

### event_webhook_deliveries_total

Total event webhook delivery attempts.

| Type    | Labels                       |
|---------|------------------------------|
| Counter | `webhook`, `event`, `result` |

**Labels:**
- `webhook`: Webhook name from configuration
- `event`: Event type (e.g., `manifest.push`)
- `result`: `success` or `error`

**Example:**
```promql
# Delivery rate by webhook and result
sum by (webhook, result) (rate(event_webhook_deliveries_total[5m]))

# Error rate for a specific webhook
rate(event_webhook_deliveries_total{webhook="audit", result="error"}[5m])
```

### event_webhook_delivery_duration_seconds

Event webhook delivery duration.

| Type      | Labels             |
|-----------|--------------------|
| Histogram | `webhook`, `event` |

**Example:**
```promql
# P95 delivery latency
histogram_quantile(0.95, rate(event_webhook_delivery_duration_seconds_bucket[5m]))

# Delivery latency by webhook
histogram_quantile(0.95, sum by (webhook, le) (rate(event_webhook_delivery_duration_seconds_bucket[5m])))
```

---

## Pull-Through Cache Metrics

### angos_pull_through_total

Manifest and blob pulls served by a repository that mirrors an upstream, by
outcome. A repository with no `[[repository."name".upstream]]` caches nothing
and records nothing, so the counter covers pull-through traffic alone. See
[Pull-Through Caching](../explanation/pull-through-caching.md).

| Type    | Labels                            |
|---------|-----------------------------------|
| Counter | `repository`, `kind`, `outcome`   |

**Labels:**
- `repository`: the `[repository."name"]` entry serving the pull
- `kind`: `manifest` or `blob`
- `outcome`: `hit` when the stored copy answered the pull; `miss` when nothing
  was stored, so the content came from the upstream; `refresh` when a mutable
  tag was stored but the upstream has since re-pointed it, so the manifest was
  refetched. Only a manifest refreshes: a blob is content-addressed, so a
  stored one is never re-checked.

Each pull records exactly one outcome, `HEAD` and `GET` alike. A `hit` on a
mutable tag still costs the upstream a `HEAD` to confirm the digest; an
`immutable_tags` entry skips that check, which is what makes a cached tag free.

Every outcome series of a repository with an upstream exists at zero from
startup and from a configuration reload, so the first pull after a restart is
a step that `rate` and `increase` see.

**Example:**
```promql
# Cache hit ratio per repository
sum by (repository) (rate(angos_pull_through_total{outcome="hit"}[5m])) /
sum by (repository) (rate(angos_pull_through_total[5m]))

# Pulls that reached the upstream, the rate a remote registry sees
sum by (repository) (rate(angos_pull_through_total{outcome=~"miss|refresh"}[5m]))

# Manifest refreshes: mutable tags moving upstream
sum by (repository) (rate(angos_pull_through_total{kind="manifest", outcome="refresh"}[5m]))
```

A blob `miss` also enqueues a cache-fill job, so it is followed by an
`angos_job_queue_enqueued_total{queue="cache"}` increment (`dedup="hit"` when a
fill for that blob was already queued).

---

## Job Queue Metrics

`angos_job_queue_pending` and `angos_job_queue_failed` are published only when
`[global.job_queue]` is configured; the two counters below increment on every
enqueue (the in-process queue included) and are always exposed on the server's
`/metrics`. See [Enable Durable Cache Jobs](../how-to/durable-cache-jobs.md).

### angos_job_queue_pending

Pending jobs that are **ready for handling within the readiness horizon**
(`not_before ≤ now + pending_ready_horizon_secs`, default 600 seconds).
Suitable for KEDA-style autoscaling of `angos worker` pods. Refreshed by a
background ticker on every server replica.

Envelopes backed off further into the future are deliberately excluded so the
gauge tracks *actionable* work: spinning up workers for jobs that won't be
claimable for an hour wastes capacity. Tune `pending_ready_horizon_secs`
(under `[global.job_queue]`) to give your autoscaler enough lead time to spin
up replicas before the work becomes ready.

The gauge **saturates at 10 000**: any value at the cap should be read as
"≥ 10 000". Combined with the readiness-horizon filter, this bounds the S3
`LIST` cost per refresh tick to ~10 paginated calls regardless of queue depth.
KEDA's `ScaledObject` only needs ordinal granularity above its scale-to-max
threshold, which is normally well below this cap.

| Type  | Labels  |
|-------|---------|
| Gauge | `queue` |

### angos_job_queue_failed

Dead-lettered jobs currently held in the queue (jobs that exhausted their retry
budget). Refreshed by the same server-side background ticker as
`angos_job_queue_pending`, so it stays scrapeable even when `angos worker`
drains the queue. Saturates at 10 000 like the pending gauge. Alert on
`angos_job_queue_failed{queue="replication"} > 0` to catch stuck replication.

| Type  | Labels  |
|-------|---------|
| Gauge | `queue` |

### angos_job_queue_enqueued_total

Total jobs submitted to the queue.

| Type    | Labels             |
|---------|--------------------|
| Counter | `queue`, `dedup`   |

**Labels:**
- `queue`: queue name (e.g. `cache`, `replication`)
- `dedup`: `hit` when a duplicate `lock_key` was suppressed, otherwise `miss`

---

### angos_job_queue_enqueue_failures_total

Total enqueue attempts that did not land on the queue (envelope build or storage error).

| Type    | Labels  |
|---------|---------|
| Counter | `queue` |

**Labels:**
- `queue`: queue name (e.g. `cache`, `replication`)

---

## Replication Metrics

The `angos_job_queue_pending{queue="replication"}` gauge (above) reports
replication backlog depth; the metrics below cover push outcomes, staleness,
and `angos reconcile replication` reconciliation. See
[Bi-Directional Replication](../explanation/replication.md).

`angos_replication_push_total` and
`angos_replication_last_success_timestamp_seconds` increment in the process
that drains the replication queue. Without `[global.job_queue]` the server
drains the queue in-process, so both appear on the server's `/metrics`. With
`[global.job_queue]` the queue is drained by `angos worker`, which exposes no
HTTP endpoint, so in that mode these two metrics are not scrapeable; use the
server-published `angos_job_queue_failed{queue="replication"}` gauge to alert on
stuck replication and `angos_job_queue_pending{queue="replication"}` for
backlog.

### angos_replication_push_total

Total replication pushes to a downstream, by outcome.

| Type    | Labels                  |
|---------|-------------------------|
| Counter | `downstream`, `outcome` |

**Labels:**
- `downstream`: the configured downstream `name`
- `outcome`: `pushed` (manifest/blobs transferred, or a delete applied), `converged` (the downstream already matched, a push whose digest was already present, or a delete whose target was already absent so nothing transferred), `superseded` (downstream already held a newer copy, last-writer-wins, counted as success), `unsupported` (the downstream rejected the delete method with `405`, e.g. it does not support tag deletion; the job completes without converging rather than dead-lettering), or `failed` (the push errored and the job will retry)

**Example:**
```promql
# Push rate by downstream and outcome
sum by (downstream, outcome) (rate(angos_replication_push_total[5m]))

# Replication failure rate
sum by (downstream) (rate(angos_replication_push_total{outcome="failed"}[5m]))
```

### angos_replication_last_success_timestamp_seconds

Unix timestamp (seconds) of the last `pushed`, `converged`, or `superseded` replication push per downstream (the convergent outcomes set it; `unsupported` and `failed` do not). Use it to detect a stalled downstream.

| Type  | Labels       |
|-------|--------------|
| Gauge | `downstream` |

**Example:**
```promql
# Seconds since the last successful push (staleness) per downstream
time() - angos_replication_last_success_timestamp_seconds
```

### angos_replication_reconcile_total

Replication reconcile enqueues emitted by `angos reconcile replication`, by outcome.

| Type    | Labels    |
|---------|-----------|
| Counter | `outcome` |

**Labels:**
- `outcome`: `enqueued` (a divergence was enqueued: a push, or a prune delete for a `prune = true` downstream), `failed` (the envelope build or enqueue errored), or `skipped` (a downstream HEAD probe failed, e.g. auth rejection, 5xx, or timeout, so the tag stays unreconciled this pass; a persistently non-zero `skipped` with zero `enqueued` typically means bad downstream credentials)

This counter lives in the `angos reconcile replication` process, which serves no `/metrics`
endpoint and exits when the run completes, so Prometheus cannot scrape it. The
warn-level log lines emitted for failed and skipped tags are the operational
signal: watch the replicate run's logs (or its exit status) rather than this
counter.

### Replication Backlog

Replication shares the durable job queue, so backlog depth is reported by
`angos_job_queue_pending{queue="replication"}` (see [Job Queue Metrics](#job-queue-metrics)).

```promql
# Pending replication pushes
angos_job_queue_pending{queue="replication"}
```

A deep replication queue is the normal state during a downstream outage and does
**not** affect `/readyz`. Alert on sustained backlog instead. When the server
drains the queue in-process (no `[global.job_queue]`), the staleness gauge is
also on the server's `/metrics` and supports a staleness alert:

```promql
# Replication stale for over 10 minutes (in-process drain only)
(time() - angos_replication_last_success_timestamp_seconds) > 600
```

With a separate `angos worker`, the gauge is not scrapeable; alert on the
server-published `angos_job_queue_failed{queue="replication"}` dead-letter gauge
(stuck pushes) and the `angos_job_queue_pending{queue="replication"}` backlog
instead.

```promql
# Stuck replication pushes (dead-lettered)
angos_job_queue_failed{queue="replication"} > 0
```

---

## Example Prometheus Configuration

```yaml
scrape_configs:
  - job_name: 'angos'
    static_configs:
      - targets: ['registry:8000']
    metrics_path: /metrics
    scheme: http  # or https
    # With a policy that allows `metrics` only to an identity, present it:
    # basic_auth:
    #   username: prometheus
    #   password_file: /etc/prometheus/angos-password
```

---

## Example Grafana Dashboard Queries

### Overview

```promql
# Request rate
sum(rate(http_requests_total[5m]))

# Error rate percentage
100 * sum(rate(http_requests_total{status=~"5.."}[5m])) /
sum(rate(http_requests_total[5m]))

# P95 latency
histogram_quantile(0.95, sum(rate(http_request_duration_ms_bucket[5m])) by (le))

# Request rate by route
sum by (route) (rate(http_requests_total[5m]))

# Manifest operations latency
histogram_quantile(0.95, sum(rate(http_request_duration_ms_bucket{route=~".*-manifest"}[5m])) by (le))
```

### Authentication

```promql
# Auth success rate
100 * sum(rate(auth_attempts_total{result="success"}[5m])) /
sum(rate(auth_attempts_total[5m]))

# Auth method distribution
sum by (method) (rate(auth_attempts_total[5m]))
```

### Authorization Webhooks

```promql
# Webhook error rate (denials)
100 * sum(rate(webhook_authorization_requests_total{result="deny"}[5m])) /
sum(rate(webhook_authorization_requests_total[5m]))
```

### Event Webhooks

```promql
# Event webhook delivery rate
sum by (webhook, result) (rate(event_webhook_deliveries_total[5m]))

# Event webhook error rate
100 * sum(rate(event_webhook_deliveries_total{result="error"}[5m])) /
sum(rate(event_webhook_deliveries_total[5m]))

# Event webhook P95 latency
histogram_quantile(0.95, sum by (webhook, le) (rate(event_webhook_delivery_duration_seconds_bucket[5m])))
```

---

## Alerting Examples

```yaml
groups:
  - name: angos
    rules:
      - alert: HighErrorRate
        expr: |
          sum(rate(http_requests_total{status=~"5.."}[5m])) /
          sum(rate(http_requests_total[5m])) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate on Angos"

      - alert: HighLatency
        expr: |
          histogram_quantile(0.95, sum(rate(http_request_duration_ms_bucket[5m])) by (le)) > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High latency on Angos"

      - alert: AuthFailures
        expr: |
          sum(rate(auth_attempts_total{result="failed"}[5m])) > 10
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High authentication failure rate"
```
