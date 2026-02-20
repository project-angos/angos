---
displayed_sidebar: reference
sidebar_position: 6
title: "Metrics"
---

# Metrics Reference

Angos exposes Prometheus metrics at the `/metrics` endpoint.

---

## HTTP Metrics

### http_requests_total

Total number of HTTP requests.

| Type    | Labels                      |
|---------|-----------------------------|
| Counter | `method`, `route`, `status` |

**Labels:**
- `method`: HTTP method (`GET`, `POST`, `PUT`, `DELETE`, etc.)
- `route`: Route action (e.g., `get-manifest`, `put-blob`, `list-tags`)
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

Current number of HTTP requests being processed.

| Type  | Labels |
|-------|--------|
| Gauge | none   |

**Example:**
```promql
# Current in-flight requests
http_requests_in_flight

# Max in-flight over time
max_over_time(http_requests_in_flight[1h])
```

### Route Values

The `route` label uses action names from the OCI Distribution API:

| Route               | Description        |
|---------------------|--------------------|
| `healthz`           | Health check       |
| `metrics`           | Prometheus metrics |
| `get-api-version`   | API version check  |
| `get-blob`          | Download blob      |
| `delete-blob`       | Delete blob        |
| `start-upload`      | Start blob upload  |
| `update-upload`     | Chunk upload       |
| `complete-upload`   | Complete upload    |
| `get-upload`        | Upload status      |
| `cancel-upload`     | Cancel upload      |
| `get-manifest`      | Pull manifest      |
| `put-manifest`      | Push manifest      |
| `delete-manifest`   | Delete manifest    |
| `list-tags`         | List tags          |
| `list-catalog`      | List repositories  |
| `get-referrers`     | Get referrers      |
| `ui-asset`          | UI static files    |
| `ui-config`         | UI configuration   |
| `list-repositories` | Extension API      |
| `list-namespaces`   | Extension API      |
| `list-revisions`    | Extension API      |
| `list-uploads`      | Extension API      |
| `unknown`           | Unrecognized route |

---

## Authentication Metrics

### auth_attempts_total

Total number of authentication attempts.

| Type    | Labels             |
|---------|--------------------|
| Counter | `method`, `result` |

**Labels:**
- `method`: `basic`, `mtls`, `oidc`
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
- `result`: `allow`, `deny`, `cached_allow`, `cached_deny`

**Example:**
```promql
# Webhook hit rate
sum by (webhook) (rate(webhook_authorization_requests_total[5m]))

# Cache effectiveness
sum(rate(webhook_authorization_requests_total{result=~"cached_.*"}[5m])) /
sum(rate(webhook_authorization_requests_total[5m]))

# Denial rate by webhook
sum by (webhook) (rate(webhook_authorization_requests_total{result=~".*deny"}[5m]))
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

## Example Prometheus Configuration

```yaml
scrape_configs:
  - job_name: 'angos'
    static_configs:
      - targets: ['registry:5000']
    metrics_path: /metrics
    scheme: http  # or https
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
# Webhook cache hit rate
100 * sum(rate(webhook_authorization_requests_total{result=~"cached_.*"}[5m])) /
sum(rate(webhook_authorization_requests_total[5m]))

# Webhook error rate (denials)
100 * sum(rate(webhook_authorization_requests_total{result=~".*deny"}[5m])) /
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
