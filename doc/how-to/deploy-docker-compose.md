---
displayed_sidebar: howto
sidebar_position: 1
title: "Deploy with Docker Compose"
---

# Deploy with Docker Compose

Deploy Angos using Docker Compose with persistent storage and TLS.

**Note:** This guide uses filesystem storage for simplicity. For production multi-host deployments, use S3 storage instead (see the "With S3 Locking for Multi-Replica" section below).

## Prerequisites

- Docker with the Compose plugin installed
- A domain name (for TLS) or self-signed certificates
- Optional: Docker Hub credentials for pull-through cache

## Basic Deployment (Development/Testing)

**This basic deployment uses filesystem storage and is suitable for development and testing only.** For production, see the sections below on S3 and multi-replica setups.

### Step 1: Create Configuration

Create a directory for your deployment:

```bash
mkdir -p registry/{config,data,certs}
cd registry
```

Create `config/config.toml`:

```toml
[server]
bind_address = "0.0.0.0"
port = 8000

[blob_store.fs]
root_dir = "/data"

[ui]
enabled = true
name = "My Registry"
```

### Step 2: Create docker-compose.yml

```yaml
version: '3.8'

services:
  registry:
    image: ghcr.io/project-angos/angos:latest
    ports:
      - "8000:8000"
    volumes:
      - ./config:/config:ro
      - ./data:/data
    command: ["-c", "/config/config.toml", "server"]
    restart: unless-stopped
```

### Step 3: Start the Registry

```bash
docker compose up -d
```

### Step 4: Verify

```bash
curl http://localhost:8000/v2/
```

---

## Production Deployment with TLS

### Step 1: Obtain Certificates

Place your certificates in the `certs` directory:
- `server.crt` - Server certificate
- `server.key` - Server private key

For testing, generate self-signed certificates:

```bash
openssl req -x509 -newkey rsa:4096 -keyout certs/server.key \
  -out certs/server.crt -days 365 -nodes \
  -subj "/CN=registry.example.com"
```

### Step 2: Update Configuration

Update `config/config.toml`:

```toml
[server]
bind_address = "0.0.0.0"
port = 8000

[server.tls]
server_certificate_bundle = "/certs/server.crt"
server_private_key = "/certs/server.key"

[global]
max_concurrent_requests = 8

[blob_store.fs]
root_dir = "/data"

[ui]
enabled = true
name = "My Registry"
```

### Step 3: Update docker-compose.yml

```yaml
version: '3.8'

services:
  registry:
    image: ghcr.io/project-angos/angos:latest
    ports:
      - "443:8000"
    volumes:
      - ./config:/config:ro
      - ./data:/data
      - ./certs:/certs:ro
    command: ["-c", "/config/config.toml", "server"]
    restart: unless-stopped
    healthcheck:
      # /healthz returns 200 when the registry is ready to serve requests
      test: ["CMD", "curl", "-f", "-k", "https://localhost:8000/healthz"]
      interval: 30s
      timeout: 10s
      retries: 3
```

---

## With Pull-Through Cache

### Configuration

```toml
[server]
bind_address = "0.0.0.0"
port = 8000

[server.tls]
server_certificate_bundle = "/certs/server.crt"
server_private_key = "/certs/server.key"

[global]
max_concurrent_cache_jobs = 8

[blob_store.fs]
root_dir = "/data"

# Docker Hub
[repository."library"]
immutable_tags = true
immutable_tags_exclusions = ["^latest$"]

[[repository."library".upstream]]
url = "https://registry-1.docker.io"
# Add credentials for higher rate limits
# username = "your-dockerhub-username"
# password = "your-dockerhub-password"

# GitHub Container Registry
[repository."ghcr.io"]
immutable_tags = true

[[repository."ghcr.io".upstream]]
url = "https://ghcr.io"

[ui]
enabled = true
```

---

## With Redis for Multi-Replica

### docker-compose.yml

```yaml
version: '3.8'

services:
  registry:
    image: ghcr.io/project-angos/angos:latest
    ports:
      - "8000:8000"
    volumes:
      - ./config:/config:ro
      - ./data:/data
    command: ["-c", "/config/config.toml", "server"]
    depends_on:
      - redis
    restart: unless-stopped
    deploy:
      replicas: 2

  redis:
    image: redis:7-alpine
    volumes:
      - redis-data:/data
    restart: unless-stopped

volumes:
  redis-data:
```

### Configuration

```toml
[server]
bind_address = "0.0.0.0"
port = 8000

[blob_store.fs]
root_dir = "/data"

[metadata_store.fs]
root_dir = "/data"

[metadata_store.fs.lock_strategy.redis]
url = "redis://redis:6379"
ttl = 10

[cache.redis]
url = "redis://redis:6379"
```

---

## With S3 Locking for Multi-Replica

If your S3 provider supports conditional writes, you can run multiple replicas without Redis by using S3-based locking.

### docker-compose.yml

```yaml
version: '3.8'

services:
  registry:
    image: ghcr.io/project-angos/angos:latest
    ports:
      - "8000:8000"
    volumes:
      - ./config:/config:ro
    command: ["-c", "/config/config.toml", "server"]
    restart: unless-stopped
    deploy:
      replicas: 2
```

### Configuration

```toml
[server]
bind_address = "0.0.0.0"
port = 8000

[blob_store.s3]
bucket = "my-registry"
endpoint = "https://s3.amazonaws.com"
region = "us-east-1"

[metadata_store.s3]
bucket = "my-registry"
endpoint = "https://s3.amazonaws.com"
region = "us-east-1"

[metadata_store.s3.lock_strategy.s3]
ttl_secs = 30
max_retries = 100
retry_delay_ms = 50
```

At startup, Angos probes the S3 provider to verify conditional write support. If the probe fails, check that your provider supports `If-None-Match` headers or fall back to the Redis-based setup above.

---

## Scheduled Storage Maintenance

For scheduled maintenance (scrub), use your system's cron scheduler or a dedicated cron container:

```bash
# Run manual maintenance with Docker Compose
docker compose run --rm registry /angos -c /config/config.toml scrub --tags --manifests --blobs --retention
```

**Cron scheduling approaches:**

1. **System cron (recommended):**
   ```bash
   # Add to crontab -e: run scrub daily at 3 AM
   0 3 * * * cd /path/to/registry && docker compose run --rm registry /angos -c /config/config.toml scrub --tags --manifests --blobs --retention
   ```

2. **Docker container cron (ofelia):**
   Add to docker-compose.yml:
   ```yaml
   services:
     ofelia:
       image: mcuadros/ofelia:latest
       volumes:
         - /var/run/docker.sock:/var/run/docker.sock
       command: daemon --docker

     # Add to registry service:
     # labels:
     #   ofelia.enabled: "true"
     #   ofelia.job-exec.registry-scrub.schedule: "@daily"
     #   ofelia.job-exec.registry-scrub.command: "/angos -c /config/config.toml scrub --tags --manifests --blobs --retention"
   ```

---

## Verification

```bash
# Check service status
docker compose ps

# View logs
docker compose logs -f registry

# Test push
docker pull alpine:latest
docker tag alpine:latest localhost:8000/test/alpine:latest
docker push localhost:8000/test/alpine:latest

# Test pull-through cache
docker pull localhost:8000/library/nginx:latest
```

---

## Troubleshooting

**Container won't start:**
```bash
docker compose logs registry
```

**Permission denied on volumes:**
```bash
sudo chown -R 1000:1000 data/
```

**TLS certificate errors:**
```bash
# Verify certificate
openssl x509 -in certs/server.crt -text -noout
```

## Next Steps

- [Configure mTLS](configure-mtls.md) for client certificate authentication
- [Set Up Access Control](set-up-access-control.md) for policy-based authorization
- [Configure Retention Policies](configure-retention-policies.md) for automated cleanup
