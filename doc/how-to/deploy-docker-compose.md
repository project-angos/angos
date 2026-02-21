---
displayed_sidebar: howto
sidebar_position: 1
title: "Deploy with Docker Compose"
---

# Deploy with Docker Compose

Deploy Angos using Docker Compose with persistent storage and TLS.

## Prerequisites

- Docker with the Compose plugin installed
- A domain name (for TLS) or self-signed certificates
- Optional: Docker Hub credentials for pull-through cache

## Basic Deployment

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
port = 5000

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
      - "5000:5000"
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
curl http://localhost:5000/v2/
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
port = 5000

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
      - "443:5000"
    volumes:
      - ./config:/config:ro
      - ./data:/data
      - ./certs:/certs:ro
    command: ["-c", "/config/config.toml", "server"]
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "-k", "https://localhost:5000/healthz"]
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
port = 5000

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
      - "5000:5000"
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
port = 5000

[blob_store.fs]
root_dir = "/data"

[metadata_store.fs]
root_dir = "/data"

[metadata_store.fs.redis]
url = "redis://redis:6379"
ttl = 10

[cache.redis]
url = "redis://redis:6379"
```

---

## Scheduled Storage Maintenance

Add a maintenance service to docker-compose.yml:

```yaml
services:
  # ... registry service ...

  maintenance:
    image: ghcr.io/project-angos/angos:latest
    volumes:
      - ./config:/config:ro
      - ./data:/data
    command: ["-c", "/config/config.toml", "scrub", "-t", "-m", "-b", "-r"]
    profiles:
      - maintenance

  # Better use a systemd.timer approach
  maintenance-cron:
    image: ghcr.io/project-angos/angos:latest
    volumes:
      - ./config:/config:ro
      - ./data:/data
    entrypoint: /bin/sh
    command: |
      -c 'while true; do
        sleep 86400
        /angos -c /config/config.toml scrub -t -m -b -r
      done'
    restart: unless-stopped
```

Run manual maintenance:

```bash
docker compose --profile maintenance run --rm maintenance
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
docker tag alpine:latest localhost:5000/test/alpine:latest
docker push localhost:5000/test/alpine:latest

# Test pull-through cache
docker pull localhost:5000/library/nginx:latest
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
