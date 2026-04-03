---
displayed_sidebar: howto
sidebar_position: 2
title: "Deploy on Kubernetes"
---

# Deploy on Kubernetes

Deploy Angos on Kubernetes as a stateless service. Angos requires only configuration and storage, the binary itself is stateless.

## Prerequisites

- Kubernetes cluster
- `kubectl` configured
- S3-compatible storage (AWS S3, MinIO, etc.) for production deployments
- Optional: Redis for distributed locking and caching in multi-replica deployments
- Optional: Ingress controller for external access

---

## Quick Start with Kustomize

### Step 1: Clone the Repository

```bash
git clone https://github.com/project-angos/angos.git
cd angos
```

### Step 2: Choose a TLS termination approach

```bash
# TLS terminated by ingress controller
kubectl apply -k contrib/kubernetes/kustomize/overlays/simple

# TLS passthrough (required for mTLS policy enforcement)
kubectl apply -k contrib/kubernetes/kustomize/overlays/tls

# TLS passthrough with Traefik
kubectl apply -k contrib/kubernetes/kustomize/overlays/tls-traefik
```

---

## Recommended Deployment (S3 + Stateless)

This is the production-ready pattern: stateless Deployment + S3 storage backend.

### Step 1: Create Namespace

```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: registry
```

### Step 2: Create Secret with Configuration

The configuration file contains S3 credentials. Since ConfigMaps are not encrypted at rest, store it as a Kubernetes Secret instead. Consider enabling [encryption at rest](https://kubernetes.io/docs/tasks/administer-cluster/encrypt-data/) for Secrets in your cluster.

```toml
# config.toml
[server]
bind_address = "0.0.0.0"
port = 8000

[blob_store.s3]
bucket = "my-registry-bucket"
endpoint = "https://s3.amazonaws.com"
region = "us-east-1"
access_key_id = "YOUR_ACCESS_KEY"
secret_key = "YOUR_SECRET_KEY"

[metadata_store.s3]
bucket = "my-registry-bucket"
endpoint = "https://s3.amazonaws.com"
region = "us-east-1"
access_key_id = "YOUR_ACCESS_KEY"
secret_key = "YOUR_SECRET_KEY"
link_cache_ttl = 30
access_time_debounce_secs = 60

[metadata_store.s3.lock_strategy.s3]
ttl_secs = 30

[ui]
enabled = true
name = "My Registry"
```

```bash
kubectl create secret generic registry-config \
  --namespace registry \
  --from-file=config.toml=config.toml
```

### Step 3: Create stateless Deployment

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: registry
  namespace: registry
spec:
  replicas: 3
  selector:
    matchLabels:
      app: registry
  template:
    metadata:
      labels:
        app: registry
    spec:
      containers:
        - name: registry
          image: ghcr.io/project-angos/angos:latest
          args: ["-c", "/config/config.toml", "server"]
          ports:
            - containerPort: 8000
          volumeMounts:
            - name: config
              mountPath: /config
              readOnly: true
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8000
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /readyz
              port: 8000
            initialDelaySeconds: 5
            periodSeconds: 5
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 1000m
              memory: 512Mi
      volumes:
        - name: config
          secret:
            secretName: registry-config
```

### Step 4: Create Service

```yaml
# service.yaml
apiVersion: v1
kind: Service
metadata:
  name: registry
  namespace: registry
spec:
  selector:
    app: registry
  ports:
    - port: 8000
      targetPort: 8000
```

### Step 5: Create Ingress

```yaml
# ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: registry
  namespace: registry
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
spec:
  ingressClassName: nginx
  tls:
    - hosts:
        - registry.example.com
      secretName: registry-tls
  rules:
    - host: registry.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: registry
                port:
                  number: 8000
```

### Step 6: Apply Manifests

```bash
kubectl apply -f namespace.yaml
kubectl create secret generic registry-config \
  --namespace registry \
  --from-file=config.toml=config.toml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl apply -f ingress.yaml
```

---

## With Redis for Locking and Caching

For improved performance in multi-replica deployments, add Redis for distributed locking and token caching.

### Deploy Redis

```yaml
# redis.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
  namespace: registry
spec:
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
        - name: redis
          image: redis:7-alpine
          ports:
            - containerPort: 6379
---
apiVersion: v1
kind: Service
metadata:
  name: redis
  namespace: registry
spec:
  selector:
    app: redis
  ports:
    - port: 6379
```

### Update Configuration

Add Redis to `config.toml`:

```toml
[metadata_store.s3.lock_strategy.redis]
url = "redis://redis:6379"
ttl = 10

[cache.redis]
url = "redis://redis:6379"
```

---

## Scheduled Storage Maintenance (CronJob)

Run periodic maintenance to enforce retention policies and verify storage integrity.

**Important:** With S3 storage, the scrub job only needs the config volume, no data storage volume is required.

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: registry-maintenance
  namespace: registry
spec:
  schedule: "0 3 * * *"  # Daily at 3 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: scrub
              image: ghcr.io/project-angos/angos:latest
              args: ["-c", "/config/config.toml", "scrub", "--tags", "--manifests", "--blobs", "--retention"]
              volumeMounts:
                - name: config
                  mountPath: /config
                  readOnly: true
          volumes:
            - name: config
              secret:
                secretName: registry-config
          restartPolicy: OnFailure
```

---

## Verification

```bash
# Check pods
kubectl get pods -n registry

# View logs
kubectl logs -n registry -l app=registry -f

# Port forward for testing
kubectl port-forward -n registry svc/registry 8000:8000

# Test
curl http://localhost:8000/v2/
```

---

## Backup and Disaster Recovery

### With S3 Storage (Recommended)

Angos is stateless—only configuration and S3 bucket data need protection.

**Protect the S3 Bucket:**

1. **Enable versioning:**
   ```bash
   aws s3api put-bucket-versioning \
     --bucket my-registry-bucket \
     --versioning-configuration Status=Enabled
   ```

2. **Enable cross-region replication:**
   ```bash
   aws s3api put-bucket-replication \
     --bucket my-registry-bucket \
     --replication-configuration file://replication.json
   ```

3. **Enable MFA delete protection** (requires root access):
   ```bash
   aws s3api put-bucket-versioning \
     --bucket my-registry-bucket \
     --versioning-configuration Status=Enabled,MFADelete=Enabled
   ```

**Backup Configuration:**

Store `config.toml` and TLS certificates in version control or a separate secure location:

```bash
# Backup current config and secrets
kubectl get secret -n registry registry-config -o yaml > config-backup.yaml
kubectl get secret -n registry registry-tls -o yaml > tls-backup.yaml
```

**Recovery Plan:**

- **Bucket loss:** Restore from cross-region replication or versioning
- **Configuration loss:** Reapply from version control or backup files
- **Complete failure:** Redeploy Deployment manifests, reapply Secrets, data is intact in S3

---

## Troubleshooting

**Pod not starting:**
```bash
kubectl describe pod -n registry -l app=registry
kubectl logs -n registry -l app=registry
```

**S3 connection errors:**
```bash
# Verify S3 credentials
aws s3 ls s3://my-registry-bucket --region us-east-1

# Check access key permissions
aws iam get-user
```

**Ingress not working:**
```bash
kubectl describe ingress -n registry registry
kubectl get events -n registry
```

---

## Next Steps

- [Configure mTLS](configure-mtls.md) with TLS passthrough
- [Configure GitHub Actions OIDC](configure-github-actions-oidc.md) for CI/CD
- [Set Up Access Control](set-up-access-control.md) for policy-based authorization
- [Storage Maintenance](run-storage-maintenance.md) for retention policies and cleanup
