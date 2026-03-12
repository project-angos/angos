---
displayed_sidebar: howto
sidebar_position: 2
title: "Deploy on Kubernetes"
---

# Deploy on Kubernetes

Deploy Angos on Kubernetes using Kustomize or raw manifests.

## Prerequisites

- Kubernetes cluster
- `kubectl` configured
- Persistent storage (PVC or S3)
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

## Manual Deployment

### Step 1: Create Namespace

```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: registry
```

### Step 2: Create ConfigMap

```yaml
# configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: registry-config
  namespace: registry
data:
  config.toml: |
    [server]
    bind_address = "0.0.0.0"
    port = 5000

    [blob_store.fs]
    root_dir = "/data"

    [ui]
    enabled = true
    name = "My Registry"
```

### Step 3: Create PersistentVolumeClaim

```yaml
# pvc.yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: registry-data
  namespace: registry
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
  storageClassName: standard  # Adjust for your cluster
```

### Step 4: Create Deployment

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: registry
  namespace: registry
spec:
  replicas: 1
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
            - containerPort: 5000
          volumeMounts:
            - name: config
              mountPath: /config
              readOnly: true
            - name: data
              mountPath: /data
          livenessProbe:
            httpGet:
              path: /healthz
              port: 5000
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /readyz
              port: 5000
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
          configMap:
            name: registry-config
        - name: data
          persistentVolumeClaim:
            claimName: registry-data
```

### Step 5: Create Service

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
    - port: 5000
      targetPort: 5000
```

### Step 6: Create Ingress

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
                  number: 5000
```

### Step 7: Apply Manifests

```bash
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f pvc.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl apply -f ingress.yaml
```

---

## With S3 Backend

For multi-replica deployments, use S3 storage.

### Create Secret

```bash
kubectl create secret generic registry-s3 \
  --namespace registry \
  --from-literal=access-key-id=YOUR_ACCESS_KEY \
  --from-literal=secret-access-key=YOUR_SECRET_KEY
```

### Create ConfigMap Template

Create a ConfigMap with placeholders that will be substituted:

```yaml
# configmap-template.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: registry-config-template
  namespace: registry
data:
  config.toml.template: |
    [server]
    bind_address = "0.0.0.0"
    port = 5000

    [blob_store.s3]
    access_key_id = "${S3_ACCESS_KEY_ID}"
    secret_key = "${S3_SECRET_ACCESS_KEY}"
    endpoint = "https://s3.amazonaws.com"
    bucket = "my-registry-bucket"
    region = "us-east-1"

    [ui]
    enabled = true
```

### Update Deployment with Init Container

Use an init container to substitute environment variables into the config:

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: registry
  namespace: registry
spec:
  replicas: 1
  selector:
    matchLabels:
      app: registry
  template:
    metadata:
      labels:
        app: registry
    spec:
      initContainers:
        - name: config-init
          image: busybox:1.36
          command: ['sh', '-c', 'envsubst < /template/config.toml.template > /config/config.toml']
          env:
            - name: S3_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: registry-s3
                  key: access-key-id
            - name: S3_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: registry-s3
                  key: secret-access-key
          volumeMounts:
            - name: config-template
              mountPath: /template
            - name: config
              mountPath: /config
      containers:
        - name: registry
          image: ghcr.io/project-angos/angos:latest
          args: ["-c", "/config/config.toml", "server"]
          ports:
            - containerPort: 5000
          volumeMounts:
            - name: config
              mountPath: /config
            - name: data
              mountPath: /data
      volumes:
        - name: config-template
          configMap:
            name: registry-config-template
        - name: config
          emptyDir: {}
        - name: data
          persistentVolumeClaim:
            claimName: registry-data
```

---

## With Redis for Locking

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

```toml
[metadata_store.s3.lock_strategy.redis]
url = "redis://redis:6379"
ttl = 10

[cache.redis]
url = "redis://redis:6379"
```

---

## Scheduled Storage Maintenance

### CronJob

Run periodic maintenance to check storage integrity and enforce retention policies:

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
              args: ["-c", "/config/config.toml", "scrub", "-t", "-m", "-b", "-r"]
              volumeMounts:
                - name: config
                  mountPath: /config
                  readOnly: true
                - name: data
                  mountPath: /data
          volumes:
            - name: config
              configMap:
                name: registry-config
            - name: data
              persistentVolumeClaim:
                claimName: registry-data
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
kubectl port-forward -n registry svc/registry 5000:5000

# Test
curl http://localhost:5000/v2/
```

---

## Troubleshooting

**Pod not starting:**
```bash
kubectl describe pod -n registry -l app=registry
kubectl logs -n registry -l app=registry
```

**PVC not binding:**
```bash
kubectl get pvc -n registry
kubectl describe pvc -n registry registry-data
```

**Ingress not working:**
```bash
kubectl describe ingress -n registry registry
kubectl get events -n registry
```

## Next Steps

- [Configure mTLS](configure-mtls.md) with TLS passthrough
- [Configure GitHub Actions OIDC](configure-github-actions-oidc.md) for CI/CD
- [Set Up Access Control](set-up-access-control.md) for policy-based authorization
