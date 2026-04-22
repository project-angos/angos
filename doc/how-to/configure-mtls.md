---
displayed_sidebar: howto
sidebar_position: 3
title: "Configure mTLS"
---

# Configure mTLS

Set up mutual TLS for client certificate authentication between container runtimes and Angos.

## Prerequisites

- Angos instance that terminates TLS itself (not behind a TLS-terminating proxy)
- Server certificate and private key
- Client CA certificate (from your internal PKI)
- Client certificate and private key

## Configure Angos

### Step 1: Prepare Certificates

Organize your certificates:

```
/tls/
├── server-certificate.pem   # Server cert (can include chain)
├── server-private-key.pem   # Server private key
└── client-ca-bundle.pem     # Trusted client CAs
```

### Step 2: Update Configuration

Add TLS configuration to `config.toml`:

```toml
[server]
bind_address = "0.0.0.0"
port = 8000

[server.tls]
server_certificate_bundle = "/tls/server-certificate.pem"
server_private_key = "/tls/server-private-key.pem"
client_ca_bundle = "/tls/client-ca-bundle.pem"
```

### Step 3: Restart the Registry

```bash
./angos -c config.toml server
```

---

## Configure Container Runtimes

### Docker

Create the certificate directory:

```bash
mkdir -p /etc/docker/certs.d/registry.example.com
```

Copy certificates:

```bash
cp client-certificate.pem /etc/docker/certs.d/registry.example.com/client.cert
cp client-private-key.pem /etc/docker/certs.d/registry.example.com/client.key
cp ca-certificate.pem /etc/docker/certs.d/registry.example.com/ca.crt
```

Restart Docker:

```bash
systemctl restart docker
```

### Podman

Create the certificate directory:

```bash
mkdir -p /etc/containers/certs.d/registry.example.com
```

Copy certificates:

```bash
cp client-certificate.pem /etc/containers/certs.d/registry.example.com/client.cert
cp client-private-key.pem /etc/containers/certs.d/registry.example.com/client.key
cp ca-certificate.pem /etc/containers/certs.d/registry.example.com/ca.crt
```

### containerd

Create the certificate directory:

```bash
mkdir -p /etc/containerd/certs.d/registry.example.com
```

Copy certificates:

```bash
cp client-certificate.pem /etc/containerd/certs.d/registry.example.com/client.cert
cp client-private-key.pem /etc/containerd/certs.d/registry.example.com/client.key
cp ca-certificate.pem /etc/containerd/certs.d/registry.example.com/ca.crt
```

Configure containerd 2.x (`/etc/containerd/config.toml`):

```toml
version = 3

[plugins."io.containerd.cri.v1.images".registry]
config_path = "/etc/containerd/certs.d"
```

For containerd 1.x:

```toml
version = 2

[plugins."io.containerd.grpc.v1.cri".registry]
config_path = "/etc/containerd/certs.d"
```

Restart containerd:

```bash
systemctl restart containerd
```

---

## Access Control with Certificates

Use certificate attributes in access policies:

```toml
[global.access_policy]
default_allow = false
rules = [
  # Allow clients with specific organization
  "identity.certificate.organizations.contains('Infrastructure')",

  # Allow specific common name
  "'build-server' in identity.certificate.common_names",

  # Combine with other auth methods
  "identity.username == 'admin'"
]
```

Available certificate variables:
- `identity.certificate.common_names` - List of CNs
- `identity.certificate.organizations` - List of Organizations

---

## Verification

Test with curl:

```bash
curl --cert client.pem --key client-key.pem \
  --cacert ca.pem \
  https://registry.example.com:8000/v2/
```

Test with Docker:

```bash
docker pull registry.example.com/test/alpine:latest
```

---

## mTLS Scenarios

### Scenario 1: Certificate Required

All clients must present a valid certificate:

```toml
[global.access_policy]
default_allow = false
rules = [
  "size(identity.certificate.common_names) > 0"
]
```

### Scenario 2: Certificate Optional

Certificate enhances access but isn't required:

```toml
[global.access_policy]
default_allow = false
rules = [
  # Certificate grants full access
  "identity.certificate.organizations.contains('DevOps')",

  # Basic auth for read-only
  "identity.username != '' && request.action.startsWith('get-')"
]
```

### Scenario 3: Different Orgs, Different Access

```toml
# Developers can push to dev repo
[repository."dev".access_policy]
default_allow = false
rules = [
  "identity.certificate.organizations.contains('Developers')"
]

# Only platform team can push to production
[repository."prod".access_policy]
default_allow = false
rules = [
  "identity.certificate.organizations.contains('Platform')"
]
```

---

## Troubleshooting

**Certificate rejected:**
- Verify the client certificate is signed by a CA in `client_ca_bundle`
- Check certificate expiration: `openssl x509 -in client.pem -noout -dates`
- Verify the chain is complete

**Connection refused:**
- Ensure TLS is configured (not running in insecure mode)
- Check firewall rules
- Verify the server certificate is valid for the hostname

**Debug logging:**
```bash
RUST_LOG=angos::auth=debug ./angos server
```

## Next Steps

- [Set Up Access Control](set-up-access-control.md) for fine-grained policies
- [Configure Webhook Authorization](configure-webhook-authorization.md) for external policy decisions
