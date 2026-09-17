---
displayed_sidebar: howto
sidebar_position: 5
title: "OIDC"
---

# Configure OIDC

Set up Angos to accept tokens from any OIDC-compliant identity provider (Google, Okta, Auth0, Keycloak, etc.).

## Prerequisites

- Angos running
- OIDC provider configured with:
  - Client ID (for audience validation)
  - OIDC discovery endpoint or JWKS URI

## Configure the Registry

### Step 1: Add OIDC Provider

Add a provider to `config.toml`. A provider is an issuer plus how its tokens are
validated, so every provider takes the same options; the issuer is what tells
them apart.

```toml
[auth.oidc.my-provider]
issuer = "https://auth.example.com"
```

The registry automatically discovers the JWKS endpoint from the issuer's `.well-known/openid-configuration`.

### Step 2: Optional Settings

```toml
[auth.oidc.my-provider]
issuer = "https://auth.example.com"
required_audience = "my-registry"        # Validate audience claim
required_claims = ["email"]              # Reject a token that does not carry them
jwks_uri = "https://auth.example.com/.well-known/jwks.json"  # Override discovery
jwks_refresh_interval = 3600             # Refresh keys hourly (default)
clock_skew_tolerance = 60                # Allow 60s clock drift (default)
allowed_algorithms = ["RS256"]           # Restrict accepted JWT algorithms (default)
server_ca_bundle = "/certs/ca.pem"       # Trust a private CA for this issuer
client_certificate_bundle = "/certs/client.pem"   # Authenticate to an issuer that
client_private_key = "/certs/client-key.pem"      # refuses anonymous discovery
```

`required_claims` checks presence only. To test a claim's *value*, use an access
policy rule, which sees the whole claim map.

### Step 3: Add Access Policy

```toml
[global.access_policy]
default = "deny"
rules = [
  "identity.oidc != null && identity.oidc.claims['email'].endsWith('@example.com')"
]
```

---

## Provider-Specific Examples

### Google Cloud Identity

```toml
[auth.oidc.google]
issuer = "https://accounts.google.com"
required_audience = "your-client-id.apps.googleusercontent.com"
```

### Okta

```toml
[auth.oidc.okta]
issuer = "https://your-org.okta.com"
required_audience = "your-client-id"
```

### Auth0

```toml
[auth.oidc.auth0]
issuer = "https://your-tenant.auth0.com/"
required_audience = "your-api-identifier"
```

### Keycloak

```toml
[auth.oidc.keycloak]
issuer = "https://keycloak.example.com/realms/myrealm"
required_audience = "registry-client"
```

### Azure AD

```toml
[auth.oidc.azure]
issuer = "https://login.microsoftonline.com/your-tenant-id/v2.0"
required_audience = "api://your-app-id"
```

### Kubernetes API Server

Validating a cluster's service-account tokens takes a private CA and an
authenticated discovery fetch, and pulls take a kubelet plugin on top:
[Configure Kubernetes OIDC](configure-kubernetes-oidc.md).

---

## Multiple Providers

You can declare more than one provider; on each request the registry tries them
one after another and stops at the first one that authenticates the token. The
provider whose `issuer` the token names goes first, so a token normally costs
one provider's JWKS lookup rather than every provider's, and a provider whose
issuer is unreachable does not delay tokens meant for another. That ordering
reads the token's `iss` before verifying it, so it only decides who validates
first: the provider it names still checks the signature in full, and a token no
provider accepts is still refused by all of them. Providers that don't recognise
the credentials are skipped silently; if a provider rejects the token (e.g.
signature mismatch), the next one is still tried. Pick distinct, well-scoped
issuers/audiences so a token only matches the provider that actually signed it.

```toml
[auth.oidc.github-actions]
issuer = "https://token.actions.githubusercontent.com"
required_claims = ["repository", "actor"]

[auth.oidc.corporate]
issuer = "https://auth.corp.example.com"
required_audience = "registry"

[auth.oidc.cloud]
issuer = "https://accounts.google.com"
```

Access policies can check which provider authenticated:

```toml
rules = [
  # CI/CD via GitHub Actions
  '''identity.oidc != null &&
     identity.oidc.provider_name == "github-actions" &&
     identity.oidc.claims["repository"].startsWith("myorg/")''',

  # Developers via corporate SSO
  '''identity.oidc != null &&
     identity.oidc.provider_name == "corporate" &&
     identity.oidc.claims["email"].endsWith("@corp.example.com")''',

  # Service accounts via Google Cloud
  '''identity.oidc != null &&
     identity.oidc.provider_name == "cloud" &&
     identity.oidc.claims["email"].endsWith(".iam.gserviceaccount.com")'''
]
```

---

## Policy Examples

### Email Domain Restriction

```toml
rules = [
  '''identity.oidc != null &&
     identity.oidc.claims["email"].endsWith("@company.com")'''
]
```

### Group Membership

```toml
rules = [
  '''identity.oidc != null &&
     "registry-admins" in identity.oidc.claims["groups"]'''
]
```

### Specific User

```toml
rules = [
  '''identity.oidc != null &&
     identity.oidc.claims["sub"] == "user-123-456"'''
]
```

### Combined Conditions

```toml
rules = [
  '''identity.oidc != null &&
     identity.oidc.claims["email_verified"] == true &&
     identity.oidc.claims["role"] == "developer"'''
]
```

---

## Using the Token

### With Docker

```bash
# Get token from your identity provider
TOKEN=$(get-oidc-token)  # Provider-specific

# Login using provider name as username
echo $TOKEN | docker login registry.example.com \
  --username my-provider --password-stdin

# Push/pull as normal
docker push registry.example.com/myapp:latest
```

### With curl

```bash
TOKEN=$(get-oidc-token)

curl -H "Authorization: Bearer $TOKEN" \
  https://registry.example.com/v2/
```

---

## Verification

Enable debug logging to see token validation:

```bash
RUST_LOG=angos::auth=debug ./angos server
```

You should see:
```
OIDC token validated for provider my-provider
Issuer: https://auth.example.com
Subject: user@example.com
```

---

## Troubleshooting

**Token rejected - issuer mismatch:**
- The `iss` claim in the token must exactly match the configured `issuer`
- Check for trailing slashes

**Token rejected - audience mismatch:**
- The `aud` claim must match `required_audience` if configured
- Remove `required_audience` to skip this check

**JWKS fetch failed:**
- Verify the registry can reach the issuer
- Check if a custom `jwks_uri` is needed

**Claims not available:**
- Use bracket notation: `identity.oidc.claims["claim_name"]`
- Check what claims your provider includes in tokens

## Next Steps

- [Set Up Access Control](set-up-access-control.md) for comprehensive policies
- [Configure GitHub Actions OIDC](configure-github-actions-oidc.md) for CI/CD
- [Configure Kubernetes OIDC](configure-kubernetes-oidc.md) for service-account tokens and image pulls
- [Enable the Web UI](enable-web-ui.md) to sign browsers in against a provider configured here
