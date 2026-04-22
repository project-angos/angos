---
displayed_sidebar: howto
sidebar_position: 5
title: "Generic OIDC"
---

# Configure Generic OIDC

Set up Angos to accept tokens from any OIDC-compliant identity provider (Google, Okta, Auth0, Keycloak, etc.).

## Prerequisites

- Angos running
- OIDC provider configured with:
  - Client ID (for audience validation)
  - OIDC discovery endpoint or JWKS URI

## Configure the Registry

### Step 1: Add OIDC Provider

Add a generic provider to `config.toml`:

```toml
[auth.oidc.my-provider]
provider = "generic"
issuer = "https://auth.example.com"
```

The registry automatically discovers the JWKS endpoint from the issuer's `.well-known/openid-configuration`.

### Step 2: Optional Settings

```toml
[auth.oidc.my-provider]
provider = "generic"
issuer = "https://auth.example.com"
required_audience = "my-registry"        # Validate audience claim
jwks_uri = "https://auth.example.com/.well-known/jwks.json"  # Override discovery
jwks_refresh_interval = 3600             # Refresh keys hourly (default)
clock_skew_tolerance = 60                # Allow 60s clock drift (default)
```

### Step 3: Add Access Policy

```toml
[global.access_policy]
default_allow = false
rules = [
  "identity.oidc != null && identity.oidc.claims['email'].endsWith('@example.com')"
]
```

---

## Provider-Specific Examples

### Google Cloud Identity

```toml
[auth.oidc.google]
provider = "generic"
issuer = "https://accounts.google.com"
required_audience = "your-client-id.apps.googleusercontent.com"
```

### Okta

```toml
[auth.oidc.okta]
provider = "generic"
issuer = "https://your-org.okta.com"
required_audience = "your-client-id"
```

### Auth0

```toml
[auth.oidc.auth0]
provider = "generic"
issuer = "https://your-tenant.auth0.com/"
required_audience = "your-api-identifier"
```

### Keycloak

```toml
[auth.oidc.keycloak]
provider = "generic"
issuer = "https://keycloak.example.com/realms/myrealm"
required_audience = "registry-client"
```

### Azure AD

```toml
[auth.oidc.azure]
provider = "generic"
issuer = "https://login.microsoftonline.com/your-tenant-id/v2.0"
required_audience = "api://your-app-id"
```

---

## Multiple Providers

Configure multiple providers simultaneously:

```toml
[auth.oidc.github-actions]
provider = "github"

[auth.oidc.corporate]
provider = "generic"
issuer = "https://auth.corp.example.com"
required_audience = "registry"

[auth.oidc.cloud]
provider = "generic"
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
