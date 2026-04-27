---
displayed_sidebar: howto
sidebar_position: 4
title: "GitHub Actions OIDC"
---

# Configure GitHub Actions OIDC

Set up Angos to accept GitHub Actions OIDC tokens for passwordless authentication from CI/CD pipelines.

## Prerequisites

- Angos running with network access from GitHub Actions
- GitHub repository with Actions enabled

## Configure the Registry

### Step 1: Add OIDC Provider

Add the GitHub provider to `config.toml`:

```toml
[auth.oidc.github-actions]
provider = "github"
```

That's it for basic configuration. The registry automatically uses GitHub's default issuer and JWKS endpoints.

### Step 2: Add Access Policy

Configure which repositories can access your registry:

```toml
[global.access_policy]
default = "deny"
rules = [
  # Allow GitHub Actions from your organization
  "identity.oidc != null && identity.oidc.claims['repository'].startsWith('myorg/')"
]
```

### Step 3: Restart the Registry

```bash
./angos -c config.toml server
```

---

## Configure GitHub Actions

### Step 1: Add Permissions

Your workflow needs `id-token: write` permission:

```yaml
name: Push to Registry
on: push

jobs:
  push:
    runs-on: ubuntu-latest
    permissions:
      id-token: write   # Required for OIDC
      contents: read
```

### Step 2: Get and Use the Token

```yaml
    steps:
      - uses: actions/checkout@v4

      - name: Get OIDC Token
        id: get-token
        run: |
          TOKEN=$(curl -s -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \
            "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=https://github.com/${{ github.repository }}" \
            | jq -r '.value')
          echo "::add-mask::$TOKEN"
          echo "token=$TOKEN" >> $GITHUB_OUTPUT

      - name: Login to Registry
        run: |
          echo "${{ steps.get-token.outputs.token }}" | \
            docker login registry.example.com \
              --username github-actions \
              --password-stdin

      - name: Build and Push
        run: |
          docker build -t registry.example.com/myapp:${{ github.sha }} .
          docker push registry.example.com/myapp:${{ github.sha }}
```

The username must match the provider name (`github-actions` in this example).

---

## Policy Examples

### Allow Specific Repositories

```toml
[repository."myapp".access_policy]
default = "deny"
rules = [
  '''identity.oidc != null &&
     identity.oidc.claims["repository"].matches("^myorg/(app1|app2|app3)$")'''
]
```

### Allow Main Branch Only

```toml
[repository."production".access_policy]
default = "deny"
rules = [
  '''identity.oidc != null &&
     identity.oidc.claims["ref"] == "refs/heads/main" &&
     identity.oidc.claims["repository"].startsWith("myorg/")'''
]
```

### Allow Release Tags Only

```toml
[repository."releases".access_policy]
default = "deny"
rules = [
  '''identity.oidc != null &&
     identity.oidc.claims["ref"].matches("^refs/tags/v[0-9]+\\.[0-9]+\\.[0-9]+$")'''
]
```

### Allow Specific Workflows

```toml
rules = [
  '''identity.oidc != null &&
     identity.oidc.claims["workflow"].matches("^(deploy|release)\\.yml$")'''
]
```

### Allow Specific Environments

```toml
rules = [
  '''identity.oidc != null &&
     identity.oidc.claims["environment"] == "production"'''
]
```

### Allow Specific Actors

```toml
rules = [
  '''identity.oidc != null &&
     identity.oidc.claims["actor"] in ["myuser", "dependabot[bot]", "renovate[bot]"]'''
]
```

---

## Complete Example

### config.toml

```toml
[server]
bind_address = "0.0.0.0"
port = 8000

[server.tls]
server_certificate_bundle = "/tls/server.crt"
server_private_key = "/tls/server.key"

[blob_store.fs]
root_dir = "/data"

[auth.oidc.github-actions]
provider = "github"

# Production: only main branch from specific repos
[repository."production".access_policy]
default = "deny"
rules = [
  '''identity.oidc != null &&
     identity.oidc.provider_name == "github-actions" &&
     identity.oidc.claims["ref"] == "refs/heads/main" &&
     identity.oidc.claims["repository"].matches("^myorg/(api|web|worker)$")'''
]

# Dev: any branch from org
[repository."dev".access_policy]
default = "deny"
rules = [
  '''identity.oidc != null &&
     identity.oidc.claims["repository"].startsWith("myorg/")'''
]

[ui]
enabled = true
```

### GitHub Workflow

```yaml
name: Build and Push
on:
  push:
    branches: [main]
    tags: ['v*']

env:
  REGISTRY: registry.example.com
  IMAGE_NAME: ${{ github.repository }}

jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      id-token: write
      contents: read

    steps:
      - uses: actions/checkout@v4

      - name: Get OIDC Token
        id: oidc
        run: |
          TOKEN=$(curl -s -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \
            "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=https://github.com/${{ github.repository }}" \
            | jq -r '.value')
          echo "::add-mask::$TOKEN"
          echo "token=$TOKEN" >> $GITHUB_OUTPUT

      - name: Login to Registry
        run: |
          echo "${{ steps.oidc.outputs.token }}" | \
            docker login $REGISTRY --username github-actions --password-stdin

      - name: Extract metadata
        id: meta
        uses: docker/metadata-action@v5
        with:
          images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}

      - name: Build and push
        uses: docker/build-push-action@v5
        with:
          context: .
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
```

---

## Verification

Check the registry logs for authentication details:

```bash
RUST_LOG=angos::auth=debug ./angos server
```

You should see:
```
OIDC token validated for provider github-actions
Claims: repository=myorg/myrepo, ref=refs/heads/main, actor=username
```

---

## Troubleshooting

**Token rejected:**
- Check `id-token: write` permission is set
- Verify the token audience matches expectations
- Enable debug logging to see validation details

**Policy not matching:**
- Always check `identity.oidc != null` first
- Use bracket notation for claims: `identity.oidc.claims["claim"]`
- Check claim values in debug logs

**Network errors:**
- Ensure the registry can reach `token.actions.githubusercontent.com` for JWKS

## Next Steps

- [Configure Generic OIDC](configure-generic-oidc.md) for other identity providers
- [Set Up Access Control](set-up-access-control.md) for comprehensive policies
