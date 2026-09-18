---
displayed_sidebar: howto
sidebar_position: 11
title: "Enable Web UI"
---

# Enable the Web UI

## Prerequisites

- Angos running

## Basic Setup

### Enable the UI

Add to `config.toml`:

```toml
[ui]
enabled = true
```

### Customize the Name

```toml
[ui]
enabled = true
name = "My Container Registry"
```

### Restart the Registry

```bash
./angos -c config.toml server
```

### Access the UI

Open `http://localhost:8000/` in your browser.

---

## UI Features

### Navigation

```
Repositories → Namespaces → Manifests → Details
```

### Repository List

- Shows all configured repositories
- Displays namespace counts
- Shows feature badges (pull-through, immutable tags)

### Namespace List

- Lists images within a repository
- Shows manifest and upload counts
- Displays repository configuration

### Manifest List

- Tree view of multi-platform images
- Platform badges (linux/amd64, etc.)
- Tags and digests

### Manifest Details

- Full manifest metadata
- Tags (with delete option)
- Layers and files
- Parent/child relationships
- Download options for ORAS artifacts

---

## Sign In with OIDC

The UI signs a browser in against one of the registry's own OIDC providers and
sends the resulting ID token as a bearer on every call it makes.

### Step 1: Register a Public Client

At the provider, create a public client (no secret, PKCE required) with:

- **Redirect URI**: the registry's root, such as `https://registry.example.com/`
- **CORS / web origins**: the same origin, since the browser reads the provider's
  discovery document and calls its token endpoint directly

### Step 2: Point the UI at a Provider

```toml
[auth.oidc.dex]
issuer = "https://dex.example.com"
required_audience = "angos-ui"

[ui]
enabled = true

[ui.oidc]
provider = "dex"
client_id = "angos-ui"
scopes = "openid profile email"      # default
```

`provider` names the `[auth.oidc.<name>]` section above it, so the browser is
sent to the issuer the registry validates tokens from; the registry refuses to
start when the name matches no provider.

An ID token's `aud` is the client id, so `required_audience = "angos-ui"` binds
the tokens the UI sends to this client. The registry accepts a JWT only, which
is what an ID token always is.

### Step 3: Let Signed-In Users Browse

```toml
[global.access_policy]
default = "deny"
rules = [
  "request.action == 'ui-asset' || request.action == 'ui-config'",
  "identity.oidc != null && identity.oidc.claims['email'].endsWith('@example.com')"
]
```

### How It Behaves

- A **Sign in** button sits in the top bar, next to the theme switcher.
- Sign-in also starts on its own the first time the registry refuses a request,
  so a private registry needs no click, while a registry readable anonymously is
  still browsed without signing in. It starts once per page load, so a token the
  registry keeps refusing cannot bounce the browser back and forth.
- The token lives in the tab's session storage and is dropped when it expires or
  when the tab closes. There is no refresh: an expired session means signing in
  again.
- **Sign out** clears the token here only. It does not end the session at the
  provider, so signing in again may not prompt for credentials.
- Download links (ORAS artifact files, layer file downloads) are followed by the
  browser rather than fetched, so they carry no token and work only where the
  policy allows the `get-blob` action anonymously.

Leave `[ui.oidc]` out and the UI offers no sign-in and sends no credentials,
which is what a registry fronted by an authenticating proxy wants.

---

## Access Control

The UI uses the same access policies as the API:

```toml
[global.access_policy]
default = "deny"
rules = [
  # Allow UI assets to load (required)
  "request.action == 'ui-asset' || request.action == 'ui-config'",

  # Allow authenticated users to browse
  "identity.username != null && request.action.startsWith('list-')",

  # Allow reading manifests
  "identity.username != null && request.action == 'get-manifest'",

  # Restrict deletion to admins
  "identity.username == 'admin' && request.action == 'delete-manifest'"
]
```

### UI-Specific Actions

| Action | Description |
|--------|-------------|
| `ui-asset` | Static files (JS, CSS) |
| `ui-config` | UI configuration endpoint |
| `list-repositories` | Repository list |
| `list-namespaces` | Namespace list |
| `list-revisions` | Manifest list |
| `list-uploads` | Active uploads |

---

## URL Structure

URLs follow Docker reference format:

| URL | Description |
|-----|-------------|
| `/` | Repository list |
| `/{repository}` | Namespace list |
| `/{repository}/{namespace}` | Manifest list |
| `/{repository}/{namespace}:{tag}` | Manifest by tag |
| `/{repository}/{namespace}@{digest}` | Manifest by digest |

**Examples:**
- `/ghcr.io` - GitHub Container Registry mirror
- `/ghcr.io/library/nginx` - nginx image manifests
- `/ghcr.io/library/nginx:latest` - latest tag details
- `/ghcr.io/library/nginx@sha256:abc...` - specific digest

---

## Features

### Delete Operations

Click a delete button once to arm, click again to confirm.

- **Delete tag**: Removes tag, keeps manifest if other tags exist
- **Delete manifest**: Removes by digest, along with the platform manifests and referrers its going orphans (anything tagged, or still named by another index, stays)
- **Cancel upload**: Aborts in-progress uploads
- **Delete several**: **Select** above the manifest table adds a checkbox per row; **delete selected (N)** removes the ticked ones

### Theme Toggle

Switch between light, dark and system themes with the switcher at the right of the top bar. Preference is saved in browser storage.

### ORAS Artifacts

For OCI artifacts, files can be downloaded directly:

- Shows filename and media type
- Size information
- Download button

### Annotations

Expand annotations with the `[+]` button. Well-known keys are displayed with friendly names.

---

## Verification

### Check UI is Enabled

```bash
curl http://localhost:8000/v2/_angos/ui/config
```

Returns:
```json
{"name": "My Container Registry"}
```

### Test Access

```bash
# With authentication
curl -u admin:password http://localhost:8000/
```

Open the UI and use **Sign in** to check an OIDC setup end to end.

---

## Troubleshooting

### UI Not Loading

- Check `ui.enabled = true` in config
- Verify access policies allow `ui-asset` and `ui-config`
- Check browser console for errors

### 403 Forbidden on Browse

- Add `list-*` actions to access policy
- Check authentication is working

### Sign-In Fails

- Check the provider allows the registry's origin (CORS) on its discovery and
  token endpoints; the browser calls both directly.
- Check the redirect URI registered at the provider is the registry's root.
- A 401 that persists after signing in is the registry refusing the token: check
  `required_audience` against the client id, and that the provider's `issuer`
  matches the `iss` its tokens carry.

### Can't Delete

- Verify `delete-manifest` is allowed in policy
- Check user has required permissions

### Blank Page

- Clear browser cache
- Check for JavaScript errors in console
- Verify static assets are accessible

## Reference

- [Web UI Reference](../reference/ui.md) - Complete UI reference
- [Set Up Access Control](set-up-access-control.md) - UI access policies
- [API Endpoints Reference](../reference/api-endpoints.md) - Extension endpoints
