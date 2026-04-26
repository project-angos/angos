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

## Access Control

The UI uses the same access policies as the API:

```toml
[global.access_policy]
default = "deny"
rules = [
  # Allow UI assets to load (required)
  "request.action == 'ui-asset' || request.action == 'ui-config'",

  # Allow authenticated users to browse
  "identity.username != '' && request.action.startsWith('list-')",

  # Allow reading manifests
  "identity.username != '' && request.action == 'get-manifest'",

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
- **Delete manifest**: Removes by digest
- **Cancel upload**: Aborts in-progress uploads

### Theme Toggle

Switch between dark and light themes using the header button. Preference is saved in browser storage.

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
curl http://localhost:8000/_ui/config
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

---

## Troubleshooting

### UI Not Loading

- Check `ui.enabled = true` in config
- Verify access policies allow `ui-asset` and `ui-config`
- Check browser console for errors

### 403 Forbidden on Browse

- Add `list-*` actions to access policy
- Check authentication is working

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
