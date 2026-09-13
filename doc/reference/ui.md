---
displayed_sidebar: reference
sidebar_position: 5
title: "Web UI"
---

# Web UI Reference

Reference for the Angos web interface.

---

## Configuration

```toml
[ui]
enabled = true
name = "My Registry"
```

| Option    | Type   | Default             | Description                           |
|-----------|--------|---------------------|---------------------------------------|
| `enabled` | bool   | `false`             | Enable the web interface              |
| `name`    | string | `"Angos"` | Registry name displayed in the top bar |

---

## URL Structure

URLs follow Docker reference format:

| URL                                  | View             | Description                 |
|--------------------------------------|------------------|-----------------------------|
| `/`                                  | Repository list  | All configured repositories |
| `/{repository}`                      | Namespace list   | Images within a repository  |
| `/{repository}/{namespace}`          | Manifest list    | All manifests for an image  |
| `/{repository}/{namespace}:{tag}`    | Manifest details | Manifest by tag             |
| `/{repository}/{namespace}@{digest}` | Manifest details | Manifest by digest          |
| `...#history`, `...#vulnerabilities`, `...#filesystem` | Manifest details | A manifest's Pull History, Vulnerabilities or Filesystem tab; no anchor opens the OCI tab, and `...#vulnerabilities/linux/arm64` one platform's report of an index |
| `/jobs/{queue}`                      | Jobs             | Pending and failed jobs of the `cache`, `replication` or `scan` queue; `/jobs` opens the cache queue |

**Examples:**
- `/` - List all repositories
- `/library` - List namespaces in the "library" repository
- `/library/nginx` - List all nginx manifests
- `/library/nginx:latest` - Details for the latest tag
- `/library/nginx@sha256:abc123...` - Details for a specific digest

---

## API Endpoints

### UI Configuration

```
GET /v2/_angos/ui/config
```

Returns the UI configuration.

**Response:**
```json
{
  "name": "My Registry"
}
```

### Static Assets

Static assets have no dedicated URL prefix. Every `GET` or `HEAD` request outside the API routes (`/v2/...`, `/healthz`, `/readyz`, `/metrics`) serves the embedded single-page app, falling back to `index.html` for paths that match no bundled asset.

---

## Access Control Actions

UI-specific actions for access policies:

| Action              | Description                               |
|---------------------|-------------------------------------------|
| `ui-asset`          | Static files (JS, CSS, images)            |
| `ui-config`         | UI configuration endpoint (`/v2/_angos/ui/config`) |
| `list-repositories` | Repository list view                      |
| `list-namespaces`   | Namespace list view                       |
| `list-revisions`    | Manifest list view                        |
| `list-uploads`      | Active uploads view                       |

### Minimal Policy for UI Access

```toml
[global.access_policy]
default = "deny"
rules = [
  # Allow UI to load
  "request.action == 'ui-asset' || request.action == 'ui-config'",

  # Allow authenticated users to browse
  "identity.username != null && request.action.startsWith('list-')",

  # Allow reading manifests
  "identity.username != null && request.action == 'get-manifest'"
]
```

### Read-Only Policy

```toml
rules = [
  "request.action == 'ui-asset' || request.action == 'ui-config'",
  "identity.username != null && request.action.startsWith('list-')",
  "identity.username != null && request.action == 'get-manifest'",
  "identity.username != null && request.action == 'get-referrers'",
  "identity.username != null && request.action == 'get-blob'"
]
```

### Full Access Policy

```toml
rules = [
  "request.action == 'ui-asset' || request.action == 'ui-config'",
  "identity.username != null"
]
```

---

## Views

### Repository List

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/ui-repositories-dark.png" />
  <source media="(prefers-color-scheme: light)" srcset="../images/ui-repositories-light.png" />
  <img alt="Repository List" src="../images/ui-repositories-light.png" />
</picture>

Displays all configured repositories with:
- Repository name
- Namespace count
- Feature badges:
  - **Pull-through**: Has upstream configuration
  - **Immutable**: Immutable tags enabled

### Namespace List

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/ui-namespaces-dark.png" />
  <source media="(prefers-color-scheme: light)" srcset="../images/ui-namespaces-light.png" />
  <img alt="Namespace List" src="../images/ui-namespaces-light.png" />
</picture>

Displays images within a repository:
- Image name (namespace)
- Manifest count
- Upload count (if any in progress)
- Repository configuration summary

### Manifest List

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/ui-manifests-dark.png" />
  <source media="(prefers-color-scheme: light)" srcset="../images/ui-manifests-light.png" />
  <img alt="Manifest List" src="../images/ui-manifests-light.png" />
</picture>

Tree view of all manifests:
- Multi-platform indexes with expandable children
- Platform badges (e.g., `linux/amd64`, `linux/arm64`)
- Attestations badges: SBOM, SLSA, signature, vulnerability report, etc.
- Tags as clickable badges
- Digest (shortened, click to copy full)
- Push time
- Last pull time (if tracked): the newest pull by digest or through any of its tags

### Manifest Details

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/ui-manifest-details-dark.png" />
  <source media="(prefers-color-scheme: light)" srcset="../images/ui-manifest-details-light.png" />
  <img alt="Manifest Details" src="../images/ui-manifest-details-light.png" />
</picture>

The page opens with a **title**, the full name it was addressed by (`namespace:tag` or `namespace@digest`) with a copy button, and splits the detail into tabs. Each tab has its own URL, the anchor naming it (`#history`, `#vulnerabilities`, `#filesystem`) and none for OCI, so a tab can be linked to; an anchor naming a tab the manifest lacks opens OCI. On a multi-platform index the platform shown follows, as in `#vulnerabilities/linux/arm64`.

- **OCI**: the manifest (digest, tags with delete buttons, media type, artifact type, subject, expandable annotations, a delete action), the config, the layers (or, for an ORAS artifact, its files with download links), an index's platform manifests, the referrers (signatures, SBOMs, reports), and what references the manifest. Referrers load the first 100 with the view; a "Load more referrers" control fetches the next page, which needs the `get-referrers` action.
- **Pull History**: the newest 100 recorded pulls of the reference the view was addressed by, a tag and a digest recorded separately. The heading states the configured retention, since superseded entries are collected past it; recording happens only when `update_pull_time` is enabled.
- **Vulnerabilities** (only when a report is attached, with the total finding count in a grey square on the tab): the report shown inline, its findings filterable by severity, each with the package, installed and fixed versions and a link to the advisory, and the scanner named. A multi-platform index gets a sub-tab per platform manifest that has a report. The counts also appear next to the `vuln` badge in the manifest tree and in referrer lists, as count pills coloured by severity with the severity named on hover.
- **Filesystem** (only for an image with tar layers): the layers merged into one tree, the way a runtime applies them, whiteouts removing what lower layers put there, and every entry carrying the layer that last set it, shown as a tree in the list view or as tiles walked folder by folder in the icon view. The layers menu narrows the tree to what the checked layers added, changed or removed, a path filter to matching names. Opening a file shows it inline up to 512 KiB, or offers it for download, and a symlink is followed to its target; both go through the registry's layer endpoints, which need the `get-blob` action. The first visit to an image nobody indexed shows "Indexing" until the registry's index jobs have walked its layers.

### Uploads

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/ui-uploads-dark.png" />
  <source media="(prefers-color-scheme: light)" srcset="../images/ui-uploads-light.png" />
  <img alt="Uploads" src="../images/ui-uploads-light.png" />
</picture>

Shows in-progress blob uploads:
- Upload UUID
- Current size
- Start time
- Cancel button

---

## Interactive Features

### Delete Operations

Delete buttons require double-click confirmation:
1. First click: Arms the button (changes to red)
2. Second click: Executes the deletion
3. Click elsewhere: Disarms

**Deletable items:**
- Tags (removes tag, keeps the manifest unless a retention policy allows its deletion)
- Manifests (by digest)
- Uploads (cancels in-progress uploads)

### Copy to Clipboard

The manifest page's title carries a copy button; it copies the full name, `namespace:tag` or `namespace@sha256:...`, so what is copied can be pulled as is.

### Theme Toggle

Toggle between light, dark and system themes with the switcher at the right of the top bar. Preference is saved in browser local storage.

![Dark and Light Theme](../images/ui-dark-light.png)

### Annotations Expansion

Click `[+]` to expand annotation values. Well-known annotation keys are displayed with friendly names:
- `org.opencontainers.image.title` → Title
- `org.opencontainers.image.description` → Description
- `org.opencontainers.image.version` → Version
- `org.opencontainers.image.created` → Created
- `org.opencontainers.image.source` → Source

---

## ORAS Artifacts

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/ui-oras-files-dark.png" />
  <source media="(prefers-color-scheme: light)" srcset="../images/ui-oras-files-light.png" />
  <img alt="ORAS Files" src="../images/ui-oras-files-light.png" />
</picture>

For OCI artifacts (non-container content), the UI displays:
- Filename (from annotations or media type)
- Media type
- Size
- Download button

Download URL format:
```
/v2/{namespace}/blobs/{digest}
```

---

## Platform Display

Multi-platform images show platform information:

| Badge           | Meaning                |
|-----------------|------------------------|
| `linux/amd64`   | Linux on x86_64        |
| `linux/arm64`   | Linux on ARM64         |
| `linux/arm/v7`  | Linux on ARMv7         |
| `windows/amd64` | Windows on x86_64      |
| `unknown`       | Platform not specified |

---

## Error States

| State        | Display                              |
|--------------|--------------------------------------|
| Loading      | Spinner animation                    |
| Not found    | 404 message with navigation          |
| Unauthorized | Login prompt or 401 message          |
| Forbidden    | 403 message explaining access denied |
| Server error | 500 message with retry option        |

---

## Browser Requirements

- Modern browser with JavaScript enabled
- ES2020+ support (Chrome 80+, Firefox 74+, Safari 14+, Edge 80+)
- CSS Grid and Flexbox support

---

## Related

- [Enable the Web UI](../how-to/enable-web-ui.md) - Setup guide
- [Set Up Access Control](../how-to/set-up-access-control.md) - Policy configuration
- [API Endpoints Reference](api-endpoints.md) - Extension APIs used by UI
