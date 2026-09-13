#!/usr/bin/env bash
# Regenerates doc/images/ui-*.png: builds angos, runs it and its scanner on a
# temporary directory with the config.toml next to this script, seeds it with
# public images and artifacts, then shoot.mjs screenshots every web UI view in
# light and dark. Needs cargo, node, oras, curl, jq, nc and trivy (SCANNER=grype
# for Grype); the first run also downloads Playwright's Chromium.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
registry=http://127.0.0.1:8900
target=localhost:8900

cargo build --manifest-path "$root/Cargo.toml"
angos=$root/target/debug/angos
(cd "$here/.." && npm install --no-audit --no-fund && npx playwright install chromium)

work=$(mktemp -d)
cd "$work"
# On failure the logs of the two processes are the only clue, so print them.
trap 'status=$?; pkill -P $$ 2>/dev/null; [ $status -eq 0 ] || tail -n 30 "$work"/*.log; rm -rf "$work"; exit $status' EXIT

RUST_LOG=warn "$angos" -c "$here/config.toml" server >server.log 2>&1 &
RUST_LOG=warn "$angos" -c "$here/config.toml" scanner "${SCANNER:-trivy}" >scanner.log 2>&1 &
# The downstream that never answers, holding the replication jobs in flight.
tail -f /dev/null | nc -kl 8902 >/dev/null 2>&1 &
# Grype pulls its database before it listens, which takes a while the first time;
# Trivy pulls its own on the first scan.
for i in $(seq 300); do
  curl -sf -o /dev/null "$registry/readyz" && curl -s -o /dev/null http://127.0.0.1:8901/ && break
  [ "$i" -eq 300 ] && { echo "angos or the scanner did not start" >&2; exit 1; }
  sleep 1
done

# A multi-platform index and two single-platform images, old enough to carry
# findings; linux/amd64 whatever the host, so the digests are the same everywhere.
oras cp docker.io/library/alpine:3.19 "$target/library/alpine:3.19"
oras tag "$target/library/alpine:3.19" latest
oras cp --platform linux/amd64 docker.io/library/nginx:1.25-alpine "$target/library/nginx:1.25-alpine"
oras cp --platform linux/amd64 docker.io/library/nginx:1.27-alpine "$target/library/nginx:1.27-alpine"

# Referrers for the SBOM and signature badges; placeholders, only their type shows.
printf '{"spdxVersion":"SPDX-2.3","name":"nginx:1.25-alpine"}' >sbom.spdx.json
oras attach --artifact-type application/spdx+json "$target/library/nginx:1.25-alpine" sbom.spdx.json:application/spdx+json
printf '{"critical":{"identity":{"docker-reference":"library/nginx"}}}' >signature.json
oras attach --artifact-type application/vnd.dev.cosign.simplesigning.v1+json "$target/library/nginx:1.25-alpine" signature.json:application/vnd.dev.cosign.simplesigning.v1+json

# An ORAS artifact with annotations, for the Files view.
cp "$root/README.md" "$root/LICENSE" .
oras push "$target/artifacts/charts/demo:1.0,latest" --artifact-type application/vnd.example.chart.v1 \
  --annotation org.opencontainers.image.title="Demo chart" \
  --annotation org.opencontainers.image.description="An ORAS artifact, as the UI lists its files" \
  --annotation org.opencontainers.image.version=1.0 \
  README.md:text/markdown LICENSE:text/plain

# One pull through the cache, so its namespace has content.
oras manifest fetch --platform linux/amd64 "$target/docker.io/library/busybox:1.36" >/dev/null

# Uploads left open, for the Uploads card.
for mib in 1 4 12; do
  location=$(curl -sS -i -o - -X POST "$registry/v2/library/nginx/blobs/uploads/" | tr -d '\r' | awk 'tolower($1) == "location:" { print $2 }')
  head -c $((mib * 1024 * 1024)) /dev/urandom | curl -sS -o /dev/null -X PATCH -H 'Content-Type: application/octet-stream' --data-binary @- "$registry$location"
done

# A few pulls under an identity, for the Pull History tab. curl sends Basic auth
# on the first request, where oras waits for a 401 this allow-all registry never
# sends, so its pulls would record as anonymous.
manifest_accept="application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json"
for _ in 1 2 3; do
  curl -sf -u ci-runner:test -H "Accept: $manifest_accept" -o /dev/null "$registry/v2/library/nginx/manifests/1.25-alpine"
done

node "$here/shoot.mjs" "$root/doc/images"
