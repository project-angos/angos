#!/bin/sh
# Bumps the version, finalises the changelog, tags, and pushes commit + tag atomically.
# Usage: contrib/release.sh X.Y.Z
set -eu
v=${1:?usage: $0 X.Y.Z}
case $v in *[!0-9.]*) echo "bad version: $v" >&2; exit 1;; esac

[ "$(git branch --show-current)" = main ] || { echo "not on main" >&2; exit 1; }
git diff --quiet HEAD || { echo "uncommitted changes" >&2; exit 1; }
git fetch -q origin main
[ "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)" ] || { echo "main != origin/main" >&2; exit 1; }
grep -qx "## $v - Unreleased" CHANGELOG.md || { echo "CHANGELOG.md has no '## $v - Unreleased'" >&2; exit 1; }

perl -pi -e "s/^## \Q$v\E - Unreleased\$/## $v/" CHANGELOG.md
perl -0pi -e "s/(\[package\]\nname = \"angos\"\nversion = )\"[^\"]*\"/\${1}\"$v\"/" Cargo.toml
cargo update --workspace

git commit -q -m "chore: Release $v" Cargo.toml Cargo.lock CHANGELOG.md
git tag -a "v$v" -m "Release $v"
git push --atomic origin main "v$v"
