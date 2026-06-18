# End-to-end docker workflow scenarios

These `overlay-*.toml` files are **not** standalone configurations. Each one
contains only a scenario's `[global.access_policy]` and `[repository.*]`
sections — the registry-facing behaviour under test.

The `Build` workflow ([.github/workflows/build.yaml](../../.github/workflows/build.yaml))
runs every scenario against the same backend matrix as the conformance suite
(filesystem / S3, memory / redis / CAS lock, uniform / variable parts). For each
matrix variant it:

1. Strips the trailing `[global.access_policy]` + `[repository."conformance"]`
   block from the variant's `conformance/config-*.toml` to obtain a
   backend-only fragment.
2. Concatenates that fragment with each scenario overlay to produce a complete
   config, so the e2e workflows exercise the exact backend under test.

| Overlay                     | Scenario                                                    |
|-----------------------------|-------------------------------------------------------------|
| `overlay-push.toml`         | push an image to angos, drop it locally, pull it back       |
| `overlay-cache.toml`        | pull-through cache for Docker Hub                           |
| `overlay-replication-a.toml`| replication source; mirrors the `repl` repo to instance B   |
| `overlay-replication-b.toml`| replication target (runs on port 8001)                      |

For the replication scenario the two instances share the same backend services
(versitygw, redis), so instance B is rewritten to use an isolated S3 bucket
(`registry-b`) and redis lock prefix. Filesystem backends are already isolated
because each instance runs in its own container.
