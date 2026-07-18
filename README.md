<div align="center">

# Angos

<img src="doc/images/angos-hero.svg" alt="Angos Logo" width="120" height="120">

A fully OCI-compliant and Docker-compatible container registry.

**[Website](https://project-angos.github.io/angos/)** | **[Documentation](doc/)** | **[Quick Start](doc/tutorials/quickstart.md)**

</div>

## Key Features

- Online garbage collection
- Pull-through cache
- Immutable tags with configurable exclusions
- Access control policies (CEL-based)
- Retention policies
- Native mTLS support
- OIDC authentication (GitHub Actions, Google, Okta, and more)
- Webhook authorization for external policy decisions
- Web UI for browsing and managing images

## Properties

- Resource efficient: Asynchronous, streaming operations
- Secure: mTLS, OIDC/JWT authentication, authorization policies (CEL and webhooks)
- Scalable: Light footprint, S3-compatible storage, distributed locking
- Easy to operate: Online garbage collection, auto-reload of configuration and certificates
- Cross-platform: Portable on most mainstream operating systems just by recompiling

## Quick Start

```bash
# Create a minimal config
cat > config.toml << 'EOF'
[server]
bind_address = "0.0.0.0"
port = 8000

[blob_store.fs]
root_dir = "./registry-data"

[global.access_policy]
default = "allow"

[repository."test"]
EOF

# Run the registry
./angos -c config.toml server

# Push an image
docker tag alpine:latest localhost:8000/test/alpine:latest
docker push localhost:8000/test/alpine:latest
```

See the [Quickstart Tutorial](doc/tutorials/quickstart.md) for a complete walkthrough.

## Documentation

The complete documentation index lives in [doc/index.md](doc/index.md).

### Tutorials

- [Quickstart](doc/tutorials/quickstart.md) - Get a registry running in 5 minutes
- [Your First Private Registry](doc/tutorials/your-first-private-registry.md) - Add authentication and access control
- [Mirror Docker Hub](doc/tutorials/mirror-docker-hub.md) - Set up a pull-through cache

### How-To Guides

- [Deploy with Docker Compose](doc/how-to/deploy-docker-compose.md)
- [Deploy on Kubernetes](doc/how-to/deploy-kubernetes.md)
- [Configure mTLS](doc/how-to/configure-mtls.md)
- [Configure GitHub Actions OIDC](doc/how-to/configure-github-actions-oidc.md)
- [Configure Generic OIDC](doc/how-to/configure-generic-oidc.md)
- [Set Up Access Control](doc/how-to/set-up-access-control.md)
- [Configure Retention Policies](doc/how-to/configure-retention-policies.md)
- [Protect Tags with Immutability](doc/how-to/protect-tags-immutability.md)
- [Configure Webhook Authorization](doc/how-to/configure-webhook-authorization.md)
- [Configure Event Webhooks](doc/how-to/configure-event-webhooks.md)
- [Configure Replication](doc/how-to/configure-replication.md)
- [Run Storage Maintenance](doc/how-to/run-storage-maintenance.md)
- [Enable Durable Cache Jobs](doc/how-to/durable-cache-jobs.md)
- [Enable the Web UI](doc/how-to/enable-web-ui.md)
- [Troubleshoot Common Issues](doc/how-to/troubleshoot-common-issues.md)
- [Upgrade Angos](doc/how-to/upgrade.md)

### Reference

- [Configuration Reference](doc/reference/configuration.md)
- [CLI Reference](doc/reference/cli.md)
- [CEL Expressions Reference](doc/reference/cel-expressions.md)
- [API Endpoints Reference](doc/reference/api-endpoints.md)
- [Web UI Reference](doc/reference/ui.md)
- [Event Webhooks Reference](doc/reference/event-webhooks.md)
- [Metrics Reference](doc/reference/metrics.md)

### Understanding Angos

- [Architecture Overview](doc/explanation/architecture.md)
- [Storage Backends](doc/explanation/storage-backends.md)
- [Authentication and Authorization](doc/explanation/authentication-authorization.md)
- [Pull-Through Caching](doc/explanation/pull-through-caching.md)
- [Bi-Directional Replication](doc/explanation/replication.md)
- [Security Model](doc/explanation/security-model.md)

## Upgrading

Version-specific migration notes are in the [Upgrade guide](doc/how-to/upgrade.md).

## Usage

```
Usage: angos [-c <config>] <command> [<args>]

An OCI-compliant and docker-compatible registry service

Options:
  -c, --config      the path to the configuration file, defaults to
                    `config.toml`
  --help, help      display usage information

Commands:
  argon             Hash a password following the argon2id algorithm
  migrate           Convert pre-JSON bare-digest link metadata to the current
                    JSON format
  prune             Enforce retention policies and reclaim aged upload-lifecycle
                    leftovers
  replicate         Reconcile replicated namespaces with their configured
                    downstreams
  scrub             Walk the store, repair inconsistencies, and quarantine
                    unrecognized objects
  server            Run the registry listeners
  worker            Process durable background jobs
```

## Additional Endpoints

In addition to the standard OCI Distribution endpoints:

- `/healthz`: Liveness health check endpoint
- `/readyz`: Readiness health check endpoint
- `/metrics`: Prometheus metrics endpoint

## References

- [OCI Distribution Specification](https://github.com/opencontainers/distribution-spec/blob/main/spec.md)
- [OCI Image Specification](https://github.com/opencontainers/image-spec)
- [OCI Image Index](https://github.com/opencontainers/image-spec/blob/main/image-index.md)
- [Docker Registry HTTP API V2](https://github.com/openshift/docker-distribution/blob/master/docs/spec/api.md)
