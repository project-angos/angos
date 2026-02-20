# Angos Documentation

Welcome to the Angos documentation. Angos is a fully OCI-compliant and Docker-compatible container registry designed for resource efficiency, security, and operational simplicity.

## Getting Started

New to Angos? Start with these tutorials:

- [Quickstart](tutorials/quickstart.md) - Get a registry running in 5 minutes
- [Your First Private Registry](tutorials/your-first-private-registry.md) - Add authentication and access control
- [Mirror Docker Hub](tutorials/mirror-docker-hub.md) - Set up a pull-through cache

## How-To Guides

Step-by-step instructions for specific tasks:

### Deployment
- [Deploy with Docker Compose](how-to/deploy-docker-compose.md)
- [Deploy on Kubernetes](how-to/deploy-kubernetes.md)

### Authentication
- [Configure mTLS](how-to/configure-mtls.md)
- [Configure GitHub Actions OIDC](how-to/configure-github-actions-oidc.md)
- [Configure Generic OIDC](how-to/configure-generic-oidc.md)

### Policies
- [Set Up Access Control](how-to/set-up-access-control.md)
- [Configure Retention Policies](how-to/configure-retention-policies.md)
- [Protect Tags with Immutability](how-to/protect-tags-immutability.md)
- [Configure Webhook Authorization](how-to/configure-webhook-authorization.md)

### Events
- [Configure Event Webhooks](how-to/configure-event-webhooks.md)

### Operations
- [Run Storage Maintenance](how-to/run-storage-maintenance.md)
- [Enable the Web UI](how-to/enable-web-ui.md)
- [Troubleshoot Common Issues](how-to/troubleshoot-common-issues.md)

## Reference

Detailed technical reference:

- [Configuration Reference](reference/configuration.md) - All configuration options
- [CLI Reference](reference/cli.md) - Command-line interface
- [CEL Expressions Reference](reference/cel-expressions.md) - Policy language reference
- [API Endpoints Reference](reference/api-endpoints.md) - OCI and extension APIs
- [Web UI Reference](reference/ui.md) - Web interface
- [Event Webhooks Reference](reference/event-webhooks.md) - Event webhook configuration and payloads
- [Metrics Reference](reference/metrics.md) - Prometheus metrics

## Understanding Angos

Conceptual explanations and architecture:

- [Architecture Overview](explanation/architecture.md) - System design and components
- [Storage Backends](explanation/storage-backends.md) - Filesystem vs S3
- [Authentication and Authorization](explanation/authentication-authorization.md) - Security model
- [Pull-Through Caching](explanation/pull-through-caching.md) - How caching works
- [Security Model](explanation/security-model.md) - Trust boundaries and best practices

## Key Features

- **OCI-compliant**: Full OCI Distribution Specification v1.1 support
- **Pull-through cache**: Mirror upstream registries with intelligent caching
- **Access control**: CEL-based policies and webhook authorization
- **Retention policies**: Automated cleanup with flexible rules
- **Immutable tags**: Protect releases from overwrites
- **mTLS support**: Client certificate authentication
- **OIDC authentication**: GitHub Actions, Google, Okta, and more
- **Event webhooks**: Notify external systems with required, optional, or async delivery
- **Online garbage collection**: Clean up without downtime
- **Web UI**: Browse and manage images visually

## Getting Help

- [Troubleshoot Common Issues](how-to/troubleshoot-common-issues.md)
- [GitHub Issues](https://github.com/project-angos/angos/issues)
