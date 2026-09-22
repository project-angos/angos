# Kustomize deployment

A stateless registry on S3, with its configuration split in two
files the way [Deploy on Kubernetes](../../../doc/how-to/deploy-kubernetes.md)
describes: `config.toml` in a ConfigMap and every secret in a Secret, both
mounted as directories and both reloaded in place when they change.

```
base/                       the registry, its Service, the prune and scrub CronJobs
components/kubernetes-oidc  lets pods pull with their own service-account token
components/kubelet-credential-provider
                            installs the kubelet plugin those pulls need, on every node
components/reconcile-replication
                            a daily `reconcile replication`, once downstreams are configured
components/reconcile-scan   a daily `reconcile scan`, refreshing reports under the scan policies
components/reconcile-index  a daily `reconcile index`, once an index policy is configured
overlays/simple             TLS terminated at an Ingress controller
overlays/gateway-api        TLS terminated at a Gateway, through an HTTPRoute
overlays/tls                TLS terminated by the registry, passed through an Ingress
overlays/tls-gateway-api    the same through a Gateway by SNI, with a client CA for mTLS
```

## Before applying

1. In `base/config.toml`, set the S3 endpoint, bucket and region, the public
   URL in `[auth.token_service] realm`, and the `[repository]` sections: what
   is hosted, who may push to it, and what is mirrored.
2. In `base/credentials-secret.yaml`, fill in the S3 keys, a signing key and
   at least one identity, then keep the filled file out of git: seal it, or
   have an ExternalSecret render the same `secrets.toml` key. The placeholders
   are refused at startup.
3. In the overlay, replace `cr.example.com`.

Then, for instance:

```bash
kubectl apply -k contrib/kubernetes/kustomize/overlays/gateway-api
```

## Notes

- `/healthz`, `/readyz` and `/metrics` are admitted without credentials, for
  the kubelet and an in-cluster scraper. The gateway-api overlay keeps them
  off the host with a redirect rule; with an Ingress controller, do the same
  by its own means.
- The prune runs at :15 and the scrub at :45 on purpose: prune deletes, scrub
  reclaims what those deletions unreferenced. Prune also clears every
  namespace no `[repository]` owns, so run `prune -d` after changing them.
- The reconcile components run daily at 03:00, 04:00 and 05:00, away from the
  hourly prune and scrub slots. Include only those whose passes the
  configuration enables: `reconcile index` in particular reclaims every
  listing no image an index policy applies to uses. An overlay lists
  them under `components:`.
- Keep the CronJobs on the Deployment's image tag. Scrub judges every stored
  key by its shape, and a newer server's keys read as unknown to an older
  scrub. Run `scrub -d` after an upgrade before the next scheduled run.
- Replicas need nothing shared: the metadata store coordinates them through
  S3 conditional requests, and the in-process cache holds only derived data
  (each provider's JWKS, upstream tokens, webhook decisions) that every
  replica may keep its own copy of. `[cache.redis]` is optional and only
  shares those entries.
