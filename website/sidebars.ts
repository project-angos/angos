import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  tutorials: [
    {
      type: 'category',
      label: 'Learn',
      collapsible: false,
      items: [
        'tutorials/quickstart',
        'tutorials/your-first-private-registry',
        'tutorials/mirror-docker-hub',
      ],
    },
  ],
  howto: [
    {
      type: 'category',
      label: 'Guides',
      collapsible: false,
      items: [
        'how-to/deploy-docker-compose',
        'how-to/deploy-kubernetes',
        'how-to/configure-mtls',
        'how-to/configure-github-actions-oidc',
        'how-to/push-from-github-actions',
        'how-to/configure-generic-oidc',
        'how-to/configure-kubernetes-oidc',
        'how-to/set-up-access-control',
        'how-to/configure-retention-policies',
        'how-to/protect-tags-immutability',
        'how-to/configure-webhook-authorization',
        'how-to/configure-event-webhooks',
        'how-to/scan-images',
        'how-to/explore-image-filesystems',
        'how-to/configure-replication',
        'how-to/run-storage-maintenance',
        'how-to/durable-cache-jobs',
        'how-to/enable-web-ui',
        'how-to/troubleshoot-common-issues',
        'how-to/upgrade',
      ],
    },
  ],
  reference: [
    {
      type: 'category',
      label: 'Reference',
      collapsible: false,
      items: [
        'reference/configuration',
        'reference/cli',
        'reference/cel-expressions',
        'reference/api-endpoints',
        'reference/ui',
        'reference/event-webhooks',
        'reference/metrics',
      ],
    },
  ],
  explanation: [
    {
      type: 'category',
      label: 'Concepts',
      collapsible: false,
      items: [
        'explanation/architecture',
        'explanation/storage-backends',
        'explanation/authentication-authorization',
        'explanation/pull-through-caching',
        'explanation/replication',
        'explanation/security-model',
      ],
    },
  ],
};

export default sidebars;
