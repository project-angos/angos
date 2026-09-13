import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import remarkThemedImages from './src/plugins/remark-themed-images.mjs';

const config: Config = {
  title: 'Angos',
  tagline: 'A lightweight, OCI-compliant container registry',
  favicon: 'img/logo.svg',

  url: 'https://angos.dev',
  baseUrl: '/',

  organizationName: 'angos',
  projectName: 'angos',

  onBrokenLinks: 'throw',

  headTags: [
    {
      tagName: 'link',
      attributes: {
        rel: 'icon',
        type: 'image/png',
        href: '/favicon-32x32.png',
      },
    },
  ],

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    mermaid: true,
  },

  themes: ['@docusaurus/theme-mermaid'],

  presets: [
    [
      'classic',
      {
        docs: {
          path: '../doc',
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/project-angos/angos/tree/main/doc/',
          beforeDefaultRemarkPlugins: [remarkThemedImages],
        },
        pages: {},
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/social-card.png',
    navbar: {
      title: 'Angos',
      logo: {
        alt: 'Angos Logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          to: '/',
          label: 'Home',
          position: 'right',
          activeBaseRegex: '^/$',
        },
        {
          to: '/docs/tutorials/quickstart',
          label: 'Learn',
          position: 'right',
          activeBasePath: '/docs/tutorials',
        },
        {
          to: '/docs/how-to/deploy-docker-compose',
          label: 'Guides',
          position: 'right',
          activeBasePath: '/docs/how-to',
        },
        {
          to: '/docs/reference/configuration',
          label: 'Reference',
          position: 'right',
          activeBasePath: '/docs/reference',
        },
        {
          to: '/docs/explanation/architecture',
          label: 'Concepts',
          position: 'right',
          activeBasePath: '/docs/explanation',
        },
        {
          href: 'https://github.com/project-angos/angos',
          position: 'right',
          className: 'header-github-link',
          'aria-label': 'GitHub repository',
        },
      ],
    },
    footer: {
      style: 'light',
      links: [
        {
          title: 'Learn',
          items: [
            {label: 'Quickstart', to: '/docs/tutorials/quickstart'},
            {label: 'Private Registry', to: '/docs/tutorials/your-first-private-registry'},
            {label: 'Mirror Docker Hub', to: '/docs/tutorials/mirror-docker-hub'},
          ],
        },
        {
          title: 'Guides',
          items: [
            {label: 'Docker Compose', to: '/docs/how-to/deploy-docker-compose'},
            {label: 'Kubernetes', to: '/docs/how-to/deploy-kubernetes'},
            {label: 'Access Control', to: '/docs/how-to/set-up-access-control'},
            {label: 'Troubleshooting', to: '/docs/how-to/troubleshoot-common-issues'},
          ],
        },
        {
          title: 'Reference',
          items: [
            {label: 'Configuration', to: '/docs/reference/configuration'},
            {label: 'CLI', to: '/docs/reference/cli'},
            {label: 'API Endpoints', to: '/docs/reference/api-endpoints'},
          ],
        },
        {
          title: 'Community',
          items: [
            {label: 'GitHub', href: 'https://github.com/project-angos/angos'},
            {label: 'Issues', href: 'https://github.com/project-angos/angos/issues'},
            {label: 'Releases', href: 'https://github.com/project-angos/angos/releases'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Angos Maintainers.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.vsDark,
      additionalLanguages: ['bash', 'toml', 'yaml', 'rust', 'json'],
    },
    mermaid: {
      theme: {light: 'neutral', dark: 'dark'},
    },
    colorMode: {
      defaultMode: 'light',
      disableSwitch: false,
      respectPrefersColorScheme: true,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
