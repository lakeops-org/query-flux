import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// Keep in sync with `url` + `baseUrl` (used in headTags / structured data). Project Pages:
// https://<org>.github.io/<repo>/
const siteUrl = 'https://lakeops-org.github.io';
/** Public docs homepage (project Pages: host + repo path, no trailing slash). */
const siteCanonicalUrl = `${siteUrl}/query-flux`;

const config: Config = {
  title: 'QueryFlux',
  tagline: 'Universal SQL query proxy and router in Rust',
  favicon: 'img/queryflux-logo.png',

  // Must match GitHub Pages path: repo `query-flux` → baseUrl `/query-flux/`.
  url: siteUrl,
  baseUrl: '/query-flux/',

  // Used by `npm run deploy` to pick the target repo — must match `git remote` (org/repo).
  organizationName: 'lakeops-org',
  projectName: 'query-flux',

  // https://docusaurus.io/docs/deployment#deploying-to-github-pages
  trailingSlash: false,

  onBrokenLinks: 'throw',

  // https://docusaurus.io/docs/seo — global <head> injection
  headTags: [
    {
      tagName: 'link',
      attributes: {
        rel: 'preconnect',
        href: 'https://github.com',
      },
    },
    {
      tagName: 'script',
      attributes: {
        type: 'application/ld+json',
      },
      innerHTML: JSON.stringify({
        '@context': 'https://schema.org',
        '@type': 'SoftwareSourceCode',
        name: 'QueryFlux',
        description:
          'Universal SQL query proxy and router in Rust. Front protocols include Trino HTTP, PostgreSQL wire, MySQL wire, and Arrow Flight; backends include Trino, DuckDB, StarRocks, and more with routing and sqlglot dialect translation.',
        url: siteCanonicalUrl,
        codeRepository: 'https://github.com/lakeops-org/query-flux',
        license: 'https://www.apache.org/licenses/LICENSE-2.0',
        programmingLanguage: 'Rust',
      }),
    },
    {
      tagName: 'script',
      attributes: {
        type: 'application/ld+json',
      },
      innerHTML: JSON.stringify({
        '@context': 'https://schema.org',
        '@type': 'WebSite',
        name: 'QueryFlux',
        url: siteCanonicalUrl,
        description:
          'Documentation and resources for QueryFlux, a universal SQL query proxy and router.',
      }),
    },
  ],

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  // https://docusaurus.io/docs/search#using-local-search — offline index, no Algolia
  plugins: [
    [
      '@cmfcmf/docusaurus-search-local',
      {
        indexBlog: false,
        indexPages: true,
        language: 'en',
      },
    ],
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl:
            'https://github.com/lakeops-org/query-flux/tree/main/website/',
          // https://docusaurus.io/docs/versioning — latest = first entry in versions.json
          versions: {
            current: {
              label: 'Next',
              path: 'next',
              banner: 'unreleased',
            },
          },
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    // Default Open Graph / Twitter card image (path under static/).
    image: 'img/queryflux-logo.png',
    colorMode: {
      defaultMode: 'dark',
      // If true, OS "prefers light" overrides defaultMode for first visit.
      respectPrefersColorScheme: false,
    },
    navbar: {
      title: 'QueryFlux',
      logo: {
        alt: 'QueryFlux',
        src: 'img/queryflux-logo.png',
        style: {height: '1.85rem', width: 'auto'},
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          type: 'docsVersionDropdown',
          position: 'left',
        },
        {
          href: 'https://github.com/lakeops-org/query-flux',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {
              label: 'Getting started',
              to: '/docs/getting-started',
            },
            {
              label: 'Architecture',
              to: '/docs/architecture/overview',
            },
          ],
        },
        {
          title: 'Repository',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/lakeops-org/query-flux',
            },
            {
              label: 'Issues',
              href: 'https://github.com/lakeops-org/query-flux/issues',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} QueryFlux contributors. Apache-2.0.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['rust', 'yaml', 'bash', 'python'],
    },
    // https://docusaurus.io/docs/seo#global-metadata
    metadata: [
      {
        name: 'keywords',
        content:
          'QueryFlux, SQL proxy, query router, Trino, PostgreSQL, MySQL, Arrow Flight, DuckDB, StarRocks, Rust, sqlglot, load balancing, Iceberg',
      },
      {name: 'twitter:card', content: 'summary_large_image'},
      {property: 'og:type', content: 'website'},
    ],
  } satisfies Preset.ThemeConfig,
};

export default config;
