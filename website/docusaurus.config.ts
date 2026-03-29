import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// Keep in sync with `url` + `baseUrl` (used in headTags / structured data). Project Pages:
// https://<org>.github.io/<repo>/
const siteUrl = 'https://lakeops-org.github.io';
/** Public docs homepage (project Pages: host + repo path, no trailing slash). */
const siteCanonicalUrl = `${siteUrl}/queryflux`;

/** Local dev: `npm run dev` sets this so the site is at http://localhost:3000/ (not /queryflux/). */
const useRootBaseUrl =
  process.env.DOCUSAURUS_USE_ROOT_BASE === 'true' ||
  process.env.DOCUSAURUS_USE_ROOT_BASE === '1';

const config: Config = {
  title: 'QueryFlux',
  tagline: 'Universal SQL query proxy and router in Rust',
  favicon: 'img/queryflux-logo.png',

  // Must match GitHub Pages path: repo `queryflux` → baseUrl `/queryflux/`.
  url: siteUrl,
  baseUrl: useRootBaseUrl ? '/' : '/queryflux/',

  // Used by `npm run deploy` to pick the target repo — must match `git remote` (org/repo).
  organizationName: 'lakeops-org',
  projectName: 'queryflux',

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
        codeRepository: 'https://github.com/lakeops-org/queryflux',
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
    // Confirms you hit `npm run dev` (root baseUrl), not production or `npm start` (/queryflux/).
    ...(useRootBaseUrl
      ? [
          {
            tagName: 'meta',
            attributes: {
              name: 'queryflux-dev',
              content: 'root-baseUrl',
            },
          },
        ]
      : []),
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
            'https://github.com/lakeops-org/queryflux/tree/main/website/',
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
    // 1200×630 recommended for og:image (not the square logo). Replace with branded art anytime.
    image: 'img/queryflux-logo_default.png',
    colorMode: {
      defaultMode: 'light',
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'QueryFlux',
      hideOnScroll: false,
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          type: 'search',
          position: 'right',
        },
        {
          type: 'docsVersionDropdown',
          position: 'right',
        },
        {
          href: 'https://github.com/lakeops-org/queryflux',
          label: 'GitHub',
          position: 'right',
          className: 'navbar-link-github',
        },
      ],
    },
    footer: {
      style: 'dark',
      logo: {
        alt: 'QueryFlux',
        src: 'img/queryflux-logo.png',
        height: 36,
        href: 'https://github.com/lakeops-org/queryflux',
      },
      links: [
        {
          title: 'Documentation',
          items: [
            {label: 'Introduction', to: '/docs/intro'},
            {label: 'Getting started', to: '/docs/getting-started'},
            {label: 'Architecture', to: '/docs/architecture/overview'},
          ],
        },
        {
          title: 'Project',
          items: [
            {label: 'Configuration', to: '/docs/configuration'},
            {label: 'Development', to: '/docs/development'},
            {label: 'Contribute', to: '/docs/contribute'},
          ],
        },
        {
          title: 'Repository',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/lakeops-org/queryflux',
            },
            {
              label: 'Issues',
              href: 'https://github.com/lakeops-org/queryflux/issues',
            },
          ],
        },
        {
          title: 'LakeOps',
          items: [{label: 'lakeops.dev', href: 'https://lakeops.dev'}],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} LakeOps. QueryFlux documentation — Apache-2.0.`,
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
          'LakeOps, QueryFlux, SQL proxy, query router, Trino, PostgreSQL, MySQL, Arrow Flight, DuckDB, StarRocks, Rust, sqlglot, data lake',
      },
      {name: 'twitter:card', content: 'summary_large_image'},
      {property: 'og:type', content: 'website'},
      {property: 'og:image:width', content: '1200'},
      {property: 'og:image:height', content: '630'},
    ],
  } satisfies Preset.ThemeConfig,
};

export default config;
