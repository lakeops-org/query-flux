import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'intro',
    {
      type: 'category',
      label: 'Guide',
      collapsed: false,
      items: [
        'getting-started',
        'configuration',
        'project-structure',
        'benchmarks',
        'development',
        'contribute',
        'roadmap',
      ],
    },
    {
      type: 'category',
      label: 'Architecture',
      collapsed: false,
      items: [
        'architecture/overview',
        'architecture/motivation-and-goals',
        'architecture/system-map',
        'architecture/query-translation',
        'architecture/routing-and-clusters',
        'architecture/query-tags',
        'architecture/observability',
        {
          type: 'category',
          label: 'Frontends',
          collapsed: false,
          items: [
            'architecture/frontends/overview',
            'architecture/frontends/trino-http',
            'architecture/frontends/postgres-wire',
            'architecture/frontends/mysql-wire',
            'architecture/frontends/flight-sql',
            'architecture/frontends/snowflake',
          ],
        },
        {
          type: 'category',
          label: 'Extending QueryFlux',
          collapsed: false,
          items: [
            'architecture/adding-support/overview',
            'architecture/adding-support/backend',
            'architecture/adding-support/frontend',
          ],
        },
        'architecture/auth-authz-design',
      ],
    },
  ],
};

export default sidebars;
