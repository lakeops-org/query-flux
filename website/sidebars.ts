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
        'architecture/observability',
        'architecture/adding-engine-support',
        'architecture/auth-authz-design',
      ],
    },
  ],
};

export default sidebars;
