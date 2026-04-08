import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type OptionItem = {
  title: string;
  to: string;
  body: string;
  cta: string;
  icon: ReactNode;
};

const options: OptionItem[] = [
  {
    title: 'Getting started',
    to: '/docs/getting-started',
    body:
      'Docker Compose examples, ports, and a one-line curl to verify Trino HTTP through QueryFlux.',
    cta: 'Run your first stack',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        <path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/>
        <polyline points="9 22 9 12 15 12 15 22"/>
      </svg>
    ),
  },
  {
    title: 'Configuration',
    to: '/docs/configuration',
    body:
      'Full YAML reference: frontends, cluster groups, routing rules, admin API, and persistence.',
    cta: 'Open the YAML reference',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        <circle cx="12" cy="12" r="3"/>
        <path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 012.83-2.83l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z"/>
      </svg>
    ),
  },
  {
    title: 'Architecture',
    to: '/docs/architecture/overview',
    body:
      'How components fit together: frontends, router, cluster groups, translation, observability.',
    cta: 'Read the system overview',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        <rect x="3" y="3" width="7" height="7" rx="1"/>
        <rect x="14" y="3" width="7" height="7" rx="1"/>
        <rect x="14" y="14" width="7" height="7" rx="1"/>
        <rect x="3" y="14" width="7" height="7" rx="1"/>
      </svg>
    ),
  },
  {
    title: 'Roadmap',
    to: '/docs/roadmap',
    body:
      'What is shipped, in progress, and planned — engines, routing, and integrations.',
    cta: "See what's next",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        <line x1="12" y1="20" x2="12" y2="10"/>
        <line x1="18" y1="20" x2="18" y2="4"/>
        <line x1="6" y1="20" x2="6" y2="16"/>
      </svg>
    ),
  },
];

function OptionCard({title, to, body, cta, icon}: OptionItem): ReactNode {
  return (
    <article className={styles.optionCol}>
      <Link className={styles.optionCard} to={to}>
        <div className={styles.cardAccent} aria-hidden />
        <div className={styles.optionIconWrap} aria-hidden="true">{icon}</div>
        <Heading as="h3" className={styles.optionTitle}>
          {title}
        </Heading>
        <p className={styles.optionText}>{body}</p>
        <span className={styles.optionAction}>{cta} →</span>
      </Link>
    </article>
  );
}

export default function HomepageNextSteps(): ReactNode {
  return (
    <section className={styles.section} aria-labelledby="homepage-getting-started-heading">
      <div className={clsx('container', styles.grid)}>
        <h2 id="homepage-getting-started-heading" className={styles.sectionHeading}>
          Documentation
        </h2>
        <p className={styles.sectionSub}>
          Quick guides and reference — same layout as the sidebar.
        </p>

        <section className={styles.primaryCta} aria-label="Quick start with Docker Compose">
          <div className={styles.cardAccent} aria-hidden />
          <p className={styles.primaryEyebrow}>Overview</p>
          <Heading as="h3" className={styles.primaryTitle}>
            What QueryFlux does
          </Heading>
          <p className={styles.primaryText}>
            Read the documentation home for quick guides, how routing works, and links into the
            reference manual — then open Getting started when you are ready to run a stack.
          </p>
          <div className={styles.primaryActions}>
            <Link className={clsx('button button--lg', styles.primaryButton)} to="/docs/intro">
              Open documentation overview
            </Link>
            <Link className={styles.secondaryLink} to="/docs/getting-started">
              Skip to Docker Compose
            </Link>
          </div>
        </section>

        <div className={styles.optionRow}>
          {options.map((item) => (
            <OptionCard key={item.title} {...item} />
          ))}
        </div>
      </div>
    </section>
  );
}
