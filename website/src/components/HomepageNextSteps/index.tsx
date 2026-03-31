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
};

const options: OptionItem[] = [
  {
    title: 'Architecture',
    to: '/docs/intro#how-does-it-work',
    body:
      'How queries move through QueryFlux: protocol ingestion, routing rules, cluster groups, dialect translation, and dispatch.',
    cta: 'See how queries flow',
  },
  {
    title: 'Motivation and goals',
    to: '/docs/intro#what-is-queryflux',
    body:
      'Why a universal SQL proxy matters: fragmented engines, N×M integrations, and QueryFlux as the compute interoperability layer.',
    cta: 'Understand the problem space',
  },
  {
    title: 'Configuration reference',
    to: '/docs/intro#next-steps',
    body:
      'Start from the intro’s next steps: Docker Compose, architecture deep-dives, full YAML reference, and roadmap links.',
    cta: 'Follow the documentation map',
  },
  {
    title: 'Roadmap',
    to: '/docs/roadmap',
    body:
      'What is shipped, in progress, and planned — engines, routing features, and integrations.',
    cta: "See what's shipped and what's next",
  },
];

function OptionCard({title, to, body, cta}: OptionItem): ReactNode {
  return (
    <article className={styles.optionCol}>
      <Link className={styles.optionCard} to={to}>
        <div className={styles.cardAccent} aria-hidden />
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
          Getting started
        </h2>
        <p className={styles.sectionSub}>
          Start fast, then dive deeper by track.
        </p>

        <section className={styles.primaryCta} aria-label="Quick start with Docker Compose">
          <div className={styles.cardAccent} aria-hidden />
          <p className={styles.primaryEyebrow}>Quick start</p>
          <Heading as="h3" className={styles.primaryTitle}>
            Run with Docker Compose
          </Heading>
          <p className={styles.primaryText}>
            The fastest path is a Docker Compose example under <code>examples/</code> in the
            repository — then read the intro for how routing and protocols fit together.
          </p>
          <div className={styles.primaryActions}>
            <Link className={clsx('button button--lg', styles.primaryButton)} to="/docs/getting-started">
              Open getting started guide
            </Link>
            <Link className={styles.secondaryLink} to="/docs/intro#what-is-queryflux">
              Read the project overview
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

