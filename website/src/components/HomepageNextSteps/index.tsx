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
    to: '/docs/architecture/overview',
    body:
      'This section describes how QueryFlux is put together: why it exists, how SQL is translated, and how traffic is routed to cluster groups and individual clusters.',
    cta: 'Explore architecture',
  },
  {
    title: 'Motivation and goals',
    to: '/docs/architecture/motivation-and-goals',
    body:
      'Modern data stacks are fragmented by design. Different engines exist because different problems demand different trade-offs.',
    cta: 'Read the motivation',
  },
  {
    title: 'Configuration reference',
    to: '/docs/configuration',
    body: 'Copy config.example.yaml in the repository root and adjust for your environment.',
    cta: 'Open configuration',
  },
  {
    title: 'Roadmap',
    to: '/docs/roadmap',
    body:
      'QueryFlux is under active development. This page tracks what is shipped, what is in progress, and where the project is headed.',
    cta: 'See roadmap',
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

        <section className={styles.primaryCta} aria-label="Get started">
          <div className={styles.cardAccent} aria-hidden />
          <p className={styles.primaryEyebrow}>Quick start</p>
          <Heading as="h3" className={styles.primaryTitle}>
            Getting started
          </Heading>
          <p className={styles.primaryText}>
            The fastest way to run QueryFlux is one of the Docker Compose examples under
            examples/ in the repository.
          </p>
          <div className={styles.primaryActions}>
            <Link className={clsx('button button--lg', styles.primaryButton)} to="/docs/getting-started">
              Get started
            </Link>
            <Link className={styles.secondaryLink} to="/docs/intro">
              Read intro
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

