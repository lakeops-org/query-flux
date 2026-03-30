import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

const nextSteps: string[] = [
  'Getting started — Docker Compose examples (minimal, full stack), curl, and make dev for contributors',
  'Architecture — system map, routing mechanics, dialect translation, observability',
  'Motivation and goals — the full analysis: fragmented engine landscape, cost modeling, and the case for a proxy',
  'Configuration reference — complete YAML reference for clusters, routers, auth, and persistence',
  'Roadmap — what is shipped, what is next (ClickHouse, cost-aware routing, Snowflake, BigQuery)',
];

function StepCard({text, full}: {text: string; full?: boolean}): ReactNode {
  return (
    <article className={clsx(styles.cardCol, full && styles.cardColFull)}>
      <div className={styles.card}>
        <div className={styles.cardAccent} aria-hidden />
        <p className={styles.cardText}>{text}</p>
      </div>
    </article>
  );
}

export default function HomepageNextSteps(): ReactNode {
  return (
    <section className={styles.section} aria-labelledby="homepage-next-steps-heading">
      <div className={clsx('container', styles.grid)}>
        <h2 id="homepage-next-steps-heading" className={styles.sectionHeading}>
          Next steps
        </h2>
        <div className={styles.cardRow}>
          {nextSteps.map((text, idx) => (
            <StepCard key={text} text={text} full={idx === nextSteps.length - 1} />
          ))}
        </div>
      </div>
    </section>
  );
}

