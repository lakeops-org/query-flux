import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Many front doors',
    description: (
      <>
        Trino HTTP, PostgreSQL wire, MySQL wire, and Arrow Flight SQL —
        route with rules instead of one engine per deployment.
      </>
    ),
  },
  {
    title: 'Heterogeneous backends',
    description: (
      <>
        Trino, DuckDB, StarRocks, and more — with sqlglot-backed dialect
        translation when the client and engine disagree on SQL.
      </>
    ),
  },
  {
    title: 'Operator ready',
    description: (
      <>
        Per-group concurrency and queuing, Prometheus metrics, optional
        PostgreSQL-backed state, and an admin API.
      </>
    ),
  },
];

function Feature({title, description}: FeatureItem): ReactNode {
  return (
    <div className={styles.featureCol}>
      <div className={styles.featureCard}>
        <Heading as="h3" className={styles.featureTitle}>
          {title}
        </Heading>
        <p className={styles.featureBody}>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features} aria-labelledby="homepage-features-heading">
      <div className={clsx('container', styles.featuresGrid)}>
        <h2 id="homepage-features-heading" className={styles.sectionHeading}>Built for multi-engine SQL estates</h2>
        <p className={styles.sectionSub}>
          Same patterns you expect from a serious proxy — explicit routing, capacity
          limits, and observability — without locking teams to a single database.
        </p>
        <div className={styles.featureRow}>
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
