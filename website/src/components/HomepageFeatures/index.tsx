import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  icon: ReactNode;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Many front doors',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        <rect x="2" y="3" width="4" height="4" rx="1"/>
        <rect x="2" y="10" width="4" height="4" rx="1"/>
        <rect x="2" y="17" width="4" height="4" rx="1"/>
        <path d="M6 5h5l3 7-3 7H6"/>
        <rect x="16" y="9" width="6" height="6" rx="2"/>
      </svg>
    ),
    description: (
      <>
        Trino HTTP, PostgreSQL wire, MySQL wire, and Arrow Flight SQL —
        route with rules instead of one engine per deployment.
      </>
    ),
  },
  {
    title: 'Heterogeneous backends',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        <ellipse cx="12" cy="5" rx="9" ry="3"/>
        <path d="M3 5v4c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
        <path d="M3 9v4c0 1.66 4 3 9 3s9-1.34 9-3V9"/>
        <path d="M3 13v4c0 1.66 4 3 9 3s9-1.34 9-3v-4"/>
      </svg>
    ),
    description: (
      <>
        Trino, DuckDB, StarRocks, and more — with sqlglot-backed dialect
        translation when the client and engine disagree on SQL.
      </>
    ),
  },
  {
    title: 'Operator ready',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        <line x1="4" y1="21" x2="4" y2="14"/>
        <line x1="4" y1="10" x2="4" y2="3"/>
        <line x1="12" y1="21" x2="12" y2="12"/>
        <line x1="12" y1="8" x2="12" y2="3"/>
        <line x1="20" y1="21" x2="20" y2="16"/>
        <line x1="20" y1="12" x2="20" y2="3"/>
        <line x1="1" y1="14" x2="7" y2="14"/>
        <line x1="9" y1="8" x2="15" y2="8"/>
        <line x1="17" y1="16" x2="23" y2="16"/>
      </svg>
    ),
    description: (
      <>
        Per-group concurrency and queuing, Prometheus metrics, optional
        PostgreSQL-backed state, and an admin API.
      </>
    ),
  },
];

function Feature({title, icon, description}: FeatureItem): ReactNode {
  return (
    <div className={styles.featureCol}>
      <div className={styles.featureCard}>
        <div className={styles.featureIconWrap} aria-hidden="true">
          {icon}
        </div>
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
