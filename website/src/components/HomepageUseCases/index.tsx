import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type UseCase = {
  title: string;
  body: ReactNode;
};

const useCases: UseCase[] = [
  {
    title: 'Multi-engine data platform',
    body: (
      <>
        QueryFlux is one front door with routing rules: BI tools connect over MySQL wire and land
        on StarRocks, scheduled jobs stay on Trino, and ad-hoc{' '}
        <code className={styles.inlineCode}>SELECT *</code>-style exploration can be matched by
        regex and sent to Athena. Each engine gets the workload it is built for—no new connection
        strings or drivers for your teams.
      </>
    ),
  },
  {
    title: 'Cost-aware workload dispatch',
    body: (
      <>
        A Python router in QueryFlux inspects every query and steers CPU-heavy joins and windows
        to compute-priced Trino while scan-heavy Iceberg reads go to scan-priced Athena. You encode
        the cost model once; every client inherits the same dispatch automatically.
      </>
    ),
  },
  {
    title: 'Dashboard SLA protection',
    body: (
      <>
        Put a <code className={styles.inlineCode}>maxRunningQueries</code> cap on the StarRocks
        group so dashboard traffic always has headroom: when the group is full, ad-hoc queries
        queue at the proxy or spill to a Trino fallback. Grafana can show queue depth in real
        time—dashboards keep a fast path while analysts wait transparently.
      </>
    ),
  },
  {
    title: 'Transparent engine migration',
    body: (
      <>
        Weighted load balancing across a cluster group runs Trino and StarRocks together—ramp
        StarRocks from 10% to 100% with zero client changes. QueryFlux Studio query history lines up
        per-engine latency until you are ready to flip weights and skip a flag day.
      </>
    ),
  },
];

function UseCaseCard({title, body, index}: UseCase & {index: number}): ReactNode {
  return (
    <article className={styles.cardCol}>
      <div className={styles.card}>
        <div className={styles.cardAccent} aria-hidden />
        <div className={styles.cardBadge} aria-hidden="true">
          {String(index + 1).padStart(2, '0')}
        </div>
        <Heading as="h3" className={styles.cardTitle}>
          {title}
        </Heading>
        <p className={styles.cardBody}>{body}</p>
      </div>
    </article>
  );
}

export default function HomepageUseCases(): ReactNode {
  return (
    <section className={styles.section} aria-labelledby="homepage-use-cases-heading">
      <div className={clsx('container', styles.grid)}>
        <h2 id="homepage-use-cases-heading" className={styles.sectionHeading}>
          Example use cases
        </h2>
        <p className={styles.sectionSub}>
          Routing, capacity limits, and load balancing—one proxy, every client, no separate
          stack per engine.
        </p>
        <div className={styles.cardRow}>
          {useCases.map((item, idx) => (
            <UseCaseCard key={item.title} index={idx} {...item} />
          ))}
        </div>
      </div>
    </section>
  );
}
