import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

function BenefitBlock({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}): ReactNode {
  return (
    <article className={styles.block}>
      <div className={styles.blockAccent} aria-hidden />
      <Heading as="h3" className={styles.blockTitle}>
        {title}
      </Heading>
      <div className={styles.blockContent}>{children}</div>
    </article>
  );
}

export default function HomepageBenefits(): ReactNode {
  return (
    <section className={styles.section} aria-labelledby="homepage-benefits-heading">
      <div className={clsx('container', styles.grid)}>
        <h2 id="homepage-benefits-heading" className={styles.sectionHeading}>
          Benefits
        </h2>
        <p className={styles.sectionSub}>
          Cost control, SLA protection, and operational consistency across engines.
        </p>
        <div className={styles.blocks}>
          <BenefitBlock title="Cut query costs by routing to the right engine">
            <p className={styles.prose}>
              Cloud engines charge in fundamentally different ways. Compute-priced backends
              (Trino, StarRocks) charge for cluster uptime or CPU-seconds. Scan-priced backends
              (Athena, BigQuery) charge for bytes read. Without a routing layer, every query goes
              to the same engine regardless of its shape — CPU-heavy joins land on Athena, cold
              selective filters land on StarRocks, and you pay the wrong model each time.
            </p>
            <p className={styles.prose}>
              In our own benchmarking, workload-aware routing — steering CPU-heavy work to
              compute-priced engines and selective cold-data queries to scan-priced ones — reduced
              total workload cost by up to{' '}
              <strong className={styles.proseStrong}>56%</strong>, with individual queries sometimes
              dropping by up to <strong className={styles.proseStrong}>90%</strong> compared with
              always using a single default.
            </p>
          </BenefitBlock>

          <BenefitBlock title="Enforce latency SLAs without touching clients">
            <p className={styles.prose}>
              A batch ETL job competing with an interactive dashboard on the same Trino cluster
              degrades both. QueryFlux lets you encode performance intent in routing rules and
              apply it to all clients uniformly — no application changes, no conventions that drift:
            </p>
            <div className={styles.routeLines}>
              <p className={styles.routeLine}>
                Route all PostgreSQL wire connections (typically interactive tooling) to a
                low-latency StarRocks pool
              </p>
              <p className={styles.routeLine}>
                Route queries tagged <code className={styles.inlineCode}>workload:etl</code> to
                the Trino cluster reserved for batch
              </p>
              <p className={styles.routeLine}>
                Route queries matching <code className={styles.inlineCode}>SELECT.*LIMIT \d+</code>{' '}
                to DuckDB for sub-10 ms response
              </p>
            </div>
          </BenefitBlock>

          <BenefitBlock title="Absorb burst pressure with proxy-side queuing">
            <p className={styles.prose}>
              When a cluster is saturated, the default behavior is engine-specific and invisible
              across engines. QueryFlux adds a controlled throttle per cluster group: queries queue
              at the proxy rather than hammering the backend, overflow spills to a secondary group
              via fallback routing, and queue depth is a first-class Prometheus metric. One pane of
              glass across all engines instead of fragmented per-engine UIs.
            </p>
          </BenefitBlock>

          <BenefitBlock title="Eliminate the N×M integration problem">
            <p className={styles.prose}>
              One endpoint replaces N×M driver configurations. Clients connect to QueryFlux once;
              the backend topology—which engines exist, how they are grouped, how load is
              balanced—is config, not code. Add an engine, change a routing rule, swap a backend:
              no client changes, no deploys, no coordination.
            </p>
          </BenefitBlock>

          <BenefitBlock title="~0.35 ms proxy overhead">
            <p className={styles.prose}>
              QueryFlux is written in Rust. The measured p50 proxy overhead (routing + dialect
              translation, from the <code className={styles.inlineCode}>queryflux-bench</code>{' '}
              suite) is approximately <strong className={styles.latencyHighlight}>0.35 ms</strong>.
              For the typical analytical workload, the proxy is not on the critical path.
            </p>
          </BenefitBlock>
        </div>
      </div>
    </section>
  );
}
