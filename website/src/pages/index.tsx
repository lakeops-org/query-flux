import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import HomepageFeatures from '@site/src/components/HomepageFeatures';
import HomepageUseCases from '@site/src/components/HomepageUseCases';
import HomepageBenefits from '@site/src/components/HomepageBenefits';
import HomepageNextSteps from '@site/src/components/HomepageNextSteps';
import styles from './index.module.css';

function HomepageHeader(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const heroUrl = useBaseUrl('/img/queryflux-hero-cover.png');

  return (
    <header className={styles.hero}>
      <div className={styles.heroGlow} aria-hidden />
      <div className={clsx('container', styles.heroInner)}>
        <img
          className={styles.heroImage}
          src={heroUrl}
          alt="QueryFlux — multi-engine SQL routing in Rust, connecting clients to Trino, DuckDB, StarRocks, Snowflake, Databricks, and more"
          width={1024}
          height={682}
          decoding="async"
        />
        <Heading as="h1" className={styles.heroTitle}>
          <span className={styles.heroTitleQuery}>Query</span>
          <span className={styles.heroTitleFlux}>Flux</span>
        </Heading>
        <p className={styles.heroTagline}>{siteConfig.tagline}</p>
        <p className={styles.heroLeadin}>
          One front door for SQL clients — Trino, PostgreSQL, MySQL, and Flight on
          the wire; Trino, DuckDB, StarRocks, and more behind it. Route by rules,
          translate dialects with sqlglot, and run with metrics and queueing.
        </p>
        <div className={styles.ctaRow}>
          <Link className={clsx('button button--lg', styles.ctaPrimary)} to="/docs/intro">
            Documentation
          </Link>
          <Link
            className={clsx('button button--lg button--outline', styles.ctaGhost)}
            href="https://github.com/lakeops-org/queryflux">
            GitHub
          </Link>
        </div>
      </div>
    </header>
  );
}

/** Homepage meta description; keep concise for search-result snippets (~155 chars / ~1000px). */
const HOME_DESCRIPTION =
  'Multi-engine query routing proxy for SQL clients: route Trino, PostgreSQL, MySQL, and Flight queries to the best backend engine with translation, queueing, and observability.';

/**
 * Page title before Docusaurus appends ` | QueryFlux` (~12 chars). Keep the *final*
 * string ≤ ~60 chars for Twitter / strict OG validators.
 */
const HOME_PAGE_TITLE = 'Multi-engine SQL query routing proxy';

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout title={HOME_PAGE_TITLE} description={HOME_DESCRIPTION}>
      <HomepageHeader />
      <main className={styles.mainBelowFold}>
        <HomepageFeatures />
        <HomepageUseCases />
        <HomepageBenefits />
        <HomepageNextSteps />
      </main>
    </Layout>
  );
}
