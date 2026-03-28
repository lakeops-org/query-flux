import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import HomepageFeatures from '@site/src/components/HomepageFeatures';
import styles from './index.module.css';

function HomepageHeader(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const logoUrl = useBaseUrl('/img/queryflux-logo.png');

  return (
    <header className={styles.hero}>
      <div className={styles.heroGlow} aria-hidden />
      <div className={clsx('container', styles.heroInner)}>
        <img
          className={styles.heroLogo}
          src={logoUrl}
          alt="QueryFlux"
          width={560}
          height={200}
          decoding="async"
        />
        <Heading as="h1" className={styles.heroTitle}>
          {siteConfig.title}
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
            href="https://github.com/lakeops-org/query-flux">
            GitHub
          </Link>
        </div>
      </div>
    </header>
  );
}

const HOME_DESCRIPTION =
  'Universal SQL query proxy and router in Rust. One front door for Trino, PostgreSQL, MySQL, and Flight clients; route and translate SQL to Trino, DuckDB, StarRocks, and more.';

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout title={siteConfig.title} description={HOME_DESCRIPTION}>
      <HomepageHeader />
      <main className={styles.mainBelowFold}>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
