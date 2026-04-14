import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import styles from './community.module.css';

/** Slack workspace invite (Queryflux / query-flux). */
const SLACK_WORKSPACE_URL =
  'https://join.slack.com/t/query-flux/shared_invite/zt-3v7qedxj9-o8ElCLGK0UXT8xBU0_bD8w';

const COMMUNITY_DESCRIPTION =
  'Connect with QueryFlux users and contributors: documentation, GitHub, and the Queryflux Slack workspace.';

const SlackGlyph = (): ReactNode => (
  <svg className={styles.slackIcon} viewBox="0 0 24 24" aria-hidden="true">
    <path
      fill="currentColor"
      d="M5.042 15.165a2.528 2.528 0 0 1-2.52 2.523A2.528 2.528 0 0 1 0 15.165a2.527 2.527 0 0 1 2.522-2.52h2.52v2.52zM6.313 15.165a2.527 2.527 0 0 1 2.521-2.52 2.527 2.527 0 0 1 2.521 2.52v6.313A2.528 2.528 0 0 1 8.834 24a2.528 2.528 0 0 1-2.521-2.522v-6.313zM8.834 5.042a2.528 2.528 0 0 1-2.521-2.52A2.528 2.528 0 0 1 8.834 0a2.528 2.528 0 0 1 2.521 2.522v2.52H8.834V5.042zm0 1.313a2.528 2.528 0 0 1 2.521 2.521 2.528 2.528 0 0 1-2.521 2.521H2.522A2.528 2.528 0 0 1 0 8.876a2.528 2.528 0 0 1 2.522-2.521h6.312zm10.123 2.521a2.528 2.528 0 0 1 2.522-2.521A2.528 2.528 0 0 1 24 8.876a2.528 2.528 0 0 1-2.522 2.521h-2.522V8.876zm-1.313 0a2.528 2.528 0 0 1-2.521 2.521 2.527 2.527 0 0 1-2.521-2.521V2.522A2.528 2.528 0 0 1 15.165 0a2.528 2.528 0 0 1 2.521 2.522v6.354zm-2.521 10.123a2.528 2.528 0 0 1 2.521 2.522A2.528 2.528 0 0 1 15.165 24a2.527 2.527 0 0 1-2.521-2.522v-2.522h2.521zm0-1.313a2.527 2.527 0 0 1-2.521-2.521 2.527 2.527 0 0 1 2.521-2.521h6.313A2.528 2.528 0 0 1 24 15.165a2.528 2.528 0 0 1-2.522 2.521h-6.313z"
    />
  </svg>
);

export default function Community(): ReactNode {
  return (
    <Layout title="Community" description={COMMUNITY_DESCRIPTION}>
      <div className={styles.page}>
        <header className={styles.hero}>
          <div className={styles.heroGlow} aria-hidden />
          <div className={clsx('container', styles.heroInner)}>
            <Heading as="h1" className={styles.title}>
              Community
            </Heading>
            <p className={styles.lead}>
              Open and inclusive — help shape QueryFlux. Use the docs for deep dives, GitHub for
              code and issues, and Slack for quick questions and real-time chat with other users.
            </p>
            <div className={styles.ctaRow}>
              <Link
                className={clsx('button button--lg', styles.slackButton)}
                href={SLACK_WORKSPACE_URL}
                target="_blank"
                rel="noopener noreferrer">
                <SlackGlyph />
                Join Slack (Queryflux)
              </Link>
              <Link className="button button--lg button--primary" to="/docs/intro">
                Documentation
              </Link>
              <Link
                className="button button--lg button--outline"
                href="https://github.com/lakeops-org/queryflux"
                target="_blank"
                rel="noopener noreferrer">
                GitHub
              </Link>
            </div>
          </div>
        </header>

        <main className={styles.main}>
          <div className="container">
            <h2 className={styles.sectionTitle}>How to get involved</h2>
            <p className={styles.sectionSub}>
              Pick the channel that fits your question — searchable reference here, fast feedback
              in Slack, and contributions always welcome on GitHub.
            </p>

            <div className={styles.cardGrid}>
              <article className={styles.card}>
                <div className={styles.cardAccent} aria-hidden />
                <div className={styles.cardIcon} aria-hidden="true">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75">
                    <path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20" />
                    <path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z" />
                    <line x1="8" y1="7" x2="16" y2="7" />
                    <line x1="8" y1="11" x2="14" y2="11" />
                  </svg>
                </div>
                <Heading as="h3" className={styles.cardTitle}>
                  Documentation
                </Heading>
                <p className={styles.cardText}>
                  Tutorials, configuration reference, and architecture — ideal for technical depth
                  and answers you can bookmark.
                </p>
                <ul className={styles.bullets}>
                  <li>Searchable guides and YAML reference</li>
                  <li>Best for setup, routing rules, and operations</li>
                </ul>
                <div className={styles.cardActions}>
                  <Link className="button button--primary" to="/docs/intro">
                    Open docs
                  </Link>
                </div>
              </article>

              <article className={styles.card}>
                <div className={styles.cardAccent} aria-hidden />
                <div className={styles.cardIcon} aria-hidden="true">
                  <svg viewBox="0 0 24 24" fill="currentColor">
                    <path d="M5.042 15.165a2.528 2.528 0 0 1-2.52 2.523A2.528 2.528 0 0 1 0 15.165a2.527 2.527 0 0 1 2.522-2.52h2.52v2.52zM6.313 15.165a2.527 2.527 0 0 1 2.521-2.52 2.527 2.527 0 0 1 2.521 2.52v6.313A2.528 2.528 0 0 1 8.834 24a2.528 2.528 0 0 1-2.521-2.522v-6.313zM8.834 5.042a2.528 2.528 0 0 1-2.521-2.52A2.528 2.528 0 0 1 8.834 0a2.528 2.528 0 0 1 2.521 2.522v2.52H8.834V5.042zm0 1.313a2.528 2.528 0 0 1 2.521 2.521 2.528 2.528 0 0 1-2.521 2.521H2.522A2.528 2.528 0 0 1 0 8.876a2.528 2.528 0 0 1 2.522-2.521h6.312zm10.123 2.521a2.528 2.528 0 0 1 2.522-2.521A2.528 2.528 0 0 1 24 8.876a2.528 2.528 0 0 1-2.522 2.521h-2.522V8.876zm-1.313 0a2.528 2.528 0 0 1-2.521 2.521 2.527 2.527 0 0 1-2.521-2.521V2.522A2.528 2.528 0 0 1 15.165 0a2.528 2.528 0 0 1 2.521 2.522v6.354zm-2.521 10.123a2.528 2.528 0 0 1 2.521 2.522A2.528 2.528 0 0 1 15.165 24a2.527 2.527 0 0 1-2.521-2.522v-2.522h2.521zm0-1.313a2.527 2.527 0 0 1-2.521-2.521 2.527 2.527 0 0 1 2.521-2.521h6.313A2.528 2.528 0 0 1 24 15.165a2.528 2.528 0 0 1-2.522 2.521h-6.313z" />
                  </svg>
                </div>
                <Heading as="h3" className={styles.cardTitle}>
                  Slack — Queryflux workspace
                </Heading>
                <p className={styles.cardText}>
                  Chat with peers, ask quick questions, and share what you are building. Use the invite
                  link to join the Queryflux workspace if you are not a member yet.
                </p>
                <ul className={styles.bullets}>
                  <li>Real-time help and shorter threads</li>
                  <li>Workspace URL: query-flux.slack.com</li>
                </ul>
                <div className={styles.cardActions}>
                  <Link
                    className={clsx('button', styles.slackButton)}
                    href={SLACK_WORKSPACE_URL}
                    target="_blank"
                    rel="noopener noreferrer">
                    <SlackGlyph />
                    Open Slack
                  </Link>
                </div>
              </article>
            </div>

            <article className={clsx(styles.card, styles.wideCard)}>
              <div className={styles.cardAccent} aria-hidden />
              <div className={styles.cardIcon} aria-hidden="true">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75">
                  <path d="M9 19c-5 1.5-5-2.5-7-3m14 6v-3.87a3.37 3.37 0 0 0-.94-2.61c3.14-.35 6.44-1.54 6.44-7A5.44 5.44 0 0 0 20 4.77 5.07 5.07 0 0 0 19.91 1S18.73.65 16 2.48a13.38 13.38 0 0 0-7 0C6.27.65 5.09 1 5.09 1A5.07 5.07 0 0 0 5 4.77a5.44 5.44 0 0 0-1.5 3.78c0 5.42 3.3 6.61 6.44 7A3.37 3.37 0 0 0 9 18.13V22" />
                </svg>
              </div>
              <Heading as="h3" className={styles.cardTitle}>
                Contribute
              </Heading>
              <p className={styles.cardText}>
                We welcome issues, design discussion, and pull requests. Star the repo, read the
                contribute guide, and open an issue when you are unsure where to start.
              </p>
              <div className={styles.cardActions}>
                <Link
                  className="button button--primary"
                  href="https://github.com/lakeops-org/queryflux"
                  target="_blank"
                  rel="noopener noreferrer">
                  GitHub repository
                </Link>
                <Link className="button button--secondary" to="/docs/contribute">
                  Contribute guide
                </Link>
                <Link
                  className="button button--outline"
                  href="https://github.com/lakeops-org/queryflux/issues"
                  target="_blank"
                  rel="noopener noreferrer">
                  Issues
                </Link>
              </div>
            </article>
          </div>
        </main>
      </div>
    </Layout>
  );
}
