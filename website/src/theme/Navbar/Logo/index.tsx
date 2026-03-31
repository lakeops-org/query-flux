/**
 * Text-only wordmark — "Query" + orange "Flux" (presentation only; same title string).
 */

import React, {type ReactNode} from 'react';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import {useThemeConfig} from '@docusaurus/theme-common';

import styles from './styles.module.css';

const FLUX_SUFFIX = 'Flux';

function splitQueryFlux(title: string): {query: string; flux: string} | null {
  if (title.endsWith(FLUX_SUFFIX)) {
    return {query: title.slice(0, -FLUX_SUFFIX.length), flux: FLUX_SUFFIX};
  }
  return null;
}

export default function NavbarLogo(): ReactNode {
  const {
    navbar: {title},
  } = useThemeConfig();
  const homeHref = useBaseUrl('/');
  const parts = title != null ? splitQueryFlux(title) : null;

  return (
    <Link
      to={homeHref}
      className={styles.brand}
      aria-label={title ? `${title} home` : 'Home'}>
      {parts != null ? (
        <span className={styles.wordmark}>
          <span className={styles.query}>{parts.query}</span>
          <span className={styles.flux}>{parts.flux}</span>
        </span>
      ) : (
        title != null && <span className={styles.wordmarkFallback}>{title}</span>
      )}
    </Link>
  );
}
