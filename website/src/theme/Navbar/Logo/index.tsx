/**
 * Text-only wordmark — no image beside the product name.
 */

import React, {type ReactNode} from 'react';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import {useThemeConfig} from '@docusaurus/theme-common';

import styles from './styles.module.css';

export default function NavbarLogo(): ReactNode {
  const {
    navbar: {title},
  } = useThemeConfig();
  const homeHref = useBaseUrl('/');

  return (
    <Link
      to={homeHref}
      className={styles.brand}
      aria-label={title ? `${title} home` : 'Home'}>
      {title != null && <span className={styles.wordmark}>{title}</span>}
    </Link>
  );
}
