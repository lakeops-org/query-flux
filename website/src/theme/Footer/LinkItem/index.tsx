/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, {type ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import isInternalUrl from '@docusaurus/isInternalUrl';
import IconExternalLink from '@theme/Icon/ExternalLink';
import type {Props} from '@theme/Footer/LinkItem';

function isLakeOpsOrgQueryFluxOnGithub(href: string): boolean {
  try {
    const u = new URL(href);
    if (u.hostname !== 'github.com') {
      return false;
    }
    const base = '/lakeops-org/queryflux';
    const p = u.pathname.replace(/\/+$/, '') || '/';
    return p === base || p.startsWith(`${base}/`);
  } catch {
    return false;
  }
}

function GitHubMarkIcon({className}: {className?: string}): ReactNode {
  return (
    <svg
      className={className}
      width="16"
      height="16"
      viewBox="0 0 98 96"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden>
      <path
        fill="currentColor"
        fillRule="evenodd"
        clipRule="evenodd"
        d="M48.854 0C21.839 0 0 22 0 49.217c0 21.756 13.993 40.172 33.405 46.69 2.427.49 3.316-1.059 3.316-2.362 0-1.141-.08-5.052-.08-9.127-13.59 2.934-16.42-5.867-16.42-5.867-2.184-5.704-5.42-7.17-5.42-7.17-4.448-3.015.324-3.015.324-3.015 4.934.326 7.523 5.052 7.523 5.052 4.367 7.496 11.404 5.378 14.235 4.074.404-3.178 1.699-5.378 3.074-6.6-10.839-1.195-22.179-5.378-22.179-24.057 0-5.378 1.939-9.778 5.014-13.173-.503-1.196-2.184-6.02.478-12.518 0 0 4.075-1.302 13.406 4.994 4.002-1.079 8.29-1.619 12.548-1.619 4.259 0 8.546.54 12.548 1.619 9.318-6.296 13.393-4.994 13.393-4.994 2.662 6.498 1.003 11.322.478 12.518 3.08 3.395 5.014 7.795 5.014 13.173 0 18.795-11.354 22.848-22.194 24.043 1.741 1.508 3.302 4.407 3.302 8.927 0 6.434-.057 11.621-.057 13.173 0 1.304.869 2.852 3.316 2.367 19.394-6.518 33.382-24.934 33.382-46.69C97.708 22 75.788 0 48.854 0z"
      />
    </svg>
  );
}

export default function FooterLinkItem({item}: Props): ReactNode {
  const {to, href, label, prependBaseUrlToHref, className, ...props} = item;
  const toUrl = useBaseUrl(to);
  const normalizedHref = useBaseUrl(href, {forcePrependBaseUrl: true});

  const githubBranded = Boolean(href && isLakeOpsOrgQueryFluxOnGithub(href));
  const showDefaultExternalIcon = Boolean(
    href && !isInternalUrl(href) && !githubBranded,
  );

  return (
    <Link
      className={clsx('footer__link-item', className)}
      {...(href
        ? {
            href: prependBaseUrlToHref ? normalizedHref : href,
          }
        : {
            to: toUrl,
          })}
      {...props}>
      <span className="footer__link-inner">
        {githubBranded && <GitHubMarkIcon className="footer__link-icon" />}
        {label}
        {showDefaultExternalIcon && (
          <span className="footer__link-external-icon">
            <IconExternalLink width={12} height={12} />
          </span>
        )}
      </span>
    </Link>
  );
}
