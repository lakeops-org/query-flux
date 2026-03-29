/**
 * Swizzled: hamburger → brand → search → doc links; version + GitHub + theme on the right.
 */
import React, {type ReactNode} from 'react';
import clsx from 'clsx';
import {
  useThemeConfig,
  ErrorCauseBoundary,
  ThemeClassNames,
} from '@docusaurus/theme-common';
import {splitNavbarItems, useNavbarMobileSidebar} from '@docusaurus/theme-common/internal';
import NavbarItem from '@theme/NavbarItem';
import NavbarColorModeToggle from '@theme/Navbar/ColorModeToggle';
import SearchBar from '@theme/SearchBar';
import NavbarMobileSidebarToggle from '@theme/Navbar/MobileSidebar/Toggle';
import NavbarLogo from '@theme/Navbar/Logo';
import NavbarSearch from '@theme/Navbar/Search';
import type {Props as NavbarItemConfig} from '@theme/NavbarItem';

import styles from './styles.module.css';

function useNavbarItems(): NavbarItemConfig[] {
  return useThemeConfig().navbar.items as NavbarItemConfig[];
}

function NavbarItems({items}: {items: NavbarItemConfig[]}): ReactNode {
  return (
    <>
      {items.map((item, i) => (
        <ErrorCauseBoundary
          key={i}
          onError={(error) =>
            new Error(
              `A theme navbar item failed to render.
Please double-check the following navbar item (themeConfig.navbar.items) of your Docusaurus config:
${JSON.stringify(item, null, 2)}`,
              {cause: error},
            )
          }>
          <NavbarItem {...item} />
        </ErrorCauseBoundary>
      ))}
    </>
  );
}

function NavbarContentLayout({left, right}: {left: ReactNode; right: ReactNode}): ReactNode {
  return (
    <div className="navbar__inner">
      <div
        className={clsx(
          ThemeClassNames.layout.navbar.containerLeft,
          'navbar__items',
          styles.navbarLeft,
        )}>
        {left}
      </div>
      <div
        className={clsx(
          ThemeClassNames.layout.navbar.containerRight,
          'navbar__items navbar__items--right',
        )}>
        {right}
      </div>
    </div>
  );
}

export default function NavbarContent(): ReactNode {
  const mobileSidebar = useNavbarMobileSidebar();
  const items = useNavbarItems();
  const searchBarItem = items.find((item) => item.type === 'search');
  const itemsWithoutSearch = items.filter((item) => item.type !== 'search');
  const [leftItems, rightItems] = splitNavbarItems(itemsWithoutSearch);

  return (
    <NavbarContentLayout
      left={
        <>
          {!mobileSidebar.disabled && <NavbarMobileSidebarToggle />}
          <NavbarLogo />
          {searchBarItem ? (
            <ErrorCauseBoundary
              onError={(error) =>
                new Error(
                  `A theme navbar item failed to render.
Please double-check the following navbar item (themeConfig.navbar.items) of your Docusaurus config:
${JSON.stringify(searchBarItem, null, 2)}`,
                  {cause: error},
                )
              }>
              <NavbarItem {...searchBarItem} />
            </ErrorCauseBoundary>
          ) : (
            <NavbarSearch className={styles.searchBesideBrand}>
              <SearchBar />
            </NavbarSearch>
          )}
          <NavbarItems items={leftItems} />
        </>
      }
      right={
        <>
          <NavbarItems items={rightItems} />
          <NavbarColorModeToggle className={styles.colorModeToggle} />
        </>
      }
    />
  );
}
