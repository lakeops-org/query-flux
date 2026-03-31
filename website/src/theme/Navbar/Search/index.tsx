import React, {type ReactNode} from 'react';
import type {Props} from '@theme/Navbar/Search';

export default function NavbarSearch({children, className}: Props): ReactNode {
  return <div className={className}>{children}</div>;
}
