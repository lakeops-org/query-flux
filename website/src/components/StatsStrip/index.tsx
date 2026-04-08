import type {ReactNode} from 'react';
import styles from './styles.module.css';

type Stat = {
  value: string;
  label: string;
  icon: ReactNode;
};

const stats: Stat[] = [
  {
    value: '4',
    label: 'front protocols',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        <path d="M10 13a5 5 0 007.54.54l3-3a5 5 0 00-7.07-7.07l-1.72 1.71"/>
        <path d="M14 11a5 5 0 00-7.54-.54l-3 3a5 5 0 007.07 7.07l1.71-1.71"/>
      </svg>
    ),
  },
  {
    value: '5+',
    label: 'backend engines',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        <rect x="2" y="3" width="20" height="4" rx="1"/>
        <rect x="2" y="10" width="20" height="4" rx="1"/>
        <rect x="2" y="17" width="20" height="4" rx="1"/>
      </svg>
    ),
  },
  {
    value: '~0.35 ms',
    label: 'p50 proxy overhead',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
      </svg>
    ),
  },
  {
    value: 'up to 56%',
    label: 'cost reduction',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        <polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/>
        <polyline points="17 6 23 6 23 12"/>
      </svg>
    ),
  },
];

export default function StatsStrip(): ReactNode {
  return (
    <div className={styles.strip} aria-label="QueryFlux key metrics">
      <div className="container">
        <dl className={styles.grid}>
          {stats.map((stat) => (
            <div key={stat.label} className={styles.item}>
              <span className={styles.icon} aria-hidden="true">{stat.icon}</span>
              <dt className={styles.value}>{stat.value}</dt>
              <dd className={styles.label}>{stat.label}</dd>
            </div>
          ))}
        </dl>
      </div>
    </div>
  );
}
