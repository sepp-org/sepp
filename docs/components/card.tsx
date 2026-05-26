import type { ReactNode } from 'react';
import Link from 'next/link';

interface CardsProps {
  children: ReactNode;
}

export function Cards({ children }: CardsProps) {
  return <div className="sepp-card-grid">{children}</div>;
}

interface CardProps {
  href: string;
  title: string;
  description?: string;
  icon?: ReactNode;
}

export function Card({ href, title, description, icon }: CardProps) {
  return (
    <Link href={href} className="sepp-card">
      {icon ? <span className="sepp-card-icon">{icon}</span> : null}
      <div className="sepp-card-title">{title}</div>
      {description ? <div className="sepp-card-desc">{description}</div> : null}
    </Link>
  );
}
