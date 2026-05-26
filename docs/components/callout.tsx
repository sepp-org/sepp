import type { ReactNode } from 'react';

type Kind = 'info' | 'warn' | 'note';

interface CalloutProps {
  type?: Kind;
  title?: string;
  children: ReactNode;
}

const ICON: Record<Kind, string> = {
  info: 'i',
  warn: '!',
  note: '·',
};

export function Callout({ type = 'note', title, children }: CalloutProps) {
  return (
    <div className={`sepp-callout sepp-callout--${type}`}>
      <span className="sepp-callout-icon" aria-hidden="true">
        {ICON[type]}
      </span>
      <div>
        {title ? (
          <p>
            <strong>{title}</strong>
          </p>
        ) : null}
        {children}
      </div>
    </div>
  );
}
