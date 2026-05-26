interface SeppMarkProps {
  size?: number;
}

// The three queued bars follow the body text color via `currentColor`, so the
// mark flips dark/light with the theme automatically. We set `color` on the
// <svg> itself rather than rely on parent inheritance — Fumadocs' nav <a>
// inserts its own foreground color higher up the chain.
export function SeppMark({ size = 22 }: SeppMarkProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 100 100"
      style={{ display: 'block', color: 'var(--ink)' }}
      aria-hidden="true"
    >
      <rect x="22" y="22" width="28" height="10" rx="2" fill="currentColor" />
      <rect x="22" y="38" width="36" height="10" rx="2" fill="currentColor" />
      <rect x="22" y="54" width="44" height="10" rx="2" fill="currentColor" />
      <rect x="22" y="70" width="60" height="10" rx="2" fill="#ec6a2e" />
    </svg>
  );
}
