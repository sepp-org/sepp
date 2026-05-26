import Link from 'next/link';

interface PageFootProps {
  prev?: { href: string; title: string };
  next?: { href: string; title: string };
}

export function PageFoot({ prev, next }: PageFootProps) {
  return (
    <div className="sepp-page-foot">
      {prev ? (
        <Link href={prev.href} className="sepp-page-foot-card">
          <div className="sepp-page-foot-eyebrow">← Previous</div>
          <div className="title">{prev.title}</div>
        </Link>
      ) : (
        <span />
      )}
      {next ? (
        <Link href={next.href} className="sepp-page-foot-card next">
          <div className="sepp-page-foot-eyebrow">Next →</div>
          <div className="title">{next.title}</div>
        </Link>
      ) : (
        <span />
      )}
    </div>
  );
}
