'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';
import Link from 'next/link';

// With `output: 'export'`, the server-side `redirect()` from next/navigation
// can't run at request time. Render a tiny landing card and navigate on mount.
export default function HomePage() {
  const router = useRouter();

  useEffect(() => {
    router.replace('/docs');
  }, [router]);

  return (
    <main
      style={{
        minHeight: '100vh',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: 24,
      }}
    >
      <Link
        href="/docs"
        style={{ color: 'var(--accent)', fontFamily: 'var(--font-plex-mono)' }}
      >
        → docs
      </Link>
    </main>
  );
}
