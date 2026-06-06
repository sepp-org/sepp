'use client';

import { useEffect, useId, useState } from 'react';
import { useTheme } from 'next-themes';

/** Pull the live sepp design tokens off :root so diagrams match the theme. */
function readTokens() {
  const s = getComputedStyle(document.documentElement);
  const v = (name: string, fallback: string) => {
    const got = s.getPropertyValue(name).trim();
    return got || fallback;
  };
  return {
    bg: v('--bg-content', '#1a1815'),
    surface: v('--surface', '#201d19'),
    surface2: v('--surface-2', '#2a2622'),
    ink: v('--ink', '#ece6da'),
    inkDim: v('--ink-dim', '#968d7d'),
    inkFaint: v('--ink-faint', '#5b5345'),
    border: v('--border-strong', 'rgba(236,230,218,0.16)'),
    accent: v('--accent', '#ec6a2e'),
    mono: v('--font-plex-mono', 'ui-monospace, SFMono-Regular, monospace'),
  };
}

/**
 * Renders a Mermaid diagram on the client, themed with the site's design
 * tokens. Diagrams are generated in the browser after hydration, so this works
 * with the static export used for GitHub Pages — no build-time browser needed.
 */
export function Mermaid({ chart }: { chart: string }) {
  const id = useId();
  const { resolvedTheme } = useTheme();
  const [svg, setSvg] = useState('');

  useEffect(() => {
    let active = true;

    (async () => {
      try {
        const t = readTokens();
        const { default: mermaid } = await import('mermaid');
        mermaid.initialize({
          startOnLoad: false,
          securityLevel: 'loose',
          theme: 'base',
          fontFamily: t.mono,
          flowchart: { curve: 'basis', padding: 12, nodeSpacing: 40, rankSpacing: 48 },
          themeVariables: {
            fontFamily: t.mono,
            fontSize: '13px',
            background: 'transparent',
            mainBkg: t.surface,
            primaryColor: t.surface,
            primaryBorderColor: t.border,
            primaryTextColor: t.ink,
            secondaryColor: t.surface2,
            tertiaryColor: t.surface2,
            nodeBorder: t.border,
            nodeTextColor: t.ink,
            lineColor: t.inkFaint,
            textColor: t.ink,
            clusterBkg: 'transparent',
            clusterBorder: t.border,
            titleColor: t.inkDim,
            edgeLabelBackground: t.bg,
          },
        });
        // Measure against the real font, not a fallback, or boxes come out
        // too small and clip the labels.
        if (document.fonts?.ready) await document.fonts.ready;
        const renderId = 'mermaid-' + id.replace(/[^a-zA-Z0-9-]/g, '');
        const { svg } = await mermaid.render(renderId, chart);
        if (active) setSvg(svg);
      } catch (err) {
        console.error('mermaid render failed', err);
      }
    })();

    return () => {
      active = false;
    };
  }, [chart, id, resolvedTheme]);

  return (
    <div
      className="my-7 flex justify-center overflow-x-auto [&_svg]:h-auto [&_svg]:max-w-full"
      role="img"
      dangerouslySetInnerHTML={{ __html: svg }}
    />
  );
}
