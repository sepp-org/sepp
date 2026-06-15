import type { ReactNode } from 'react';
import type { Metadata } from 'next';
import { IBM_Plex_Sans, IBM_Plex_Mono } from 'next/font/google';
import { RootProvider } from 'fumadocs-ui/provider/next';
import './global.css';

const plexSans = IBM_Plex_Sans({
  subsets: ['latin'],
  weight: ['400', '500', '600'],
  variable: '--font-plex-sans',
  display: 'swap',
});

const plexMono = IBM_Plex_Mono({
  subsets: ['latin'],
  weight: ['400', '500', '600'],
  variable: '--font-plex-mono',
  display: 'swap',
});

// Next does not auto-prefix `metadata.icons.url` with basePath the way it does
// `<Link>` and `<Image>`. Resolve manually so the favicon works on Pages.
const basePath = process.env.NEXT_PUBLIC_BASE_PATH ?? '';
const asset = (path: string) => `${basePath}${path}`;

export const metadata: Metadata = {
  title: {
    default: 'sepp',
    template: '%s · Sepp docs',
  },
  description:
    'Sepp is a language-agnostic job queue server written in Rust.',
  icons: {
        icon: [
      {
        rel: 'icon',
        url: asset('/sepp-avatar-dark.svg'),
        media: '(prefers-color-scheme: light)',
      },
      {
        rel: 'icon',
        url: asset('/sepp-avatar-light.svg'),
        media: '(prefers-color-scheme: dark)',
      },
    ],
  },
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html
      lang="en"
      suppressHydrationWarning
      className={`${plexSans.variable} ${plexMono.variable} dark`}
    >
      <body className="flex min-h-screen flex-col">
        <RootProvider
          theme={{
            attribute: 'class',
            defaultTheme: 'dark',
            enableSystem: true,
          }}
          search={{
            options: {
              type: 'static',
              // `trailingSlash: true` makes the dev server redirect
              // `/api/search` → `/api/search/` with a 308. Some browsers'
              // fetch implementations drop the redirected response. In static
              // export with basePath, the route exports as a single file at
              // `<basePath>/api/search` (no directory). Point at the form that
              // exists in each environment.
              api: process.env.NEXT_PUBLIC_BASE_PATH
                ? `${process.env.NEXT_PUBLIC_BASE_PATH}/api/search`
                : '/api/search/',
            },
          }}
        >
          {children}
        </RootProvider>
      </body>
    </html>
  );
}
