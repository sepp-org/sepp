import { createMDX } from 'fumadocs-mdx/next';

const withMDX = createMDX();

// `/sepp` when the site is served from github.com/sepp-org/sepp at
// https://sepp-org.github.io/sepp; empty for custom domain or local dev.
const basePath = process.env.NEXT_PUBLIC_BASE_PATH ?? '';

/** @type {import('next').NextConfig} */
const config = {
  reactStrictMode: true,
  output: 'export',
  trailingSlash: true,
  images: { unoptimized: true },
  basePath,
  assetPrefix: basePath || undefined,
  env: {
    NEXT_PUBLIC_BASE_PATH: basePath,
  },
};

export default withMDX(config);
