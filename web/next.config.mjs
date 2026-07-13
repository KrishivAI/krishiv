import { createMDX } from 'fumadocs-mdx/next';
const withMDX = createMDX();
/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'export',
  trailingSlash: true,
  reactStrictMode: true,
  poweredByHeader: false,
  images: {
    unoptimized: true,
  },
};
export default withMDX(nextConfig);
