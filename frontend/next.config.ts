import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  eslint: {
    // Warning: This allows production builds to successfully complete even if
    // your project has ESLint errors.
    ignoreDuringBuilds: true,
  },
  typescript: {
    // Warning: This allows production builds to successfully complete even if
    // your project has type errors.
    ignoreBuildErrors: true,
  },

  async rewrites() {
    const backend = process.env.NEXT_PUBLIC_BACKEND_DOMAIN;

    console.log("Using backend:", backend);

    return [
      {
        source: "/api/:path*",
        destination: `${backend}/:path*`, // IMPORTANT
      },
    ];
  },
};

export default nextConfig;