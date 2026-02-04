import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",

  async rewrites() {
    const backend =
      process.env.BACKEND_DOMAIN || "http://localhost:8001";

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