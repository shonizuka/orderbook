/** @type {import('next').NextConfig} */
const nextConfig = {
  // Required for the Docker multi-stage standalone build
  output: "standalone",

  // Proxy API calls to the backend (avoids CORS in dev and in prod via nginx)
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `${process.env.BACKEND_URL ?? "http://localhost:8000"}/api/:path*`,
      },
    ];
  },
};

module.exports = nextConfig;
