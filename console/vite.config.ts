import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Dev: the console runs on Vite's port and proxies API calls to a local
// coordinator (krishiv daemon, HTTP on 7072). Prod: the build output is
// embedded into the krishiv binary via krishiv-ui (same origin, no proxy).
// Override with KRISHIV_COORDINATOR_URL when 7072 is taken.
const coordinator = process.env.KRISHIV_COORDINATOR_URL ?? "http://127.0.0.1:7072";

export default defineConfig({
  // Served by the coordinator at /console/, so assets resolve under it.
  base: "/console/",
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      "/api": coordinator,
      "/metrics": coordinator,
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
  },
});
