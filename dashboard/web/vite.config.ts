import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In production the FastAPI backend serves both /api/* and the built SPA
// from /app/dashboard/web/dist. In dev (`npm run dev`), vite runs on 5173
// and proxies /api/* to a separately-running FastAPI on 8000.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: false,
        secure: false,
      },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
    chunkSizeWarningLimit: 800,
  },
});
