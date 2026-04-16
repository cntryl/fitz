import { askr } from "@askrjs/askr-vite";
import { defineConfig } from "vite-plus";

export default defineConfig({
  fmt: {},
  lint: {
    ignorePatterns: ["dist/**", "coverage/**", "src/adapters/generated.ts"],
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  plugins: [askr()],
  base: "/",
  define: {
    "process.env": {},
  },
  server: {
    port: 5173,
    open: true,
    proxy: {
      // Proxy API calls to Fitz backend
      "/api": {
        target: "http://localhost:4090",
        changeOrigin: true,
      },
      "/metrics": {
        target: "http://localhost:4090",
        changeOrigin: true,
      },
      "/healthz": {
        target: "http://localhost:4090",
        changeOrigin: true,
      },
      "/readyz": {
        target: "http://localhost:4090",
        changeOrigin: true,
      },
      "/startupz": {
        target: "http://localhost:4090",
        changeOrigin: true,
      },
      "/ws": {
        target: "ws://localhost:4090",
        ws: true,
      },
    },
  },
  build: {
    outDir: "../public",
    emptyOutDir: false,
    sourcemap: true,
  },
  test: {
    environment: "jsdom",
    globals: true,
    coverage: {
      reporter: ["text", "json", "html"],
    },
  },
});
