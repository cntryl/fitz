import { askr } from "@askrjs/vite";
import { defineConfig } from "vite-plus";
import { fileURLToPath, URL } from "node:url";

const srcDir = fileURLToPath(new URL("./src", import.meta.url));
const vendorDir = fileURLToPath(new URL("./vendor", import.meta.url));

export default defineConfig({
  fmt: {},
  lint: {
    ignorePatterns: ["dist/**", "coverage/**", "src/adapters/generated/**"],
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
  resolve: {
    alias: [
      {
        find: "@askrjs/askr-ui/badge",
        replacement: `${vendorDir}/askr-ui/badge.jsx`,
      },
      {
        find: "@askrjs/askr-ui/button",
        replacement: `${vendorDir}/askr-ui/button.jsx`,
      },
      {
        find: "@askrjs/askr-ui/container",
        replacement: `${vendorDir}/askr-ui/container.jsx`,
      },
      {
        find: "@askrjs/askr-ui/field",
        replacement: `${vendorDir}/askr-ui/field.jsx`,
      },
      {
        find: "@askrjs/askr-ui/input",
        replacement: `${vendorDir}/askr-ui/input.jsx`,
      },
      {
        find: "@askrjs/askr-ui/stack",
        replacement: `${vendorDir}/askr-ui/stack.jsx`,
      },
      {
        find: "@askrjs/askr-themes/default",
        replacement: `${vendorDir}/askr-themes/default.css`,
      },
      {
        find: "@askrjs/icons-lucide",
        replacement: `${vendorDir}/icons-lucide/index.jsx`,
      },
      {
        find: "@",
        replacement: srcDir,
      },
    ],
  },
  build: {
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
