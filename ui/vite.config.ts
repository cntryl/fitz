import { askr } from "@askrjs/vite";
import { defineConfig } from "vite-plus";

function fileUrlPath(path: string) {
  return decodeURIComponent(new URL(path, import.meta.url).pathname).replace(
    /^\/([A-Za-z]:\/)/,
    "$1",
  );
}

const srcDir = fileUrlPath("./src");

function chartsForCompat() {
  return {
    name: "fitz-askr-charts-for-compat",
    enforce: "pre" as const,
    transform(code: string, id: string) {
      if (
        id.includes("/@askrjs/charts/dist/components/") &&
        code.includes('from "@askrjs/askr"')
      ) {
        return code.replaceAll('from "@askrjs/askr"', 'from "@askrjs/askr/control"');
      }

      return null;
    },
  };
}

export default defineConfig({
  fmt: {},
  lint: {
    ignorePatterns: ["dist/**", "coverage/**", "src/adapters/generated/**"],
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  plugins: [chartsForCompat(), askr()],
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
      "/ws": {
        target: "ws://localhost:4090",
        ws: true,
      },
    },
  },
  resolve: {
    alias: [
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
