import { askr } from "@askrjs/vite";
import autoprefixer from "autoprefixer";
import { defineConfig } from "vite-plus";
import type { UserConfig } from "@voidzero-dev/vite-plus-core";
import { fitzMockApiPlugin } from "./dev/mock-api";

function fileUrlPath(path: string) {
  return decodeURIComponent(new URL(path, import.meta.url).pathname).replace(
    /^\/([A-Za-z]:\/)/,
    "$1",
  );
}

const srcDir = fileUrlPath("./src");
const useMockApi =
  process.env.VITE_FITZ_MOCK_API === "1" || process.env.VITE_FITZ_MOCK_API === "true";

const config = {
  fmt: {},
  lint: {
    ignorePatterns: ["dist/**", "coverage/**", "src/adapters/generated/**"],
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  plugins: [
    ...(useMockApi ? [fitzMockApiPlugin()] : []),
    askr({
      optimizeTemplates: true,
    }),
  ],
  css: {
    transformer: "postcss",
    postcss: {
      plugins: [autoprefixer()],
    },
  },
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
      "/ws": {
        target: "ws://localhost:4090",
        ws: true,
      },
    },
  },
  resolve: {
    dedupe: ["@askrjs/askr", "@askrjs/ui"],
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
    exclude: ["**/dist/**", "**/node_modules/**", "tests/e2e/**"],
    globals: true,
    coverage: {
      reporter: ["text", "json", "html"],
    },
  },
} as UserConfig;

export default defineConfig(config);
