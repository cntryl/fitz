import { askr } from "@askrjs/vite";
import autoprefixer from "autoprefixer";
import { defineConfig } from "vite-plus";
import { fitzMockApiPlugin } from "./dev/mock-api.ts";

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
  fmt: {
    ignorePatterns: ["src/adapters/generated/**"],
  },
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
    transformer: "postcss" as const,
    postcss: {
      plugins: [autoprefixer()],
    },
  },
  base: "/",
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
};

export default defineConfig(config);
