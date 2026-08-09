import { defineConfig, devices } from "@playwright/test";

const shouldPreviewProduction = process.env.PLAYWRIGHT_PREVIEW === "1";
const configuredBaseURL =
  process.env.PLAYWRIGHT_BASE_URL ??
  (shouldPreviewProduction ? "http://127.0.0.1:4173" : "http://127.0.0.1:5173");
const shouldStartWebServer = !process.env.PLAYWRIGHT_BASE_URL;
const isCI = Boolean(process.env.CI);
const shouldOpenBrowser = process.env.PLAYWRIGHT_OPEN_BROWSER === "true";
const baseURL = configuredBaseURL;

export default defineConfig({
  testDir: "./tests/e2e",
  ...(shouldPreviewProduction
    ? { testMatch: /production-preview\.spec\.ts/ }
    : { testIgnore: /production-preview\.spec\.ts/ }),
  fullyParallel: true,
  forbidOnly: isCI,
  retries: isCI ? 2 : 0,
  workers: isCI ? 1 : undefined,
  reporter: isCI ? [["dot"], ["html", { open: "never" }]] : "list",
  use: {
    baseURL,
    screenshot: "only-on-failure",
    trace: "on-first-retry",
    video: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  ...(shouldStartWebServer
    ? {
        webServer: {
          command: shouldPreviewProduction
            ? "npm run preview -- --host 127.0.0.1 --port 4173 --strictPort"
            : shouldOpenBrowser
              ? "npm run dev -- --host 127.0.0.1"
              : "npm run dev -- --host 127.0.0.1 --open false",
          reuseExistingServer: !isCI,
          timeout: 120_000,
          url: baseURL,
        },
      }
    : {}),
});
