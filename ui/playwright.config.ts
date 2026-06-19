import { defineConfig, devices } from "@playwright/test";

const configuredBaseURL = process.env.PLAYWRIGHT_BASE_URL ?? "http://127.0.0.1:5173";
const shouldStartDevServer = !process.env.PLAYWRIGHT_BASE_URL;
const isCI = Boolean(process.env.CI);
const shouldOpenBrowser = process.env.PLAYWRIGHT_OPEN_BROWSER === "true";
const baseURL = configuredBaseURL;

export default defineConfig({
  testDir: "./tests/e2e",
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
  ...(shouldStartDevServer
    ? {
        webServer: {
          command: shouldOpenBrowser
            ? "npm run dev -- --host 127.0.0.1"
            : "npm run dev -- --host 127.0.0.1 --open false",
          reuseExistingServer: !isCI,
          timeout: 120_000,
          url: baseURL,
        },
      }
    : {}),
});
