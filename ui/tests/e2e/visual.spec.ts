import { expect, test, type Page } from "@playwright/test";
import { canonicalVisualScenarios, expectRouteChrome, normalizeRoute } from "./shell/chrome";

const frozenTime = new Date("2026-06-23T18:30:00.000Z").valueOf();

async function installDeterministicBrowser(page: Page) {
  await page.addInitScript((now) => {
    const NativeDate = Date;
    class FrozenDate extends NativeDate {
      constructor(value?: string | number | Date) {
        super(value === undefined ? now : value instanceof NativeDate ? value.valueOf() : value);
      }

      static now() {
        return now;
      }
    }

    Object.defineProperty(window, "Date", { configurable: true, value: FrozenDate });
    localStorage.removeItem("fitz-admin-theme");
  }, frozenTime);

  await page.addInitScript(() => {
    document.addEventListener("DOMContentLoaded", () => {
      const style = document.createElement("style");
      style.textContent = `
        *, *::before, *::after {
          animation-delay: 0s !important;
          animation-duration: 0s !important;
          caret-color: transparent !important;
          scroll-behavior: auto !important;
          transition-delay: 0s !important;
          transition-duration: 0s !important;
        }
      `;
      document.head.append(style);
    });
  });
}

test.describe("canonical page-family visuals", () => {
  for (const route of canonicalVisualScenarios) {
    test(`@visual ${route.path} desktop light`, async ({ page }) => {
      const runtimeFailures: string[] = [];

      page.on("console", (message) => {
        if (message.type() === "warning" || message.type() === "error") {
          runtimeFailures.push(`console ${message.type()}: ${message.text()}`);
        }
      });
      page.on("pageerror", (error) => runtimeFailures.push(`page error: ${error.message}`));
      page.on("requestfailed", (request) => {
        runtimeFailures.push(
          `request failed: ${request.method()} ${request.url()} ${request.failure()?.errorText ?? ""}`,
        );
      });
      page.on("response", (response) => {
        if (response.status() >= 400) {
          runtimeFailures.push(`response ${response.status()}: ${response.url()}`);
        }
      });

      await page.setViewportSize({ width: 1440, height: 1200 });
      await installDeterministicBrowser(page);
      await route.setup(page);
      await page.goto(route.path);
      await expectRouteChrome(page, route);
      await expect(page).toHaveScreenshot(`${normalizeRoute(route.path)}.png`, {
        animations: "disabled",
        fullPage: true,
        maxDiffPixels: 100,
      });
      expect(runtimeFailures).toEqual([]);
    });
  }
});
