import { expect, test } from "@playwright/test";
import {
  applyTheme,
  expectRouteChrome,
  normalizeRoute,
  sprint16Routes,
  themeModes,
  viewportPresets,
} from "./shell/chrome";

test.describe("sprint 16 route matrix", () => {
  for (const route of sprint16Routes) {
    for (const viewport of viewportPresets) {
      for (const theme of themeModes) {
        test(`${route.path} [${viewport.key}] [${theme}]`, async ({ page }, testInfo) => {
          const knownAskrWarnings: string[] = [];
          page.on("console", (message) => {
            if (
              message.type() === "warning" &&
              /conflicting (?:shared )?query|missing key/i.test(message.text())
            ) {
              knownAskrWarnings.push(message.text());
            }
          });
          await page.setViewportSize({ width: viewport.width, height: viewport.height });
          await applyTheme(page, theme);
          await route.setup(page);
          await page.goto(route.path);

          await expectRouteChrome(page, route);

          if (theme === "dark") {
            await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
          }

          await page.screenshot({
            fullPage: true,
            path: testInfo.outputPath(`${normalizeRoute(route.path)}-${viewport.key}-${theme}.png`),
            animations: "disabled",
          });
          expect(knownAskrWarnings).toEqual([]);
        });
      }
    }
  }
});
