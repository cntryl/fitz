import { expect, test } from "@playwright/test";
import { openDashboard } from "./shell/chrome";

function iconColors(icon: HTMLElement) {
  return {
    icon: getComputedStyle(icon).color,
    parent: getComputedStyle(icon.parentElement!).color,
  };
}

test("serves built assets with visible theme-aware route-family identities", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await openDashboard(page);

  await expect(page.locator('script[src*="/@vite/client"]')).toHaveCount(0);
  await expect(page.locator('script[type="module"][src^="/assets/"]')).toHaveCount(1);

  const icon = page
    .getByRole("button", { name: "Route Family selector" })
    .locator(".route-family-icon");
  await expect(icon).toHaveAttribute("data-route-family-identity", "identity-4");
  const light = await icon.evaluate(iconColors);
  expect(light.icon).not.toBe(light.parent);

  await page.getByRole("button", { name: "Toggle color theme" }).click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  const dark = await icon.evaluate(iconColors);
  expect(dark.icon).not.toBe(dark.parent);
  expect(dark.icon).not.toBe(light.icon);
});
