import { expect, test } from "@playwright/test";
import { mockAdminFeatures, mockDomainOverviewApis, topologyApiPayload } from "./shell/api-fixtures";
import { openDashboard } from "./shell/chrome";

test("captures the desktop dashboard shell", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await openDashboard(page);

  await expect(page.getByRole("heading", { name: "Fitz status" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Issues" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Domain health" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("dashboard-desktop.png"),
    animations: "disabled",
  });
});

test("captures the tablet dashboard shell", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1024, height: 1200 });
  await openDashboard(page);

  await expect(page.getByRole("heading", { name: "Fitz status" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Issues" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Domain health" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("dashboard-tablet.png"),
    animations: "disabled",
  });
});

test("captures the desktop dashboard shell in dark mode", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await openDashboard(page, "dark");

  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(page.getByRole("heading", { name: "Fitz status" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("dashboard-dark.png"),
    animations: "disabled",
  });
});

test("captures the dashboard refreshing state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });

  let releaseRefresh: (() => void) | undefined;
  let topologyRequests = 0;

  await mockAdminFeatures(page);
  await page.route("**/api/v1/topology", async (route) => {
    topologyRequests += 1;

    if (topologyRequests > 1) {
      await new Promise<void>((resolve) => {
        releaseRefresh = resolve;
      });
    }

    await route.fulfill({
      json: topologyApiPayload,
    });
  });

  await openDashboard(page, "light", false);
  await expect(page.getByRole("heading", { name: "Issues" })).toBeVisible();

  await page.getByRole("button", { name: "Refresh overview" }).click();
  await expect(page.locator('[data-slot="badge"]').filter({ hasText: "Refreshing" })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("dashboard-refreshing.png"),
    animations: "disabled",
  });

  releaseRefresh?.();
  await page.waitForTimeout(100);
});

test("captures desktop domain navigation", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await openDashboard(page);
  const primaryNav = page.getByRole("navigation", { name: "Primary navigation" });

  await expect(primaryNav.getByRole("link", { name: "Stream" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "KV" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Schedule" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Queue" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Lease" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Notice" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "RPC" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("domains-navigation-desktop.png"),
    animations: "disabled",
  });

  await mockDomainOverviewApis(page);
  await primaryNav.getByRole("link", { name: "Queue" }).click();
  await expect(page).toHaveURL(/\/admin\/1\/queue$/);
  await expect(page.locator("main#main-content")).toHaveCount(1);
});

