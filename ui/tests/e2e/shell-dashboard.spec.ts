import { expect, test } from "@playwright/test";
import {
  mockAdminFeatures,
  mockDomainOverviewApis,
  topologyApiPayload,
} from "./shell/api-fixtures";
import { openDashboard } from "./shell/chrome";

test("uses mobile below 48rem and desktop at 48rem", async ({ page }) => {
  await page.setViewportSize({ width: 767, height: 900 });
  await openDashboard(page);

  const primaryNav = page.getByRole("navigation", { name: "Primary navigation" });
  await expect(primaryNav.getByRole("button", { name: /Menu|Navigation/ })).toBeVisible();

  await page.setViewportSize({ width: 768, height: 900 });
  await expect(primaryNav.getByRole("link", { name: "Overview" })).toBeVisible();
  await expect(primaryNav.getByRole("button", { name: /Menu|Navigation/ })).toBeHidden();
});

test("keeps route-family actions as native keyboard links", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await mockAdminFeatures(page);
  await page.goto("/admin");

  const openWorkspace = page.getByRole("link", {
    name: "Open workspace for Route Family 1",
  });

  await expect(openWorkspace).toHaveAttribute("href", "/admin/1");
  await expect(openWorkspace).not.toHaveAttribute("role", "button");
  await openWorkspace.focus();
  await expect(openWorkspace).toBeFocused();
  await openWorkspace.press("Enter");
  await expect(page).toHaveURL(/\/admin\/1$/);
});

test("lists every provisioned route family and opens a valid idle family", async ({ page }) => {
  await mockAdminFeatures(page);
  await page.goto("/admin");

  for (const family of ["1", "2", "3", "4", "5"]) {
    await expect(
      page.getByRole("link", { name: `Open workspace for Route Family ${family}` }),
    ).toBeVisible();
  }

  await page.getByRole("link", { name: "Open workspace for Route Family 5" }).click();
  await expect(page).toHaveURL(/\/admin\/5$/);
  await expect(page.getByRole("navigation", { name: "Primary navigation" })).toBeVisible();
});

test("renders unavailable base and nested route families as SPA 404 pages", async ({ page }) => {
  await mockAdminFeatures(page);

  for (const path of ["/admin/6", "/admin/6/queue"]) {
    await page.goto(path);

    await expect(page.getByRole("heading", { name: "Route Family not found" })).toBeVisible();
    await expect(page.getByText(/404/)).toBeVisible();
    await expect(page.getByRole("link", { name: "Back to Route Families" })).toHaveAttribute(
      "href",
      "/admin",
    );
    await expect(page.getByRole("navigation", { name: "Primary navigation" })).toHaveCount(0);
  }
});

test("captures the dashboard refreshing state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });

  let releaseRefresh: (() => void) | undefined;
  let topologyRequests = 0;

  await mockAdminFeatures(page);
  await page.route("**/api/v1/*/topology", async (route) => {
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
