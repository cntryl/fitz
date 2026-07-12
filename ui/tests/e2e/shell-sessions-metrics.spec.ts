import { expect, test } from "@playwright/test";
import {
  mockMetricsApi,
  mockSessionsApi,
  sessionsEmpty,
  sessionsWithData,
} from "./shell/api-fixtures";
import { openDashboard } from "./shell/chrome";

test("captures the mobile navbar panel", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await openDashboard(page);
  const primaryNav = page.getByRole("navigation", { name: "Primary navigation" });

  await primaryNav.getByRole("button", { name: /Menu|Navigation/ }).click();
  await expect(primaryNav.getByRole("link", { name: "Overview" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Diagnostics" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Metrics" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Queue" })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("mobile-nav-open.png"),
    animations: "disabled",
  });
});

test("captures sessions data state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockSessionsApi(page, sessionsWithData);

  await page.goto("/admin/1/sessions");
  await expect(page.getByRole("heading", { name: "Active sessions", exact: true })).toBeVisible();
  await expect(page.locator("table tbody tr").first()).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("sessions-desktop.png"),
    animations: "disabled",
  });
});

test("captures sessions empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockSessionsApi(page, sessionsEmpty);

  await page.goto("/admin/1/sessions");
  await expect(page.getByRole("heading", { name: "Active sessions", exact: true })).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "No active sessions", exact: true }),
  ).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("sessions-empty-desktop.png"),
    animations: "disabled",
  });
});

test("captures sessions on mobile", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockSessionsApi(page, sessionsWithData);

  await page.goto("/admin/1/sessions");
  await expect(page.getByRole("heading", { name: "Active sessions", exact: true })).toBeVisible();
  await expect(page.locator("ul.session-mobile-list li").first()).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("sessions-mobile.png"),
    animations: "disabled",
  });
});

test("captures metrics desktop", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockMetricsApi(page);
  await page.goto("/admin/1/metrics");

  await expect(page.getByRole("heading", { name: "Metrics explorer" })).toBeVisible();
  await expect(page.locator('input[aria-label="Filter metrics"]')).toBeVisible();
  await expect(page.getByRole("button", { name: "Refresh metrics" })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("metrics-desktop.png"),
    animations: "disabled",
  });
});

test("captures metrics filtered empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockMetricsApi(page);
  await page.goto("/admin/1/metrics");

  const filter = page.locator('input[aria-label="Filter metrics"]');
  await filter.fill("does-not-exist");
  await expect(page.getByText("No matching metrics")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("metrics-filtered-empty.png"),
    animations: "disabled",
  });
});

test("captures metrics on mobile", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockMetricsApi(page);
  await page.goto("/admin/1/metrics");

  await expect(page.getByRole("heading", { name: "Metrics explorer" })).toBeVisible();
  await expect(page.locator('input[aria-label="Filter metrics"]')).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("metrics-mobile.png"),
    animations: "disabled",
  });
});

test("captures metrics in dark mode", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockMetricsApi(page);
  await page.addInitScript(() => {
    localStorage.setItem("fitz-admin-theme", "dark");
  });

  await page.goto("/admin/1/metrics");

  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(page.getByRole("heading", { name: "Metrics explorer" })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("metrics-dark.png"),
    animations: "disabled",
  });
});
