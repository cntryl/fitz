import { expect, test, type Page } from "@playwright/test";

async function openDashboard(page: Page, theme: "light" | "dark" = "light") {
  if (theme === "dark") {
    await page.addInitScript(() => {
      localStorage.setItem("fitz-admin-theme", "dark");
    });
  }

  await page.goto("/admin");

  await expect(page.locator("main#main-content")).toHaveCount(1);
  const viewport = page.viewportSize();
  if ((viewport?.width ?? 0) < 768) {
    await expect(page.getByRole("button", { name: "Menu" })).toBeVisible();
    return;
  }
  await expect(page.getByRole("link", { name: "Fitz admin home" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Domains" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Toggle color theme" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Sign out" })).toBeVisible();
}

test("captures the desktop dashboard shell", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await openDashboard(page);

  await expect(page.getByRole("heading", { name: "Broker status" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("dashboard-desktop.png"),
    animations: "disabled",
  });
});

test("captures the desktop dashboard shell in dark mode", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await openDashboard(page, "dark");

  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(page.getByRole("heading", { name: "Broker status" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("dashboard-dark.png"),
    animations: "disabled",
  });
});

test("captures the desktop domain dropdown and closes on navigation", async ({
  page,
}, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await openDashboard(page);

  await page.getByRole("button", { name: "Domains" }).click();
  const dropdown = page.locator('[data-slot="dropdown-content"]');

  await expect(dropdown).toBeVisible();
  await expect(page.getByText("Domain pages")).toBeVisible();
  await expect(page.getByRole("link", { name: /Queue/ }).first()).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("domains-dropdown-open.png"),
    animations: "disabled",
  });

  await dropdown.locator('a[href="/queue"]').click();
  await expect(page).toHaveURL(/\/queue$/);
  await expect(page.locator("main#main-content")).toHaveCount(1);
  await expect(dropdown).toBeHidden();
});

test("captures a sidebar domain page", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await page.goto("/queue");

  await expect(page.locator("main#main-content")).toHaveCount(1);
  await expect(page.locator(".page-frame-sidebar")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Queue overview" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("queue-sidebar.png"),
    animations: "disabled",
  });
});

test("captures the mobile navbar panel", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await openDashboard(page);

  await page.getByRole("button", { name: "Menu" }).click();
  await expect(page.getByRole("link", { name: "Dashboard" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Domains" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Toggle color theme" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Sign out" })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("mobile-nav-open.png"),
    animations: "disabled",
  });
});
