import { expect, test } from "@playwright/test";
import { mockAdminFeatures } from "./shell/api-fixtures";

async function mockCredentialLogin(page: import("@playwright/test").Page) {
  await page.route("**/api/v1/features", async (route) => {
    await route.fulfill({
      json: {
        admin_auth_required: true,
        admin_auth_mode: "password",
        route_families: ["1"],
        route_families_wildcard: false,
      },
    });
  });

  await page.route("**/api/v1/session", async (route) => {
    await route.fulfill({ status: 401, json: { error: "unauthenticated" } });
  });
}

test("should_center_the_credential_form_in_the_full_viewport", async ({ page }) => {
  // Arrange
  await mockCredentialLogin(page);
  await page.goto("/login");
  await expect(
    page.getByRole("heading", { level: 1, name: "Sign in to Fitz Admin" }),
  ).toBeVisible();
  const viewport = page.viewportSize();

  // Act
  const pageBounds = await page.getByRole("main").boundingBox();
  const pagePadding = await page.getByRole("main").evaluate((element) => {
    const styles = getComputedStyle(element);
    return { inlineEnd: styles.paddingRight, inlineStart: styles.paddingLeft };
  });
  const cardBounds = await page.locator('[data-slot="card"]').boundingBox();

  // Assert
  expect(viewport).not.toBeNull();
  expect(pageBounds).not.toBeNull();
  expect(cardBounds).not.toBeNull();
  if (!viewport || !pageBounds || !cardBounds) return;

  expect(pagePadding).toEqual({ inlineEnd: "40px", inlineStart: "40px" });
  expect(pageBounds.width).toBeGreaterThanOrEqual(viewport.width - 1);
  expect(pageBounds.height).toBeGreaterThanOrEqual(viewport.height - 1);
  expect(cardBounds.width).toBe(384);
  expect(Math.abs(cardBounds.x + cardBounds.width / 2 - viewport.width / 2)).toBeLessThanOrEqual(1);
  expect(Math.abs(cardBounds.y + cardBounds.height / 2 - viewport.height / 2)).toBeLessThanOrEqual(
    1,
  );
  await expect(page.locator('[data-slot="card"] img.fitz-brand-logo')).toBeVisible();
  await expect(page.locator("header, footer")).toHaveCount(0);
});

test("should_preserve_mobile_padding_around_the_login_card", async ({ page }) => {
  // Arrange
  await page.setViewportSize({ width: 360, height: 800 });
  await mockCredentialLogin(page);
  await page.goto("/login");
  await expect(
    page.getByRole("heading", { level: 1, name: "Sign in to Fitz Admin" }),
  ).toBeVisible();

  // Act
  const pagePadding = await page.getByRole("main").evaluate((element) => {
    const styles = getComputedStyle(element);
    return { inlineEnd: styles.paddingRight, inlineStart: styles.paddingLeft };
  });
  const cardBounds = await page.locator('[data-slot="card"]').boundingBox();

  // Assert
  expect(pagePadding).toEqual({ inlineEnd: "24px", inlineStart: "24px" });
  expect(cardBounds).not.toBeNull();
  expect(cardBounds?.x).toBe(24);
  expect(cardBounds?.width).toBe(312);
});

test("renders truthful open-access state when authentication is disabled", async ({ page }) => {
  await mockAdminFeatures(page);
  await page.goto("/login");

  await expect(page.getByRole("heading", { level: 1, name: "Open access" })).toBeVisible();
  await expect(page.getByText("No credentials required")).toBeVisible();
  await expect(page.getByLabel("Username")).toHaveCount(0);
  await expect(page.getByLabel("Password")).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Continue to Fitz Admin" })).toBeVisible();
  await expect(page.locator('[data-slot="card"] img.fitz-brand-logo')).toBeVisible();
  await expect(page).toHaveTitle("Sign in · Fitz Admin");
  await expect(page.locator("main#main-content")).toBeFocused();
  await expect(page.locator("header, footer")).toHaveCount(0);
});
