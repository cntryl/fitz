import { expect, test } from "@playwright/test";
import { mockAdminFeatures } from "./shell/api-fixtures";

test("renders truthful open-access state when authentication is disabled", async ({ page }) => {
  await mockAdminFeatures(page);
  await page.goto("/login");

  await expect(page.getByRole("heading", { level: 1, name: "Open access" })).toBeVisible();
  await expect(page.getByText("No credentials required")).toBeVisible();
  await expect(page.getByLabel("Username")).toHaveCount(0);
  await expect(page.getByLabel("Password")).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Continue to Fitz Admin" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Toggle color theme" })).toBeVisible();
  await expect(
    page.getByRole("link", { name: "Fitz admin home" }).locator("img.fitz-brand-logo"),
  ).toBeVisible();
  await expect(page).toHaveTitle("Sign in · Fitz Admin");
  await expect(page.locator("main#main-content")).toBeFocused();
  await expect(page.locator("footer").getByRole("link", { name: "Fitz broker" })).toBeVisible();
  await expect(page.locator("footer").getByRole("link", { name: "fitz-ts" })).toBeVisible();
  await expect(page.locator("footer").getByRole("link", { name: "fitz-go" })).toBeVisible();
});
