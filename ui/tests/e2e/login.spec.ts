import { expect, test } from "@playwright/test";

test("renders the admin sign-in screen", async ({ page }) => {
  await page.goto("/login");

  await expect(page.getByRole("heading", { name: "Sign in to Fitz Admin" })).toBeVisible();
  await expect(page.getByLabel("Username")).toBeVisible();
  await expect(page.getByLabel("Password")).toBeVisible();
  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Toggle color theme" })).toBeVisible();
  await expect(page.locator("footer").getByRole("link", { name: "Fitz broker" })).toBeVisible();
  await expect(page.locator("footer").getByRole("link", { name: "fitz-ts" })).toBeVisible();
  await expect(page.locator("footer").getByRole("link", { name: "fitz-go" })).toBeVisible();
});
