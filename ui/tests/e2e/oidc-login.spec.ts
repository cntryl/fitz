import { expect, test, type Page } from "@playwright/test";

const providerStart = /\/api\/v1\/session\/oidc\/start\?/;

async function mockOidc(page: Page) {
  await page.route("**/api/v1/features", (route) =>
    route.fulfill({
      json: {
        admin_auth_required: true,
        admin_auth_mode: "protected",
        admin_oidc_enabled: true,
        route_families: [],
        route_families_wildcard: false,
      },
    }),
  );
  await page.route("**/api/v1/session", (route) => route.fulfill({ status: 401 }));
}

test("should start a full browser navigation to the OIDC provider", async ({ page }) => {
  await mockOidc(page);
  await page.route(providerStart, (route) =>
    route.fulfill({ contentType: "text/html", body: "<h1>Provider sign-in</h1>" }),
  );
  const start = page.waitForRequest((request) =>
    new URL(request.url()).pathname.endsWith("/oidc/start"),
  );
  const next = "/admin/1/sessions?view=active";

  await page.goto(next);

  const request = await start;
  expect(request.isNavigationRequest()).toBe(true);
  expect(new URL(request.url()).searchParams.get("next")).toBe(next);
  await expect(page).toHaveURL(providerStart);
});

test("should await an explicit sign-in after OIDC logout", async ({ page }) => {
  await mockOidc(page);
  let starts = 0;
  await page.route(providerStart, (route) => {
    starts += 1;
    return route.fulfill({ contentType: "text/html", body: "<h1>Provider sign-in</h1>" });
  });

  await page.goto("/login?signed_out=1");

  await expect(page.getByRole("heading", { name: "Signed out" })).toBeVisible();
  expect(starts).toBe(0);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page).toHaveURL(providerStart);
});

test("should show session recovery given an unavailable session endpoint", async ({ page }) => {
  await mockOidc(page);
  await page.route("**/api/v1/session", (route) => route.fulfill({ status: 503 }));

  await page.goto("/login");

  await expect(page.getByRole("heading", { name: "Could not verify your session" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Retry" })).toBeVisible();
});
