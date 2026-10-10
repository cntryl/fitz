import { expect, test, type Page } from "@playwright/test";
import { normalizedAdminApiSegments, scenarioTime } from "./shell/api-fixtures";
import { mockResourceDetailApis } from "./shell/resource-mocks";

const scope = { area: "default", realm: "default", resource: "primary" };
const longSession = `session-${"9f3c2a7e".repeat(8)}`;
const longCorrelation = `corr-${"a1b2c3d4".repeat(8)}`;
const viewports = [
  { height: 800, width: 360 },
  { height: 1024, width: 768 },
  { height: 1200, width: 1440 },
];

/** Long identifiers are the worst case for table width, so every fixture uses them. */
async function mockLongIdentifierRows(page: Page) {
  await page.route("**/api/v1/**", async (route) => {
    const segments = normalizedAdminApiSegments(new URL(route.request().url()).pathname);
    const [, , domain] = segments;

    if (domain === "lease" && segments[3] === "search") {
      await route.fulfill({
        json: {
          area: scope.area,
          items: ["owned_with_waiters", "waiting"].map((state, index) => ({
            acquired_at: scenarioTime(-45_000),
            area: scope.area,
            expires_at: new Date(Date.now() + 120_000).toISOString(),
            owner_id: `owner-${longSession}-${index}`,
            owner_session_id: `${longSession}-${index}`,
            pending_waiters: 2,
            queued_token: 18_446_744_073_709 + index,
            realm: scope.realm,
            resource: scope.resource,
            state,
          })),
          limit: 50,
          realm: scope.realm,
          resource: scope.resource,
          route_family: 1,
        },
      });
      return;
    }

    if (domain === "kv" && segments[9] === "transactions") {
      await route.fulfill({
        json: {
          transactions: [
            {
              ...scope,
              idle_seconds: 86_400,
              mode: "read_write",
              operations_count: 1_000_000,
              started_at: scenarioTime(-86_400_000),
              tx_id: 9_007_199_254_740_991,
            },
          ],
        },
      });
      return;
    }

    if (domain === "rpc" && segments[3] === "calls") {
      await route.fulfill({
        json: {
          limit: 50,
          observations: [
            {
              ...scope,
              age_seconds: 86_400,
              average_latency_ms: 12,
              correlation_id: longCorrelation,
              operation: "GetStatus",
              registered_at: null,
              requests_handled: 7,
              route: "GetStatus",
              route_family: 1,
              state: "awaiting_worker_response",
              submitted_at: scenarioTime(-86_400_000),
              worker_session_id: longSession,
            },
          ],
          route_family: 1,
        },
      });
      return;
    }

    await route.fallback();
  });
}

async function expectTableFits(page: Page, sectionId: string) {
  const wrap = page.locator(`[aria-labelledby="${sectionId}"] .domain-table-wrap`);
  await expect(wrap.locator('[data-slot="table"]')).toBeVisible();

  const fit = await wrap.evaluate((node) => ({
    clientWidth: node.clientWidth,
    documentFits: document.documentElement.scrollWidth <= document.documentElement.clientWidth,
    overflowX: getComputedStyle(node).overflowX,
    scrollWidth: node.scrollWidth,
  }));

  expect(fit.scrollWidth, `${sectionId} scrollWidth`).toBeLessThanOrEqual(fit.clientWidth);
  expect(fit.overflowX, `${sectionId} must not become a sideways scroller`).not.toBe("auto");
  expect(fit.documentFits, `${sectionId} page width`).toBe(true);
}

for (const viewport of viewports) {
  test.describe(`domain detail tables at ${viewport.width}px`, () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(viewport);
    });

    test("fit the lease owners and waiters table", async ({ page }) => {
      await mockResourceDetailApis(page, "lease", scope);
      await mockLongIdentifierRows(page);

      await page.goto("/admin/1/lease/default/default/primary");

      await expectTableFits(page, "lease-ownership-rows");
    });

    test("fit the KV active transactions table", async ({ page }) => {
      await mockResourceDetailApis(page, "kv", scope);
      await mockLongIdentifierRows(page);

      await page.goto("/admin/1/kv/default/default/primary?transactions=1");

      await expectTableFits(page, "kv-active-transactions");
    });

    test("fit the RPC live call evidence table", async ({ page }) => {
      await mockResourceDetailApis(page, "rpc", scope);
      await mockLongIdentifierRows(page);

      await page.goto("/admin/1/rpc/default/default/primary/GetStatus");

      await expectTableFits(page, "rpc-live-call-evidence");
    });
  });
}
