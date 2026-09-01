import { expect, test, type Page } from "@playwright/test";
import {
  applyLeaseOverride,
  domainOverviewPages,
  mockDomainOverviewApis,
} from "./shell/api-fixtures";
import {
  mockQueueResourceApis,
  mockResourceDetailApis,
  mockScheduleResourceApis,
} from "./shell/resource-mocks";

async function expectDomainSummary(page: Page, mobile: boolean) {
  const stats = page.locator('.domain-inventory-summary [data-slot="stat"]');
  await expect(stats).toHaveCount(3);
  const statTops = await stats.evaluateAll((elements) =>
    elements.map((stat) => Math.round(stat.getBoundingClientRect().top)),
  );
  expect(statTops).toHaveLength(3);
  expect(new Set(statTops).size).toBe(mobile ? 3 : 1);
}

const rollupHeaders: Record<string, string[]> = {
  lease: ["Active", "Waiters", "Oldest"],
  notice: ["Subscriptions", "Publishes / min", "Delivered"],
  rpc: ["Workers", "Pending", "Slowest avg ms"],
  schedule: ["Enabled", "Pending claims", "Next run"],
  stream: ["Committed", "Storage", "Append sessions"],
};

async function expectInventoryRollups(page: Page, domain: string, mobile: boolean) {
  const headers = rollupHeaders[domain];
  if (!headers) return;

  for (const header of headers) {
    await expect(page.getByRole("columnheader", { name: new RegExp(header) })).toBeVisible();
  }
  await page.getByRole("button", { name: new RegExp(`Sort by ${headers[0]}`) }).click();
  await expect(
    page.getByRole("button", {
      name: new RegExp(`Sort by ${headers[0]}, descending`),
    }),
  ).toBeVisible();
  const scrollHint = page.getByText("Scroll horizontally to view every metric.");
  if (mobile) {
    await expect(scrollHint).toBeVisible();
  } else {
    await expect(scrollHint).toBeHidden();
  }
  if (mobile) {
    const table = page.locator(".domain-resource-data-table");
    expect(await table.evaluate((node) => node.scrollWidth > node.clientWidth)).toBe(true);
  }
}

test("captures a domain inventory page", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page);
  await page.goto("/admin/1/queue");

  await expect(page.locator("main#main-content")).toHaveCount(1);
  await expect(page.locator(".page-frame-sidebar")).toHaveCount(0);
  await expect(page.getByRole("heading", { name: /Queue inventory/ })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Realms" })).toBeVisible();
  await expect(page.getByRole("table", { name: "Realms" })).toBeVisible();
  await expect(page.getByRole("link", { name: "default" })).toHaveAttribute(
    "href",
    "/admin/1/queue/default",
  );
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("queue-inventory.png"),
    animations: "disabled",
  });
});

test("omits comparison controls from queue resource inspection", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  const queueScope = {
    area: "payments",
    realm: "acme",
    resource: "orders",
  };
  await mockQueueResourceApis(page, queueScope);
  await page.goto("/admin/1/queue/acme/payments/orders");

  await expect(page.getByRole("heading", { level: 1, name: "orders" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Compare scopes" })).toHaveCount(0);
  await expect(page.locator("#compare-realm, #compare-family")).toHaveCount(0);
  await expect(page.getByRole("heading", { level: 2, name: "Dead letters" })).toBeVisible();
  await expect(page.getByRole("table", { name: "Dead-letter queue messages" })).toBeVisible();
  await expect(page.getByRole("table", { name: "Inflight queue messages" })).toBeVisible();
  const timeline = page.getByRole("list", { name: "Queue resource timeline" });
  await expect(timeline).toBeVisible();
  await expect(timeline.getByRole("listitem")).toHaveCount(1);
  await expect(page.locator("#queue-timeline table")).toHaveCount(0);
});

test("captures lease overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    lease: applyLeaseOverride({ realms: [] }),
  });

  await page.goto("/admin/1/lease");
  await expect(page.getByRole("heading", { name: /Lease inventory/ })).toBeVisible();
  await expect(page.getByText("No lease resources are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("lease-empty-desktop.png"),
    animations: "disabled",
  });
});

test("navigates lease scope drill-down links and shows ownership countdown updates", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  const leaseScope = {
    area: "default",
    realm: "default",
    resource: "primary",
  };

  await mockDomainOverviewApis(page);
  await mockResourceDetailApis(page, "lease", leaseScope);

  await page.goto("/admin/1/lease");
  await page.locator('a[href="/admin/1/lease/default"]').click();
  await expect(page).toHaveURL("/admin/1/lease/default");
  await page.locator('a[href="/admin/1/lease/default/default"]').click();
  await expect(page).toHaveURL("/admin/1/lease/default/default");
  await page.locator('a[href="/admin/1/lease/default/default/primary"]').click();
  await expect(page).toHaveURL("/admin/1/lease/default/default/primary");
  await expect(page.getByRole("heading", { level: 1, name: "primary", exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: "Back to area" })).toHaveCount(0);
  await expect(page.getByRole("navigation", { name: "Resource hierarchy" })).toContainText(
    "primary",
  );

  const ownershipDetails = page.getByRole("region", {
    name: "Ownership details",
  });
  await expect(ownershipDetails).toBeVisible();
  await expect(page.getByRole("table", { name: "Lease ownership rows" })).toHaveCount(0);
  const remainingCell = ownershipDetails.locator("[data-field='remaining-ttl']");
  const initialRemaining = (await remainingCell.textContent())?.trim();
  await page.waitForTimeout(1200);
  const updatedRemaining = (await remainingCell.textContent())?.trim();

  expect(initialRemaining).toBeTruthy();
  expect(updatedRemaining).toBeTruthy();
  expect(updatedRemaining).not.toBe(initialRemaining);
});

test("navigates notice scope drill-down links to operation detail", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1200 });

  await mockDomainOverviewApis(page);

  await page.goto("/admin/1/notice");
  await page.locator('a[href="/admin/1/notice/default"]').click();
  await page.locator('a[href="/admin/1/notice/default/default"]').click();
  await page.locator('a[href="/admin/1/notice/default/default/primary"]').click();
  await expect(page).toHaveURL("/admin/1/notice/default/default/primary");
  await expect(page.getByRole("heading", { level: 1, name: "primary" })).toBeVisible();
  const operations = page.getByRole("list", { name: "Notice operations" });
  await expect(operations.getByRole("listitem")).toHaveCount(2);
  await expect(page.locator("#notice-resource-operations table")).toHaveCount(0);
  await page.locator('a[href="/admin/1/notice/default/default/primary/GetStatus"]').click();
  await expect(page).toHaveURL("/admin/1/notice/default/default/primary/GetStatus");
  await expect(page.getByRole("heading", { level: 1, name: "GetStatus" })).toBeVisible();
  await expect(page.getByRole("heading", { level: 2, name: "Delivery evidence" })).toBeVisible();
  const deliveries = page.getByRole("list", { name: "Delivery evidence" });
  await expect(deliveries.getByRole("listitem")).toHaveCount(1);
  await expect(deliveries).toContainText("Notifications observed");
  await expect(page.locator("#notice-delivery-evidence table")).toHaveCount(0);
  await expect(page.getByRole("navigation", { name: "Resource hierarchy" })).toContainText(
    "primary",
  );
});

test("renders notice delivery evidence as semantic list items", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page);
  await page.goto("/admin/1/notice/acme/payments/orders/RefreshProjection");

  await expect(page.getByRole("heading", { level: 1, name: "RefreshProjection" })).toBeVisible();
  await expect(page.getByRole("heading", { level: 2, name: "Delivery evidence" })).toBeVisible();

  const deliveries = page.getByRole("list", { name: "Delivery evidence" });
  await expect(deliveries.getByRole("listitem")).toHaveCount(1);
  await expect(deliveries).toContainText("session-1");
  await expect(deliveries).toContainText("Notifications observed: 12");
  await expect(deliveries.getByLabel("Status: open")).toBeVisible();
  await expect(page.locator("#notice-delivery-evidence table")).toHaveCount(0);
});

test("navigates schedule scope drill-down links to resource detail", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  const scheduleScope = {
    area: "default",
    realm: "default",
    resource: "primary",
  };

  await mockDomainOverviewApis(page);
  await mockScheduleResourceApis(page, scheduleScope);

  await page.goto("/admin/1/schedule");
  await expect(page.getByRole("heading", { name: /Schedule inventory/ })).toBeVisible();
  await page.locator('a[href="/admin/1/schedule/default"]').click();
  await page.locator('a[href="/admin/1/schedule/default/default"]').click();
  await page.locator('a[href="/admin/1/schedule/default/default/primary"]').click();
  await expect(page).toHaveURL("/admin/1/schedule/default/default/primary");
  await expect(page.getByRole("heading", { level: 1, name: "primary" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Individual schedules" })).toBeVisible();

  await page.locator('a[href="/admin/1/schedule/default/default/primary/handoff"]').click();
  await expect(page).toHaveURL("/admin/1/schedule/default/default/primary/handoff");
  await expect(page.getByRole("heading", { level: 1, name: "handoff" })).toBeVisible();
  await expect(page.getByText("Non-authoritative; not downstream execution history")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Pending and missed handoffs" })).toBeVisible();
});

test("captures kv overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    kv: {
      realms: [],
      stats: {
        commits_failed_total: 0,
        invalid_transaction_rejects_total: 0,
        keys_total: 0,
        operations_per_second: 0,
        transactions_active: 0,
      },
    },
  });

  await page.goto("/admin/1/kv");
  await expect(page.getByRole("heading", { name: /KV tables/ })).toBeVisible();
  await expect(page.getByText("No KV tables are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("kv-empty-desktop.png"),
    animations: "disabled",
  });
});

test("captures notice overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    notice: {
      realms: [],
      stats: {
        delivery_drops_total: 0,
        publishes_per_second: 0,
        routes_active: 0,
        wildcard_limit_rejects_total: 0,
        subscriptions_active: 0,
        max_route_subscribers: 0,
      },
    },
  });

  await page.goto("/admin/1/notice");
  await expect(page.getByRole("heading", { name: /Notice inventory/ })).toBeVisible();
  await expect(page.getByText("No notice resources are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("notice-empty-desktop.png"),
    animations: "disabled",
  });
});

test("captures stream overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    stream: {
      realms: [],
      stats: {
        events_total: 0,
        operations_per_second: 0,
        streams_active: 0,
        subscriptions_active: 0,
        watermark_lag_buckets: {
          caught_up: 0,
          over_100: 0,
          under_10: 0,
          under_100: 0,
        },
      },
    },
  });

  await page.goto("/admin/1/stream");
  await expect(page.getByRole("heading", { name: /Stream inventory/ })).toBeVisible();
  await expect(page.getByText("No stream resources are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("stream-empty-desktop.png"),
    animations: "disabled",
  });
});

test("captures rpc overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    rpc: {
      realms: [],
      stats: {
        failure_total: 0,
        invalid_sequence_errors_dropped_total: 0,
        invalid_sequence_errors_forwarded_total: 0,
        invalid_sequence_responses_total: 0,
        operations_per_second: 0,
        pending_routes_active: 0,
        request_timeouts_total: 0,
        requests_pending: 0,
        responses_dropped_closed_caller_total: 0,
        responses_missing_pending_total: 0,
        workers_registered: 0,
      },
    },
  });

  await page.goto("/admin/1/rpc");
  await expect(page.getByRole("heading", { name: /RPC inventory/ })).toBeVisible();
  await expect(page.getByText("No RPC resources are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("rpc-empty-desktop.png"),
    animations: "disabled",
  });
});

test("captures schedule overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    schedule: {
      realms: [],
      stats: {
        ack_failures_total: 0,
        cancel_persistence_failures_total: 0,
        create_persistence_failures_total: 0,
        executions_per_minute: 0,
        notify_failures_total: 0,
        overdue_normalizations_total: 0,
        pending_fire_claims: 0,
        schedules_active: 0,
        subscriptions_active: 0,
        upsert_persistence_failures_total: 0,
      },
    },
  });

  await page.goto("/admin/1/schedule");
  await expect(page.getByRole("heading", { name: /Schedule inventory/ })).toBeVisible();
  await expect(page.getByText("No schedule resources are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("schedule-empty-desktop.png"),
    animations: "disabled",
  });
});

test.describe("captures domain overview templates", () => {
  for (const overviewPage of domainOverviewPages) {
    test(`captures ${overviewPage.domain} overview on desktop`, async ({ page }, testInfo) => {
      await page.setViewportSize({ width: 1440, height: 1200 });
      await mockDomainOverviewApis(page);
      const detailRequests: string[] = [];
      page.on("request", (request) => {
        if (/\/resources\/[^/?]+(?:\?.*)?$/.test(request.url())) detailRequests.push(request.url());
      });
      await page.goto(overviewPage.path);

      await expect(page.getByRole("heading", { name: overviewPage.heading })).toBeVisible();
      await expect(page.locator("main#main-content")).toHaveCount(1);
      await expect(page.getByRole("table", { name: "Realms" })).toBeVisible();
      await expectDomainSummary(page, false);
      expect(detailRequests).toEqual([]);

      await page.screenshot({
        fullPage: true,
        path: testInfo.outputPath(`${overviewPage.domain}-desktop.png`),
        animations: "disabled",
      });
    });

    test(`captures ${overviewPage.domain} overview on mobile`, async ({ page }, testInfo) => {
      await page.setViewportSize({ width: 390, height: 844 });
      await mockDomainOverviewApis(page);
      const detailRequests: string[] = [];
      page.on("request", (request) => {
        if (/\/resources\/[^/?]+(?:\?.*)?$/.test(request.url())) detailRequests.push(request.url());
      });
      await page.goto(overviewPage.path);

      await expect(page.getByRole("heading", { name: overviewPage.heading })).toBeVisible();
      await expect(page.locator("main#main-content")).toHaveCount(1);
      await expect(page.getByRole("table", { name: "Realms" })).toBeVisible();
      await expectDomainSummary(page, true);
      expect(detailRequests).toEqual([]);

      await page.screenshot({
        fullPage: true,
        path: testInfo.outputPath(`${overviewPage.domain}-mobile.png`),
        animations: "disabled",
      });
    });
  }
});

test.describe("progressive domain drilldown", () => {
  for (const overviewPage of domainOverviewPages) {
    test(`drills ${overviewPage.domain} through realm and area`, async ({ page }) => {
      await page.setViewportSize({ width: 1440, height: 1200 });
      await mockDomainOverviewApis(page);
      await page.goto(overviewPage.path);

      const realmTable = page.getByRole("table", { name: "Realms" });
      const realmLink = realmTable.getByRole("link").first();
      const realmHref = await realmLink.getAttribute("href");
      expect(realmHref).toBeTruthy();
      await realmLink.click();
      await expect(page).toHaveURL(realmHref ?? "");

      const areaTable = page.getByRole("table", { name: "Areas" });
      const areaLink = areaTable.getByRole("link").first();
      const areaHref = await areaLink.getAttribute("href");
      expect(areaHref).toBeTruthy();
      await areaLink.click();
      await expect(page).toHaveURL(areaHref ?? "");

      await expect(page.getByRole("table", { name: "Resource inventory" })).toBeVisible();
      await expectInventoryRollups(page, overviewPage.domain, false);
    });
  }
});
