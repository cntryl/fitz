import { describe, expect, it } from "vite-plus/test";
import { cleanupApp } from "@askrjs/askr/boot";
import { click, queryState } from "@askrjs/askr/testing";
import { mountRoute, pageSmokeMocks, queryOptions, resetQueries } from "./page-smoke/harness";
import {
  domainOverviews,
  inventory,
  leaseOverview,
  noticeOperationRows,
  noticeResourceRows,
  queueInventory,
  queueResourceRow,
} from "./page-smoke/fixtures";

const mocks = pageSmokeMocks();

describe("admin domain inventory smoke tests", () => {
  it("shows active subscription counts in the stream resource inventory", async () => {
    mocks.queryStates.inventory = queryState.fresh(
      {
        ...inventory,
        realms: [
          {
            realm: "default",
            areas: [
              {
                area: "events",
                resources: ["idle", "orders"],
                resourceEntries: [
                  { resource: "idle", committedEventCount: 0, subscriptionsActive: 0 },
                  { resource: "orders", committedEventCount: 12, subscriptionsActive: 3 },
                ],
              },
            ],
          },
        ],
      },
      queryOptions(),
    );

    const { default: StreamPage } = await import("@/pages/app/stream");
    const root = await mountRoute(
      "/admin/1/stream/default/events",
      "/admin/{family}/stream/{realm}/{area}",
      StreamPage,
    );
    const table = root.querySelector<HTMLTableElement>("#stream-inventory-table");

    expect(table?.querySelector('th[data-column-id="subscriptions"]')).toBeTruthy();
    expect(table?.querySelector('th[data-column-id="subscriptions"]')?.textContent).toContain(
      "Live subscriptions",
    );
    expect(
      Array.from(table?.querySelectorAll('tbody td[data-column-id="subscriptions"]') ?? []).map(
        (cell) => cell.textContent?.trim(),
      ),
    ).toEqual(["0", "3"]);
    const sort = root.querySelector<HTMLButtonElement>(
      'button[aria-label="Sort by Live subscriptions, not sorted"]',
    );
    expect(sort).toBeTruthy();
    click(sort!);
    await new Promise<void>((resolve) => queueMicrotask(resolve));
    expect(
      Array.from(
        root
          .querySelector<HTMLTableElement>("#stream-inventory-table")
          ?.querySelectorAll('tbody td[data-column-id="subscriptions"]') ?? [],
      ).map((cell) => cell.textContent?.trim()),
    ).toEqual(["3", "0"]);

    cleanupApp(root);
    document.body.innerHTML = "";
  });

  it("renders domain overview error states with page-specific framing", async () => {
    for (const page of domainOverviews) {
      resetQueries();
      mocks.queryStates[page.inventoryKey] = queryState.error(
        new Error(page.errorText),
        undefined,
        queryOptions(),
      );

      const { default: Component } = await page.module();
      const root = await mountRoute(page.path, page.routePath, Component);

      expect(root.textContent).toContain(page.errorTitle ?? "Unable to load");
      expect(root.textContent).toContain(page.errorText);

      cleanupApp(root);
      document.body.innerHTML = "";
    }
  });
  it("shows scoped lease waiters without inferring root health from counters", async () => {
    mocks.queryStates.inventory = queryState.fresh(
      {
        ...inventory,
        realms: [
          {
            realm: "default",
            areas: [
              {
                area: "ops",
                resources: ["primary"],
                resourceEntries: [
                  { resource: "primary", waiters: 4, activeLeases: 1, oldestLeaseAgeSeconds: 3600 },
                ],
              },
            ],
          },
        ],
      },
      queryOptions(),
    );

    const { default: LeasePage } = await import("@/pages/app/lease");
    const root = await mountRoute("/lease", "/lease", LeasePage);
    const text = root.textContent ?? "";

    expect(text).toContain("Lease inventory");
    expect(text).toContain("Realms");
    expect(text).toContain("Waiters present");
    expect(root.querySelector(".domain-status-reason")).toBeNull();
    expect(root.querySelector('td[data-column-id="waiters"]')?.textContent).toBe("4");
    expect(root.querySelector('a[href="/admin/1/lease/default"]')).toBeTruthy();
  });
  it("shows dead-letter evidence in the queue inventory", async () => {
    // Arrange
    mocks.queryStates.queueInventory = queryState.fresh(
      {
        ...queueInventory,
        realms: [
          {
            realm: "default",
            areas: [
              {
                area: "ops",
                resources: ["primary"],
                resourceEntries: [{ ...queueResourceRow, messagesDeadLettered: 3 }],
              },
            ],
          },
        ],
      },
      queryOptions(),
    );

    // Act
    const { default: QueuePage } = await import("@/pages/app/queue");
    const root = await mountRoute("/queue", "/queue", QueuePage);

    // Assert
    expect(root.textContent).toContain("Dead letters");
    expect(root.querySelector('td[data-column-id="dead-lettered"]')?.textContent).toBe("3");
    expect(root.querySelector(".domain-status-reason")).toBeNull();
  });

  it("omits the status reason until domain health loads", async () => {
    // Arrange
    mocks.queryStates.kv = queryState.loading(queryOptions());

    // Act
    const { default: KvPage } = await import("@/pages/app/kv");
    const root = await mountRoute("/kv", "/kv", KvPage);

    // Assert
    expect(root.querySelector(".domain-status-reason")).toBeNull();
  });

  it("keeps family-wide health off realm pages", async () => {
    // Arrange
    mocks.queryStates.inventory = queryState.fresh(inventory, queryOptions());

    // Act
    const { default: LeasePage } = await import("@/pages/app/lease");
    const root = await mountRoute("/lease/default", "/lease/{realm}", LeasePage);

    // Assert
    expect(root.querySelector(".domain-status-reason")).toBeNull();
    expect(root.querySelector(".domain-header [role='status']")).toBeNull();
  });

  it("keeps health unavailable when the inventory exposes activity only", async () => {
    // Arrange
    mocks.queryStates.lease = queryState.fresh(leaseOverview, queryOptions());

    // Act
    const { default: LeasePage } = await import("@/pages/app/lease");
    const root = await mountRoute("/lease", "/lease", LeasePage);

    // Assert
    expect(root.querySelector(".domain-header [role='status']")?.textContent).toBe(
      "Health unavailable",
    );
  });

  it("does not repeat hierarchy rollups as prose summaries", async () => {
    // Arrange
    const { default: NoticePage } = await import("@/pages/app/notice");
    const { default: LeasePage } = await import("@/pages/app/lease");

    // Act
    const notice = await mountRoute("/notice", "/notice", NoticePage);
    const noticeText = notice.textContent ?? "";
    cleanupApp(notice);
    document.body.innerHTML = "";
    const lease = await mountRoute("/lease", "/lease", LeasePage);

    // Assert
    expect(noticeText).toContain("Observed subscribers");
    expect(notice.querySelector(".domain-status-reason")).toBeNull();
    expect(lease.textContent).toContain("Oldest ownership");
    expect(lease.querySelector(".domain-status-reason")).toBeNull();
    cleanupApp(lease);
    document.body.innerHTML = "";
    const { default: SchedulePage } = await import("@/pages/app/schedule");
    const schedule = await mountRoute("/schedule", "/schedule", SchedulePage);
    expect(schedule.textContent).toContain("Next run");
    expect(schedule.querySelector(".domain-status-reason")).toBeNull();
  });

  it("renders kv tables with inventory stats and explorer links", async () => {
    const { default: KvPage } = await import("@/pages/app/kv");
    const root = await mountRoute("/kv/default/ops", "/kv/{realm}/{area}", KvPage);
    const text = root.textContent ?? "";
    const labels = [
      "Route",
      "Active transactions",
      "Worst reported read p95 ms",
      "Worst reported write p95 ms",
    ];

    let cursor = -1;
    for (const label of labels) {
      const index = text.indexOf(label, cursor + 1);
      expect(index).toBeGreaterThan(cursor);
      cursor = index;
    }

    expect(text).toContain("KV area");
    expect(text).toContain("kv://default/ops/primary");
    expect(text).toContain("300");
    expect(text).toContain("16.0 KiB");
    expect(text).toContain("12.5");
    expect(text).toContain("18.3");
    expect(text).not.toContain("2.4 / 12.5");
    expect(text).not.toContain("4.1 / 18.3");
    expect(text).not.toContain("Domain txns");
    expect(text).not.toContain("Failures");
    expect(root.querySelector('a[href="/admin/1/kv/default/ops/primary"]')).not.toBeNull();
  });
  it("omits KV telemetry columns that the inventory did not report", async () => {
    mocks.queryStates.inventory = queryState.fresh(
      {
        ...inventory,
        realms: [
          {
            areas: [
              {
                area: "ops",
                resourceEntries: [{ resource: "primary" }],
                resources: ["primary"],
              },
            ],
            realm: "default",
          },
        ],
      },
      queryOptions(),
    );

    const { default: KvPage } = await import("@/pages/app/kv");
    const root = await mountRoute("/kv/default/ops", "/kv/{realm}/{area}", KvPage);
    const headers = Array.from(root.querySelectorAll("[data-slot='table-header-cell']"), (header) =>
      header.textContent?.trim(),
    );

    expect(headers).toEqual(["Route"]);
    expect(root.textContent).not.toContain("Read p95 ms");
  });
  it("renders domain overviews with empty resource inventories", async () => {
    for (const page of domainOverviews) {
      resetQueries();

      if (page.inventoryKey === "queueInventory") {
        mocks.queryStates.queueInventory = queryState.fresh(
          {
            ...queueInventory,
            realms: [],
          },
          queryOptions(),
        );
      } else {
        mocks.queryStates.inventory = queryState.fresh(
          {
            ...inventory,
            realms: [],
          },
          queryOptions(),
        );
      }

      const { default: Component } = await page.module();
      const root = await mountRoute(page.path, page.routePath, Component);

      expect(root.textContent).toContain(page.emptyText);

      cleanupApp(root);
      document.body.innerHTML = "";
    }
  });
  it("renders lease empty states at each hierarchy scope", async () => {
    const { default: LeasePage } = await import("@/pages/app/lease");
    const { default: LeaseResourcePage } = await import("@/pages/app/lease-resource");

    mocks.queryStates.inventory = queryState.fresh(
      {
        ...inventory,
        realms: [],
      },
      queryOptions(),
    );
    let root = await mountRoute("/lease", "/lease", LeasePage);
    expect(root.textContent).toContain("No lease resources are currently visible.");

    cleanupApp(root);
    document.body.innerHTML = "";

    root = await mountRoute("/lease/default", "/lease/{realm}", LeasePage);
    expect(root.textContent).toContain("No lease resources are currently visible.");

    cleanupApp(root);
    document.body.innerHTML = "";

    root = await mountRoute("/lease/default/ops", "/lease/{realm}/{area}", LeasePage);
    expect(root.textContent).toContain("No lease resources are currently visible.");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.leaseResourceRows = queryState.fresh(
      {
        items: [],
        limit: 50,
        routeFamily: 7,
      },
      queryOptions(),
    );
    root = await mountRoute(
      "/admin/1/lease/default/ops/primary",
      "/admin/{family}/lease/{realm}/{area}/{resource}",
      LeaseResourcePage,
    );
    expect(root.textContent).toContain("No visible lease ownership rows at the current level.");
    expect(root.querySelector("[data-slot='table']")).toBeNull();
  });
  it("renders notice empty states at each hierarchy scope", async () => {
    const { default: NoticePage } = await import("@/pages/app/notice");
    const { default: NoticeOperationPage } = await import("@/pages/app/notice-operation");

    mocks.queryStates.inventory = queryState.fresh(
      {
        ...inventory,
        realms: [],
      },
      queryOptions(),
    );
    let root = await mountRoute("/notice", "/notice", NoticePage);
    expect(root.textContent).toContain("No notice resources are currently visible.");
    cleanupApp(root);
    document.body.innerHTML = "";

    root = await mountRoute("/notice/default", "/notice/{realm}", NoticePage);
    expect(root.textContent).toContain("No notice resources are currently visible.");
    cleanupApp(root);
    document.body.innerHTML = "";

    root = await mountRoute("/notice/default/ops", "/notice/{realm}/{area}", NoticePage);
    expect(root.textContent).toContain("No notice resources are currently visible.");
    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.noticeResourceRows = queryState.fresh(
      {
        ...noticeResourceRows,
        operations: [],
      },
      queryOptions(),
    );
    root = await mountRoute(
      "/admin/1/notice/default/ops/primary",
      "/admin/{family}/notice/{realm}/{area}/{resource}",
      NoticePage,
    );
    expect(root.textContent).toContain("No matching notice operations are currently visible.");
    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.noticeOperationRows = queryState.fresh(
      {
        ...noticeOperationRows,
        observations: [],
      },
      queryOptions(),
    );
    root = await mountRoute(
      "/admin/1/notice/default/ops/primary/GetStatus",
      "/admin/{family}/notice/{realm}/{area}/{resource}/{operation}",
      NoticeOperationPage,
    );
    expect(root.textContent).toContain("No matching notice deliveries are currently visible.");
    cleanupApp(root);
    document.body.innerHTML = "";
  });
  it("renders lease ownership remaining time on the lease resource page", async () => {
    const leaseExpiresAt = new Date(Date.now() + 5000).toISOString();
    mocks.queryStates.leaseResourceRows = queryState.fresh(
      {
        items: [
          {
            acquiredAt: "2026-05-21T13:00:00.000Z",
            ageSeconds: 1,
            area: "ops",
            expiresAt: leaseExpiresAt,
            ownerId: "owner-lease-primary",
            ownerSessionId: "session-lease-primary",
            pendingWaiters: 0,
            queuedToken: 12,
            realm: "default",
            resource: "primary",
            routeFamily: 7,
            state: "owned",
          },
        ],
        limit: 50,
        routeFamily: 7,
      },
      queryOptions(),
    );

    const { default: LeaseResourcePage } = await import("@/pages/app/lease-resource");
    const root = await mountRoute(
      "/admin/1/lease/default/ops/primary",
      "/admin/{family}/lease/{realm}/{area}/{resource}",
      LeaseResourcePage,
    );
    const ownershipTable = root.querySelector(
      '[aria-labelledby="lease-ownership-rows"] [data-slot="table"]',
    );
    const initialRemaining = ownershipTable
      ?.querySelector("[data-field='remaining-ttl']")
      ?.textContent?.trim();
    expect(ownershipTable).toBeTruthy();
    expect(root.textContent).toContain("Fencing token 12");
    expect(root.querySelector('table[aria-label="Lease ownership rows"]')).toBeNull();
    expect(initialRemaining).toBeTruthy();
    expect(root.textContent).not.toContain("not crash-safe continuity");
  });
  it("keeps Notice delivery health unavailable without scoped drop evidence", async () => {
    mocks.queryStates.inventory = queryState.fresh(inventory, queryOptions());

    const { default: NoticePage } = await import("@/pages/app/notice");
    const root = await mountRoute("/notice", "/notice", NoticePage);
    const text = root.textContent ?? "";

    expect(text).toContain("Notice inventory");
    expect(text).toContain("Realms");
    expect(text).toContain("Delivery health unavailable");
    expect(text).toContain("Observed subscribers");
    expect(root.querySelector(".domain-status-reason")).toBeNull();
    expect(root.querySelector('a[href="/admin/1/notice/default"]')).toBeTruthy();
  });
  it("renders Notice resource publish rates without an unavailable latency column", async () => {
    const { default: NoticePage } = await import("@/pages/app/notice");
    const root = await mountRoute(
      "/admin/1/notice/default/ops/primary",
      "/admin/{family}/notice/{realm}/{area}/{resource}",
      NoticePage,
    );
    const text = root.textContent ?? "";

    expect(root.querySelector(".domain-header-title-row > span:first-child")?.textContent).toBe(
      "primary",
    );
    expect(text).toContain("Publishes/min");
    expect(text).not.toContain("Latency");
    expect(text).not.toContain("--/N/A");
    // The operation tier uses the same table contract as realm, area, and resource.
    const operations = root.querySelector(
      '[data-slot="table"][aria-label="Observed subscription patterns"]',
    );
    expect(operations?.querySelectorAll('[data-slot="table-row"][data-row-key]')).toHaveLength(1);
    expect(
      root.querySelector('a[href="/admin/1/notice/default/ops/primary/GetStatus"]'),
    ).toBeTruthy();
    expect(root.querySelector("#notice-operations-search")).toBeTruthy();
  });
  it("shows Notice pattern evidence without repeating route totals on every subscriber", async () => {
    const { default: NoticeOperationPage } = await import("@/pages/app/notice-operation");

    const root = await mountRoute(
      "/notice/default/ops/primary/GetStatus",
      "/notice/{realm}/{area}/{resource}/{operation}",
      NoticeOperationPage,
    );
    const text = root.textContent ?? "";

    expect(text).toContain("Notice subscription pattern");
    expect(text).toContain("GetStatus");
    expect(text).toContain("Subscribers observed");
    expect(text).toContain("Publishes/min");
    expect(text).not.toContain("Current publishes / min");
    expect(text).not.toContain("Observed publish total");
    expect(text).not.toContain("does not report a reset scope");
    expect(text).toContain("session-1");
    expect(text).toContain("session-2");
    const deliveries = root.querySelector('ul[aria-label="Delivery evidence"]');
    expect(deliveries?.querySelectorAll('[data-slot="item"]')).toHaveLength(2);
    expect(root.querySelector("#notice-delivery-evidence [data-slot='table']")).toBeNull();
  });
});
