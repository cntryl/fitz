import { describe, expect, it } from "vite-plus/test";
import { cleanupApp } from "@askrjs/askr/boot";
import { click, queryState } from "@askrjs/askr/testing";
import { mountRoute, pageSmokeMocks, queryOptions, resetQueries } from "./page-smoke/harness";
import { inventory, scheduleResource } from "./page-smoke/fixtures";

const mocks = pageSmokeMocks();

describe("admin domain detail smoke tests", () => {
  it("shows pending schedule claims without interpreting family counters as health", async () => {
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
                resourceEntries: [{ resource: "primary", pendingClaims: 7, schedulesActive: 11 }],
              },
            ],
          },
        ],
      },
      queryOptions(),
    );

    const { default: SchedulePage } = await import("@/pages/app/schedule");
    const root = await mountRoute("/schedule", "/schedule", SchedulePage);
    const text = root.textContent ?? "";

    expect(text).toContain("Schedule inventory");
    expect(text).toContain("Realms");
    expect(text).toContain("Claims pending");
    expect(text).toContain("Pending claims");
    expect(root.querySelector(".domain-status-reason")).toBeNull();
    expect(root.querySelector('a[href="/admin/1/schedule/default"]')).toBeTruthy();
  });
  it("renders schedule hierarchy routes and resource drill-down pages", async () => {
    const { default: SchedulePage } = await import("@/pages/app/schedule");
    const realmRoot = await mountRoute("/schedule/default", "/schedule/{realm}", SchedulePage);
    expect(realmRoot.textContent).toContain("Schedule realm");
    expect(realmRoot.textContent).toContain("Areas");
    expect(realmRoot.querySelector('a[href="/admin/1/schedule/default/ops"]')).toBeTruthy();
    cleanupApp(realmRoot);
    document.body.innerHTML = "";

    const areaRoot = await mountRoute(
      "/schedule/default/ops",
      "/schedule/{realm}/{area}",
      SchedulePage,
    );
    expect(areaRoot.textContent).toContain("Schedule area");
    expect(areaRoot.textContent).toContain("Resource inventory");
    expect(areaRoot.textContent).toContain("schedule://default/ops/primary");
    // Schedule reports these per resource, so its inventory carries them like every
    // other domain rather than rendering a bare route list.
    expect(areaRoot.textContent).toContain("Pending claims");
    expect(
      Array.from(areaRoot.querySelectorAll("#schedule-inventory-table th")).map((th) =>
        th.textContent?.trim(),
      ),
    ).toEqual(["Route", "Pending claims", "Next run", "Enabled schedules"]);
    cleanupApp(areaRoot);
    document.body.innerHTML = "";

    const { default: ScheduleResourcePage } = await import("@/pages/app/schedule-resource");
    const resourceRoot = await mountRoute(
      "/admin/1/schedule/default/ops/primary",
      "/admin/{family}/schedule/{realm}/{area}/{resource}",
      ScheduleResourcePage,
    );
    const resourceText = resourceRoot.textContent ?? "";

    expect(
      resourceRoot.querySelector(".domain-header-title-row > span:first-child")?.textContent,
    ).toBe("primary");
    expect(resourceText).toContain("Individual schedules");
    expect(resourceText).toContain("schedule://default/ops/primary/handoff");
    expect(resourceRoot.querySelector("#schedule-resource-timing")).toBeNull();
    expect(resourceRoot.querySelector(".domain-header [role='status']")).toBeNull();
    expect(resourceRoot.querySelector(".domain-summary-strip")).toBeNull();
    expect(resourceText).toContain("Pending");
    expect(
      Array.from(resourceRoot.querySelectorAll('th[data-priority="secondary"]')).map((cell) =>
        cell.getAttribute("data-column-id"),
      ),
    ).toEqual(["cron", "last-handoff"]);
    // Single-schedule detail and the run action stay on the operation tier.
    expect(resourceText).not.toContain("Schedule timing");
    expect(resourceText).not.toContain("Pending and missed handoffs");
    expect(resourceText).not.toContain("Run now");
    expect(
      resourceRoot.querySelector('a[href="/admin/1/schedule/default/ops/primary/handoff"]'),
    ).toBeTruthy();
    cleanupApp(resourceRoot);
    document.body.innerHTML = "";

    const { default: ScheduleOperationPage } = await import("@/pages/app/schedule-operation");
    const operationRoot = await mountRoute(
      "/admin/1/schedule/default/ops/primary/handoff",
      "/admin/{family}/schedule/{realm}/{area}/{resource}/{operation}",
      ScheduleOperationPage,
    );
    const text = operationRoot.textContent ?? "";

    expect(text).toContain("Schedule timing");
    expect(text).toContain("Next run");
    expect(text).not.toContain("Non-authoritative; not downstream execution history");
    expect(text).toContain("Pending and missed handoffs");
    expect(text).not.toContain("Scroll the table horizontally");
    const missedTable = operationRoot.querySelector(
      "#schedule-missed-handoffs [data-slot='table']",
    );
    expect(
      Array.from(missedTable?.querySelectorAll('[data-slot="table-header-cell"]') ?? []).map(
        (header) => header.textContent?.trim(),
      ),
    ).toEqual(["Fire at", "Age", "Status"]);
    expect(text).toContain("Run now");
    expect(text).not.toContain("Is anyone listening?");
    expect(text).not.toContain("No live listeners visible");
    expect(text).not.toContain("Back to schedule area");
  });
  it("keeps next-run evidence in the schedule rows without a duplicate single-stat block", async () => {
    // Arrange
    mocks.queryStates.scheduleResource = queryState.fresh(
      {
        ...scheduleResource,
        executionObservations: { ...scheduleResource.executionObservations, has_more: true },
      },
      queryOptions(),
    );
    const { default: ScheduleResourcePage } = await import("@/pages/app/schedule-resource");

    // Act
    const root = await mountRoute(
      "/admin/1/schedule/default/ops/primary",
      "/admin/{family}/schedule/{realm}/{area}/{resource}",
      ScheduleResourcePage,
    );
    // Assert
    expect(root.querySelector("#schedule-resource-timing")).toBeNull();
    expect(root.querySelector("#schedule-operations-table")).toBeTruthy();
    expect(root.textContent).toContain("Next run");
  });

  it("links to the next schedule operation page when more rows exist", async () => {
    mocks.queryStates.scheduleResource = queryState.fresh(
      {
        ...scheduleResource,
        executionObservations: {
          ...scheduleResource.executionObservations,
          has_more: true,
        },
      },
      queryOptions(),
    );
    const { default: ScheduleResourcePage } = await import("@/pages/app/schedule-resource");

    const root = await mountRoute(
      "/admin/1/schedule/default/ops/primary",
      "/admin/{family}/schedule/{realm}/{area}/{resource}",
      ScheduleResourcePage,
    );
    expect(
      root.querySelector('nav[aria-label="Schedule pages"] a[href*="offset=50"]'),
    ).toBeTruthy();
  });

  it("opens the schedule run-now confirmation with ephemeral handoff copy", async () => {
    const { default: ScheduleOperationPage } = await import("@/pages/app/schedule-operation");
    const root = await mountRoute(
      "/admin/1/schedule/default/ops/primary/handoff",
      "/admin/{family}/schedule/{realm}/{area}/{resource}/{operation}",
      ScheduleOperationPage,
    );

    const runNowButton = Array.from(root.querySelectorAll("button")).find(
      (button) => button.textContent?.trim() === "Run now",
    );
    expect(runNowButton).toBeTruthy();
    click(runNowButton as HTMLButtonElement);
    const text = root.textContent ?? "";
    expect(text).toContain("Run schedule now?");
    expect(text).toContain("live matching subscriptions");
    expect(text).toContain("cron and the next run remain unchanged");
    expect(text).toContain("downstream job completion");
    cleanupApp(root);
  });

  it("keeps operation schedule status off the resource page", async () => {
    mocks.queryStates.scheduleResource = queryState.fresh(
      {
        ...scheduleResource,
        detail: {
          ...scheduleResource.detail,
          enabled: false,
        },
      },
      queryOptions(),
    );
    const { default: ScheduleResourcePage } = await import("@/pages/app/schedule-resource");

    const root = await mountRoute(
      "/admin/1/schedule/default/ops/primary",
      "/admin/{family}/schedule/{realm}/{area}/{resource}",
      ScheduleResourcePage,
    );
    expect(root.textContent).not.toContain("Disabled");
    expect(root.textContent).not.toContain("pending handoff");
    expect(root.textContent).not.toContain("Run now");
  });

  it("describes future, overdue, missing, and invalid schedule timestamps truthfully", async () => {
    const { formatScheduleTiming } = await import("@/features/schedule/schedule-format");
    const reference = Date.parse("2026-07-22T12:00:00Z");

    expect(formatScheduleTiming("2026-07-22T13:00:00Z", reference)).toBe("Next run in 1 hour");
    expect(formatScheduleTiming("2026-07-22T11:00:00Z", reference)).toBe(
      "Scheduled run was 1 hour ago",
    );
    expect(formatScheduleTiming(null, reference)).toBe("No next run scheduled");
    expect(formatScheduleTiming("not-a-timestamp", reference)).toBe("not-a-timestamp");
  });
  it("keeps Stream consumer progress unavailable without consumer positions", async () => {
    mocks.queryStates.inventory = queryState.fresh(inventory, queryOptions());

    const { default: StreamPage } = await import("@/pages/app/stream");
    const root = await mountRoute("/stream", "/stream", StreamPage);
    const text = root.textContent ?? "";

    expect(text).toContain("Stream inventory");
    expect(text).toContain("Realms");
    expect(text).toContain("Consumer health unavailable");
    expect(text).toContain("Committed");
    expect(text).toContain("Live subscriptions");
    expect(text).not.toContain("subscriber watermarks are behind");
    expect(root.querySelector(".domain-status-reason")).toBeNull();
    expect(root.querySelector('a[href="/admin/1/stream/default"]')).toBeTruthy();
  });
  it("does not label unavailable detail queries as live", async () => {
    const cases = [
      {
        key: "leaseResourceRows",
        module: () => import("@/pages/app/lease-resource"),
        path: "/admin/1/lease/default/ops/primary",
        routePath: "/admin/{family}/lease/{realm}/{area}/{resource}",
      },
      {
        key: "noticeResourceRows",
        module: () => import("@/pages/app/notice"),
        path: "/admin/1/notice/default/ops/primary",
        routePath: "/admin/{family}/notice/{realm}/{area}/{resource}",
      },
      {
        key: "noticeOperationRows",
        module: () => import("@/pages/app/notice-operation"),
        path: "/admin/1/notice/default/ops/primary/GetStatus",
        routePath: "/admin/{family}/notice/{realm}/{area}/{resource}/{operation}",
      },
      {
        key: "rpcResource",
        module: () => import("@/pages/app/rpc-resource"),
        path: "/admin/1/rpc/default/ops/primary",
        routePath: "/admin/{family}/rpc/{realm}/{area}/{resource}",
      },
      {
        key: "rpcOperation",
        module: () => import("@/pages/app/rpc-operation"),
        path: "/admin/1/rpc/default/ops/primary/GetStatus",
        routePath: "/admin/{family}/rpc/{realm}/{area}/{resource}/{operation}",
      },
      {
        key: "scheduleResource",
        module: () => import("@/pages/app/schedule-resource"),
        path: "/admin/1/schedule/default/ops/primary",
        routePath: "/admin/{family}/schedule/{realm}/{area}/{resource}",
      },
    ] as const;

    for (const page of cases) {
      resetQueries();
      mocks.queryStates[page.key] = queryState.error(
        new Error(`${page.key} unavailable`),
        undefined,
        queryOptions(),
      );
      const { default: Component } = await page.module();
      const root = await mountRoute(page.path, page.routePath, Component);
      const status = root.querySelector(".domain-header [role='status']")?.textContent ?? "";

      expect(status).toContain("Unavailable");
      expect(status).not.toContain("Live");

      cleanupApp(root);
      document.body.innerHTML = "";
    }
  });
  it("renders Stream hierarchy routes and committed resource records", async () => {
    const { default: StreamPage } = await import("@/pages/app/stream");
    const { default: StreamResourcePage } = await import("@/pages/app/stream-resource");

    let root = await mountRoute("/stream/default", "/stream/{realm}", StreamPage);
    let text = root.textContent ?? "";
    expect(text).toContain("Stream realm");
    expect(text).toContain("Areas");

    root = await mountRoute("/stream/default/ops", "/stream/{realm}/{area}", StreamPage);
    text = root.textContent ?? "";
    expect(text).toContain("Stream area");
    expect(text).toContain("Resource inventory");
    expect(text).toContain("stream://default/ops/primary");

    root = await mountRoute(
      "/admin/1/stream/default/ops/events?rows=1",
      "/admin/{family}/stream/{realm}/{area}/{resource}",
      StreamResourcePage,
    );
    text = root.textContent ?? "";
    expect(text).toContain("Stream resource");
    expect(text).toContain("From offset");
    expect(text).toContain("Stream state");
    expect(text).toContain("Active subscriptions");
    expect(text).toContain("Committed watermark");
    expect(text).not.toContain(
      "Live subscriptions; resets on disconnect cleanup or broker restart",
    );
    expect(text).not.toContain("stream://default/ops/events");
    expect(text).toContain('{"ok":true}');
    const recordsTable = root.querySelector('table[aria-label="Stream records"]');
    expect(recordsTable).toBeTruthy();
    expect(
      Array.from(recordsTable?.querySelectorAll('[data-slot="table-header-cell"]') ?? []).map(
        (header) => header.textContent?.trim(),
      ),
    ).toEqual(["Offset", "Body", "Action"]);
  });
  it("keeps RPC route health unavailable at the family scope", async () => {
    mocks.queryStates.inventory = queryState.fresh(inventory, queryOptions());

    const { default: RpcPage } = await import("@/pages/app/rpc");
    const root = await mountRoute("/rpc", "/rpc", RpcPage);
    const text = root.textContent ?? "";

    expect(text).toContain("RPC inventory");
    expect(text).toContain("Realms");
    expect(text).toContain("Route health unavailable");
    expect(text).not.toContain("not covered by a registered worker");
    expect(text).not.toContain("Pending work is in-memory");
    expect(text).not.toContain("pending requests");
    expect(text).toContain("Pending");
    expect(root.querySelector(".domain-status-reason")).toBeNull();
    expect(root.querySelector('a[href="/admin/1/rpc/default"]')).toBeTruthy();
  });
  it("renders RPC hierarchy routes and operation pages", async () => {
    const { default: RpcPage } = await import("@/pages/app/rpc");
    const { default: RpcResourcePage } = await import("@/pages/app/rpc-resource");
    const { default: RpcOperationPage } = await import("@/pages/app/rpc-operation");

    let root = await mountRoute("/rpc/default", "/rpc/{realm}", RpcPage);
    let text = root.textContent ?? "";
    expect(text).toContain("RPC realm");
    expect(text).toContain("Areas");

    root = await mountRoute("/rpc/default/ops", "/rpc/{realm}/{area}", RpcPage);
    text = root.textContent ?? "";
    expect(text).toContain("RPC area");
    expect(text).toContain("Resource inventory");
    expect(text).toContain("rpc://default/ops/primary");

    root = await mountRoute(
      "/admin/1/rpc/default/ops/primary",
      "/admin/{family}/rpc/{realm}/{area}/{resource}",
      RpcResourcePage,
    );
    text = root.textContent ?? "";
    expect(text).toContain("RPC resource");
    // Column labels match the inventory tier so the same metric reads the same way
    // on both sides of the drilldown.
    expect(text).toContain("Matching registrations");
    expect(text).toContain("Pending");
    expect(text).toContain("Handled");
    expect(text).toContain("Slowest avg ms");
    expect(text).not.toContain("Pending requests are in-memory state");
    expect(text).toContain("GetStatus");
    const operations = root.querySelector('[data-slot="table"][aria-label="RPC operations"]');
    expect(operations?.querySelectorAll('[data-slot="table-row"][data-row-key]')).toHaveLength(1);
    expect(root.querySelector("#rpc-operations-search")).toBeTruthy();
    expect(
      Array.from(operations?.querySelectorAll('th[data-priority="secondary"]') ?? []).map((cell) =>
        cell.getAttribute("data-column-id"),
      ),
    ).toEqual(["handled"]);

    root = await mountRoute(
      "/admin/1/rpc/default/ops/primary/GetStatus",
      "/admin/{family}/rpc/{realm}/{area}/{resource}/{operation}",
      RpcOperationPage,
    );
    text = root.textContent ?? "";
    expect(text).toContain("RPC operation");
    expect(text).toContain("Slowest worker average");
    expect(text).toContain("Under 25 ms");
    expect(text).toContain("Live call evidence");
    expect(text).toContain("worker-1");
    expect(text).toContain("Worker Registered");
    expect(text).toContain("Handled by exact live workers");
    const calls = root.querySelector(
      '[aria-labelledby="rpc-live-call-evidence"] [data-slot="table"]',
    );
    expect(calls).toBeTruthy();
    expect(calls?.textContent).toContain("worker-1");
  });
  it("does not repeat resource totals above route-level RPC rows", async () => {
    mocks.queryStates.rpcResource = queryState.fresh(
      {
        area: "ops",
        operations: [
          {
            averageLatencyMs: 12,
            operation: "GetStatus",
            pendingRequests: 150,
            requestsHandled: 12,
            workers: 2,
          },
          {
            averageLatencyMs: null,
            operation: "PendingOnly",
            pendingRequests: 1,
            requestsHandled: 0,
            workers: 0,
          },
        ],
        realm: "default",
        resource: "primary",
        totalPendingRequests: 151,
        totalWorkers: 2,
      },
      queryOptions(),
    );

    const { default: RpcResourcePage } = await import("@/pages/app/rpc-resource");
    const root = await mountRoute(
      "/admin/1/rpc/default/ops/primary",
      "/admin/{family}/rpc/{realm}/{area}/{resource}",
      RpcResourcePage,
    );

    expect(root.textContent).not.toContain("151");
    expect(root.textContent).toContain("150");
    expect(root.textContent).toContain("PendingOnly");
    expect(root.textContent).toContain("Handled by live workers");
  });
  it("does not double count wildcard RPC workers or invent handled totals", async () => {
    mocks.queryStates.rpcResource = queryState.fresh(
      {
        area: "ops",
        operations: [
          {
            averageLatencyMs: null,
            operation: "run",
            pendingRequests: 1,
            requestsHandled: null,
            workers: 1,
          },
          {
            averageLatencyMs: null,
            operation: "wait",
            pendingRequests: 1,
            requestsHandled: null,
            workers: 1,
          },
        ],
        realm: "default",
        resource: "primary",
        totalPendingRequests: 2,
        totalWorkers: 1,
      },
      queryOptions(),
    );

    const { default: RpcResourcePage } = await import("@/pages/app/rpc-resource");
    const root = await mountRoute(
      "/admin/1/rpc/default/ops/primary",
      "/admin/{family}/rpc/{realm}/{area}/{resource}",
      RpcResourcePage,
    );

    const table = root.querySelector('[data-slot="table"][aria-label="RPC operations"]');
    expect(table).toBeTruthy();
    expect(
      Array.from(
        table?.querySelectorAll('tbody td[data-column-id="route"] .domain-row-detail') ?? [],
      ).map((cell) => cell.textContent?.replace(/\s+/g, " ").trim()),
    ).toEqual(["Handled by live workers --", "Handled by live workers --"]);
    expect(root.querySelector(".domain-summary-strip")).toBeNull();
    expect(root.textContent).toContain("Matching registrations");
  });
});
