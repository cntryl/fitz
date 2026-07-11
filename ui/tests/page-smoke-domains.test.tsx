import { describe, expect, it } from "vite-plus/test";
import { cleanupApp } from "@askrjs/askr/boot";
import { queryState } from "@askrjs/askr/testing";
import { mountRoute, pageSmokeMocks, queryOptions, resetQueries } from "./page-smoke/harness";
import {
  diagnostics,
  domainOverviews,
  inventory,
  leaseOverview,
  noticeOperationRows,
  noticeOverview,
  noticeResourceRows,
  queueInventory,
  rpcOverview,
  scheduleOverview,
  streamOverview,
  systemOverview,
  topologyOverview,
} from "./page-smoke/fixtures";

const mocks = pageSmokeMocks();

describe("admin page smoke tests", () => {
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
  it("renders lease health in the inventory header", async () => {
    mocks.queryStates.lease = queryState.fresh(
      {
        ...leaseOverview,
        stats: {
          ...leaseOverview.stats,
          acquireTimeoutsTotal: 2,
          forcedReleasesTotal: 1,
          invalidTokenRejectsTotal: 3,
          waiterDepth: 4,
          oldestLeaseAgeSeconds: 3700,
          leasesActive: 12,
        },
      },
      queryOptions(),
    );

    const { default: LeasePage } = await import("@/pages/app/lease");
    const root = await mountRoute("/lease", "/lease", LeasePage);
    const text = root.textContent ?? "";

    expect(text).toContain("Lease inventory");
    expect(text).toContain("Resource inventory");
    expect(text).toContain("lease://default/ops/primary");
    expect(text).toContain("1h");
    expect(text).toContain("Attention");
    expect(text).toContain("acquire timeout");
    expect(text).toContain("Ephemeral ownership");
    expect(text).not.toContain("Broker-local owners");
    expect(root.querySelector('a[href="/admin/1/lease/default/ops/primary"]')).toBeTruthy();
  });
  it("renders kv tables with inventory stats and explorer links", async () => {
    const { default: KvPage } = await import("@/pages/app/kv");
    const root = await mountRoute("/kv", "/kv", KvPage);
    const text = root.textContent ?? "";
    const labels = [
      "Route",
      "Records",
      "Storage",
      "Txns",
      "Read p95 ms",
      "Write p95 ms",
      "Domain keys",
      "Domain txns",
      "Ops / sec",
      "Failures",
    ];

    let cursor = -1;
    for (const label of labels) {
      const index = text.indexOf(label, cursor + 1);
      expect(index).toBeGreaterThan(cursor);
      cursor = index;
    }

    expect(text).toContain("KV tables");
    expect(text).toContain("kv://default/ops/primary");
    expect(text).toContain("300");
    expect(text).toContain("16.0 KiB");
    expect(text).toContain("12.5");
    expect(text).toContain("18.3");
    expect(text).not.toContain("2.4 / 12.5");
    expect(text).not.toContain("4.1 / 18.3");
    expect(root.querySelector('a[href="/admin/1/kv/default/ops/primary"]')).not.toBeNull();
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
    const initialRemaining = root
      .querySelector("tbody tr")
      ?.querySelectorAll("td")[5]
      ?.textContent?.trim();
    expect(initialRemaining).toBeTruthy();
    expect(root.textContent).toContain("not crash-safe continuity");
  });
  it("renders notice health in the inventory header", async () => {
    mocks.queryStates.notice = queryState.fresh(
      {
        ...noticeOverview,
        stats: {
          ...noticeOverview.stats,
          subscriptionsActive: 14,
          publishesPerSecond: 18.5,
          deliveryDropsTotal: 2,
          wildcardLimitRejectsTotal: 1,
          routesActive: 4,
          maxRouteSubscribers: 9,
        },
      },
      queryOptions(),
    );

    const { default: NoticePage } = await import("@/pages/app/notice");
    const root = await mountRoute("/notice", "/notice", NoticePage);
    const text = root.textContent ?? "";

    expect(text).toContain("Notice inventory");
    expect(text).toContain("Resource inventory");
    expect(text).toContain("notice://default/ops/primary");
    expect(text).toContain("Attention");
    expect(text).toContain("2 delivery drop");
    expect(text).toContain("1 wildcard reject");
    expect(text).toContain("live fanout");
    expect(text).not.toContain("Communication flow");
    expect(root.querySelector('a[href="/admin/1/notice/default/ops/primary"]')).toBeTruthy();
  });
  it("renders notice operation metrics and delivery evidence", async () => {
    const { default: NoticeOperationPage } = await import("@/pages/app/notice-operation");

    const root = await mountRoute(
      "/notice/default/ops/primary/GetStatus",
      "/notice/{realm}/{area}/{resource}/{operation}",
      NoticeOperationPage,
    );
    const text = root.textContent ?? "";

    expect(text).toContain("Notice operation");
    expect(text).toContain("GetStatus");
    expect(text).toContain("Latency unavailable via current API");
    expect(text).toContain("Active subscribers");
    expect(text).toContain("Rolling messages / min");
    expect(text).toContain("Status");
    expect(text).toContain("Notifications received");
    expect(text).toContain("Publishes / min");
    expect(text).toContain("Publishes total");
    expect(text).toContain("session-1");
    expect(text).toContain("session-2");
  });
  it("renders schedule health in the inventory header", async () => {
    mocks.queryStates.schedule = queryState.fresh(
      {
        ...scheduleOverview,
        stats: {
          ...scheduleOverview.stats,
          pendingFireClaims: 7,
          ackFailuresTotal: 2,
          notifyFailuresTotal: 1,
          createPersistenceFailuresTotal: 3,
          upsertPersistenceFailuresTotal: 1,
          cancelPersistenceFailuresTotal: 0,
          subscriptionsActive: 9,
          schedulesActive: 11,
          executionsPerMinute: 8.25,
        },
      },
      queryOptions(),
    );

    const { default: SchedulePage } = await import("@/pages/app/schedule");
    const root = await mountRoute("/schedule", "/schedule", SchedulePage);
    const text = root.textContent ?? "";

    expect(text).toContain("Schedule inventory");
    expect(text).toContain("Resource inventory");
    expect(text).toContain("schedule://default/ops/primary");
    expect(text).toContain("Attention");
    expect(text).toContain("Schedule does not imply durable downstream delivery.");
    expect(text).toContain("Persistence and handoff failure counters need attention.");
    expect(text).not.toContain("Schedule realms");
    expect(root.querySelector('a[href="/admin/1/schedule/default/ops/primary"]')).toBeTruthy();
  });
  it("renders schedule hierarchy routes and resource drill-down pages", async () => {
    const { default: SchedulePage } = await import("@/pages/app/schedule");
    const realmRoot = await mountRoute("/schedule/default", "/schedule/{realm}", SchedulePage);
    expect(realmRoot.textContent).toContain("Schedule inventory");
    expect(realmRoot.textContent).toContain("Resource inventory");
    expect(realmRoot.textContent).toContain("schedule://default/ops/primary");
    cleanupApp(realmRoot);
    document.body.innerHTML = "";

    const areaRoot = await mountRoute(
      "/schedule/default/ops",
      "/schedule/{realm}/{area}",
      SchedulePage,
    );
    expect(areaRoot.textContent).toContain("Schedule inventory");
    expect(areaRoot.textContent).toContain("Resource inventory");
    expect(areaRoot.textContent).toContain("schedule://default/ops/primary");
    cleanupApp(areaRoot);
    document.body.innerHTML = "";

    const { default: ScheduleResourcePage } = await import("@/pages/app/schedule-resource");
    const resourceRoot = await mountRoute(
      "/admin/1/schedule/default/ops/primary",
      "/admin/{family}/schedule/{realm}/{area}/{resource}",
      ScheduleResourcePage,
    );
    const text = resourceRoot.textContent ?? "";

    expect(text).toContain("Schedule resource inspection");
    expect(text).toContain("Is anyone listening?");
    expect(text).toContain("Next run");
    expect(text).toContain("Broker-observed, non-authoritative counter");
    expect(text).toContain("Execution observations");
    expect(text).toContain("Pending and missed handoffs");
    expect(text).toContain("handoff");
  });
  it("renders stream health in the inventory header", async () => {
    mocks.queryStates.stream = queryState.fresh(
      {
        ...streamOverview,
        stats: {
          ...streamOverview.stats,
          eventsTotal: 4200,
          streamsActive: 7,
          subscriptionsActive: 5,
          watermarkLagBuckets: {
            caughtUp: 6,
            over100: 2,
            under10: 1,
            under100: 3,
          },
        },
      },
      queryOptions(),
    );

    const { default: StreamPage } = await import("@/pages/app/stream");
    const root = await mountRoute("/stream", "/stream", StreamPage);
    const text = root.textContent ?? "";

    expect(text).toContain("Stream inventory");
    expect(text).toContain("Resource inventory");
    expect(text).toContain("stream://default/ops/primary");
    expect(text).toContain("100+ behind");
    expect(text).toContain("Attention");
    expect(text).toContain("live subscriptions");
    expect(text).not.toContain("Stream metrics");
    expect(root.querySelector('a[href="/admin/1/stream/default/ops/primary"]')).toBeTruthy();
  });
  it("renders Stream hierarchy routes and committed resource records", async () => {
    const { default: StreamPage } = await import("@/pages/app/stream");
    const { default: StreamResourcePage } = await import("@/pages/app/stream-resource");

    let root = await mountRoute("/stream/default", "/stream/{realm}", StreamPage);
    let text = root.textContent ?? "";
    expect(text).toContain("Stream inventory");
    expect(text).toContain("Resource inventory");
    expect(text).toContain("stream://default/ops/primary");

    root = await mountRoute("/stream/default/ops", "/stream/{realm}/{area}", StreamPage);
    text = root.textContent ?? "";
    expect(text).toContain("Stream inventory");
    expect(text).toContain("Resource inventory");
    expect(text).toContain("stream://default/ops/primary");

    root = await mountRoute(
      "/admin/1/stream/default/ops/events",
      "/admin/{family}/stream/{realm}/{area}/{resource}",
      StreamResourcePage,
    );
    text = root.textContent ?? "";
    expect(text).toContain("Stream resource");
    expect(text).toContain("From offset");
    expect(text).toContain("Stream resource metrics");
    expect(text).toContain("stream://default/ops/events");
    expect(text).toContain('{"ok":true}');
  });
  it("renders rpc health in the inventory header", async () => {
    mocks.queryStates.rpc = queryState.fresh(
      {
        ...rpcOverview,
        stats: {
          ...rpcOverview.stats,
          requestsPending: 6,
          workersRegistered: 2,
          pendingRoutesActive: 3,
          requestTimeoutsTotal: 2,
          failureTotal: 1,
        },
      },
      queryOptions(),
    );

    const { default: RpcPage } = await import("@/pages/app/rpc");
    const root = await mountRoute("/rpc", "/rpc", RpcPage);
    const text = root.textContent ?? "";

    expect(text).toContain("RPC inventory");
    expect(text).toContain("Resource inventory");
    expect(text).toContain("rpc://default/ops/primary");
    expect(text).toContain("Attention");
    expect(text).toContain("Response reliability");
    expect(text).toContain("Pending work is in-memory");
    expect(text).toContain("pending request(s)");
    expect(text).not.toContain("Communication flow");
    expect(root.querySelector('a[href="/admin/1/rpc/default/ops/primary"]')).toBeTruthy();
  });
  it("renders RPC hierarchy routes and operation pages", async () => {
    const { default: RpcPage } = await import("@/pages/app/rpc");
    const { default: RpcResourcePage } = await import("@/pages/app/rpc-resource");
    const { default: RpcOperationPage } = await import("@/pages/app/rpc-operation");

    let root = await mountRoute("/rpc/default", "/rpc/{realm}", RpcPage);
    let text = root.textContent ?? "";
    expect(text).toContain("RPC inventory");
    expect(text).toContain("Resource inventory");
    expect(text).toContain("rpc://default/ops/primary");

    root = await mountRoute("/rpc/default/ops", "/rpc/{realm}/{area}", RpcPage);
    text = root.textContent ?? "";
    expect(text).toContain("RPC inventory");
    expect(text).toContain("Resource inventory");
    expect(text).toContain("rpc://default/ops/primary");

    root = await mountRoute(
      "/admin/1/rpc/default/ops/primary",
      "/admin/{family}/rpc/{realm}/{area}/{resource}",
      RpcResourcePage,
    );
    text = root.textContent ?? "";
    expect(text).toContain("RPC resource");
    expect(text).toContain("Workers");
    expect(text).toContain("Pending requests");
    expect(text).toContain("Requests handled");
    expect(text).toContain("in-memory pending request evidence");
    expect(text).toContain("GetStatus");

    root = await mountRoute(
      "/admin/1/rpc/default/ops/primary/GetStatus",
      "/admin/{family}/rpc/{realm}/{area}/{resource}/{operation}",
      RpcOperationPage,
    );
    text = root.textContent ?? "";
    expect(text).toContain("RPC operation");
    expect(text).toContain("Slowest average latency");
    expect(text).toContain("Latency <25ms");
    expect(text).toContain("Live call evidence");
    expect(text).toContain("worker-1");
  });
  it("renders the status-first dashboard sections", async () => {
    const { default: Home } = await import("@/pages/app/home");

    const root = await mountRoute("/", "/", Home);
    const text = root.textContent ?? "";

    expect(text).toContain("Fitz status");
    expect(text).toContain("Current status");
    expect(text).toContain("Issues");
    expect(text).toContain("Actionable signals only");
    expect(text).toContain("Queue blocked");
    expect(text).toContain("Schedule pending claims");
    expect(text).toContain("Open Queue");
    expect(text).toContain("Domain health");
    expect(text).toContain("Broker vitals");
    expect(text).toContain("Router pressure");
    expect(text).not.toContain("Messaging flow");
    expect(text).not.toContain("Flow inspector");

    const orderedSections = ["Current status", "Issues", "Domain health", "Broker vitals"];
    let cursor = -1;
    for (const section of orderedSections) {
      const index = text.indexOf(section, cursor + 1);
      expect(index).toBeGreaterThan(cursor);
      cursor = index;
    }
  });
  it("renders diagnostics as the infrastructure-internals console", async () => {
    const diagnosticsWithSuggestion = {
      ...diagnostics,
      incident_summary: {
        ...diagnostics.incident_summary,
        confidence: 0.82,
        explanation: "Queue backlog is increasing while RPC workers are saturated.",
        recommended_next_query: "Inspect queues",
        severity: "medium",
        status: "degraded",
        suggested_next_queries: [
          {
            endpoint: "/api/v1/queue/stats",
            priority: 1,
            rationale: "Queue backlog is the top broker-visible pressure signal.",
            remediation: "Open Queue and inspect ready, inflight, and DLQ pressure.",
            title: "Inspect queue pressure",
          },
        ],
        title: "Queue pressure",
      },
    };

    mocks.queryStates.system = queryState.fresh(
      {
        ...systemOverview,
        diagnostics: diagnosticsWithSuggestion,
      },
      queryOptions(),
    );
    mocks.queryStates.topology = queryState.fresh(
      {
        ...topologyOverview,
        diagnostics: diagnosticsWithSuggestion,
      },
      queryOptions(),
    );

    const { default: DiagnosticsPage } = await import("@/pages/app/diagnostics");
    const root = await mountRoute("/diagnostics", "/diagnostics", DiagnosticsPage);
    const text = root.textContent ?? "";

    expect(text).toContain("Diagnostics console");
    expect(text).toContain("Infrastructure signals");
    expect(text).toContain("Domain internals");
    expect(text).toContain("Advanced operational views");
    expect(text).toContain("Structured metrics");
    expect(text).toContain("Storage health");
    expect(text).toContain("Not exposed");
    expect(text).toContain("Hotspots");
    expect(text).toContain("Suggested queries");
    expect(text).toContain("Inspect queue pressure");
    expect(text).toContain("/api/v1/queue/stats");
    expect(text).toContain("Metric families");
    expect(text).toContain("fitz_rpc_latency_histogram");
  });
  it("keeps dashboard behavior visible while refresh is in flight", async () => {
    const { default: Home } = await import("@/pages/app/home");

    mocks.queryStates.topology = queryState.refreshing(topologyOverview, queryOptions());

    const root = await mountRoute("/", "/", Home);
    const text = root.textContent ?? "";

    expect(text).toContain("Refreshing");
    expect(text).toContain("Issues");
    expect(text).toContain("Domain health");
    expect(text).toContain("Broker vitals");
    expect(text).toContain("Queue");
  });
});
