import { describe, expect, it } from "vite-plus/test";
import { cleanupApp } from "@askrjs/askr/boot";
import { queryState } from "@askrjs/askr/testing";
import { mountRoute, pageSmokeMocks, queryOptions } from "./page-smoke/harness";
import {
  emptyTopology,
  healthyGlobalDiagnostics,
  queueInventory,
  queueOverview,
  systemOverview,
  topologyAppLane,
  topologyOverview,
} from "./page-smoke/fixtures";

const mocks = pageSmokeMocks();

describe("admin page smoke tests", () => {
  it("renders compact domain entry points when no lanes are visible", async () => {
    const { default: Home } = await import("@/pages/app/home");

    mocks.queryStates.topology = queryState.fresh(emptyTopology, queryOptions());

    const root = await mountRoute("/", "/", Home);
    const text = root.textContent ?? "";

    expect(text).toContain("Fitz status");
    expect(text).toContain("Domain health");
    expect(text).toContain("Broker vitals");
    expect(text).toContain("Stream");
    expect(text).toContain("Queue");
    expect(text).not.toContain("No domain lanes are visible yet");
    expect(text).not.toContain("Domain workspaces");
  });
  it("does not promote caught-up Stream signals to issues", async () => {
    const { default: Home } = await import("@/pages/app/home");
    const healthySystem = {
      ...systemOverview,
      diagnostics: healthyGlobalDiagnostics,
      domains: {
        ...systemOverview.domains,
        kv: {
          ...systemOverview.domains.kv,
          commitsFailedTotal: 0,
          invalidTransactionRejectsTotal: 0,
        },
        schedule: {
          ...systemOverview.domains.schedule,
          pendingFireClaims: 0,
        },
        stream: {
          ...systemOverview.domains.stream,
          appendConflictsTotal: 0,
          failureTotal: 0,
          notifyDropsTotal: 0,
        },
      },
    };
    const benignTopology = {
      ...topologyOverview,
      diagnostics: healthyGlobalDiagnostics,
      lanes: [
        topologyAppLane("kv", "KV", "quiet", []),
        topologyAppLane("stream", "Stream", "pressure", [
          { key: "events", label: "Events", value: 1224 },
        ]),
      ],
    };

    mocks.queryStates.system = queryState.fresh(healthySystem, queryOptions());
    mocks.queryStates.topology = queryState.fresh(benignTopology, queryOptions());

    const root = await mountRoute("/", "/", Home);
    const text = root.textContent ?? "";

    expect(text).toContain("No active issues");
    expect(text).toContain("Events 1,224");
    expect(text).not.toContain("KV write pressure");
    expect(text).not.toContain("Stream pressure");
    expect(text).not.toContain("stream latency");
  });
  it("renders a metrics posture summary and empty search state", async () => {
    const { default: MetricsPage } = await import("@/pages/app/metrics");

    const root = await mountRoute("/admin/metrics", "/admin/metrics", MetricsPage);

    expect(root.textContent).toContain("Live state");
    expect(root.textContent).toContain("Broker snapshot");
    expect(root.textContent).toContain("Quiet");
    expect(root.textContent).toContain("No backlog, contention, or failure pressure detected");
    expect(root.textContent).toContain("Metric samples");
    expect(root.textContent).toContain("Showing 3 of 3 samples");

    const filter = root.querySelector(
      'input[aria-label="Filter metrics"]',
    ) as HTMLInputElement | null;
    expect(filter).toBeTruthy();

    if (filter) {
      const queueShortcut = Array.from(root.querySelectorAll("button")).find((button) =>
        button.textContent?.startsWith("Queue "),
      ) as HTMLButtonElement | undefined;

      expect(queueShortcut).toBeTruthy();
      queueShortcut?.click();
      await new Promise<void>((resolve) => queueMicrotask(() => resolve()));

      expect(root.textContent).toContain("Showing 1 of 3 samples");

      const clearShortcut = Array.from(root.querySelectorAll("button")).find(
        (button) => button.textContent === "Clear filters",
      ) as HTMLButtonElement | undefined;

      expect(clearShortcut).toBeTruthy();
      clearShortcut?.click();
      await new Promise<void>((resolve) => queueMicrotask(() => resolve()));

      expect(root.textContent).toContain("Showing 3 of 3 samples");
    }
  });
  it("renders metrics loading and error states", async () => {
    const { default: MetricsPage } = await import("@/pages/app/metrics");

    mocks.queryStates.metrics = queryState.loading(queryOptions());
    let root = await mountRoute("/admin/metrics", "/admin/metrics", MetricsPage);
    expect(root.textContent).toContain("Loading metrics snapshot");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.metrics = queryState.error(
      new Error("metrics endpoint unavailable"),
      undefined,
      queryOptions(),
    );
    root = await mountRoute("/admin/metrics", "/admin/metrics", MetricsPage);
    expect(root.textContent).toContain("Unable to load metrics snapshot");
    expect(root.textContent).toContain("metrics endpoint unavailable");
  });
  it("renders a sessions posture summary and empty state", async () => {
    const { default: SessionsPage } = await import("@/pages/app/sessions");

    let root = await mountRoute("/sessions", "/sessions", SessionsPage);

    expect(root.textContent).toContain("Session summary");
    expect(root.textContent).toContain("Healthy");
    expect(root.textContent).toContain("Sessions");
    expect(root.textContent).toContain("Route families");
    expect(root.textContent).toContain("Transports");
    expect(root.textContent).toContain("Idle risk");
    expect(root.textContent).toContain("session-1");
    expect(root.textContent).toContain("2001:db8::1ff:fe23:4567:890a");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.activeSessions = queryState.fresh({ sessions: [] }, queryOptions());
    root = await mountRoute("/sessions", "/sessions", SessionsPage);

    expect(root.textContent).toContain("No active sessions");
    expect(root.textContent).toContain("No live broker or admin sessions are currently connected");
  });
  it("renders sessions loading and error states", async () => {
    const { default: SessionsPage } = await import("@/pages/app/sessions");

    mocks.queryStates.activeSessions = queryState.loading(queryOptions());
    let root = await mountRoute("/sessions", "/sessions", SessionsPage);
    expect(root.textContent).toContain("Loading active sessions");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.activeSessions = queryState.error(
      new Error("session endpoint unavailable"),
      undefined,
      queryOptions(),
    );
    root = await mountRoute("/sessions", "/sessions", SessionsPage);
    expect(root.textContent).toContain("Unable to load active sessions");
    expect(root.textContent).toContain("session endpoint unavailable");
  });
  it("mounts the dashboard loading and error states", async () => {
    const { default: Home } = await import("@/pages/app/home");
    mocks.queryStates.currentSession = queryState.loading(queryOptions());

    let root = await mountRoute("/", "/", Home);
    expect(root.querySelectorAll("main#main-content")).toHaveLength(1);
    expect(root.textContent).toContain("Loading admin dashboard");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.currentSession = queryState.error(
      new Error("Session lookup failed"),
      undefined,
      queryOptions(),
    );

    root = await mountRoute("/", "/", Home);
    expect(root.querySelectorAll("main#main-content")).toHaveLength(1);
    expect(root.textContent).toContain("Session lookup failed");

    cleanupApp(root);
    document.body.innerHTML = "";
  });
  it("mounts queue loading, error, and empty states", async () => {
    mocks.queryStates.currentSession = queryState.fresh({ username: "admin" }, queryOptions());

    const { default: QueuePage } = await import("@/pages/app/queue");

    mocks.queryStates.queueInventory = queryState.loading(queryOptions());
    let root = await mountRoute("/queue", "/queue", QueuePage);
    expect(root.textContent).toContain("Loading queue inventory");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.queueInventory = queryState.error(
      new Error("Queue inventory unavailable"),
      undefined,
      queryOptions(),
    );
    root = await mountRoute("/queue", "/queue", QueuePage);
    expect(root.textContent).toContain("Queue inventory unavailable");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.queueInventory = queryState.fresh(
      {
        ...queueInventory,
        realms: [],
      },
      queryOptions(),
    );
    root = await mountRoute("/queue", "/queue", QueuePage);
    expect(root.textContent).toContain("No queue resources are currently visible");
  });
  it("keeps queue inventory content visible while refresh is in flight", async () => {
    const { default: QueuePage } = await import("@/pages/app/queue");

    mocks.queryStates.queue = queryState.refreshing(queueOverview, queryOptions());

    const root = await mountRoute("/queue", "/queue", QueuePage);

    expect(root.textContent).toContain("Refreshing");
    expect(root.textContent).toContain("Queue inventory");
    expect(root.textContent).toContain("Resource inventory");
    expect(root.textContent).toContain("queue://default/ops/primary");
    expect(root.textContent).toContain("message(s) are visible");
    expect(root.querySelector('a[href="/admin/1/queue/default/ops/primary"]')).toBeTruthy();
  });
});
