import { describe, expect, it } from "vite-plus/test";
import { cleanupApp } from "@askrjs/askr/boot";
import { queryState } from "@askrjs/askr/testing";
import { mountRoute, pageSmokeMocks, queryOptions } from "./page-smoke/harness";
import {
  activeSessions,
  emptyTopology,
  healthyGlobalDiagnostics,
  queueInventory,
  queueOverview,
  systemOverview,
  topologyAppLane,
  topologyOverview,
} from "./page-smoke/fixtures";

const mocks = pageSmokeMocks();

const currentActivityMetricNames = [
  "fitz_queue_messages_pending",
  "fitz_queue_inflight_active",
  "fitz_rpc_requests_pending",
  "fitz_lease_waiter_depth",
  "fitz_schedule_pending_fire_claims",
  "fitz_schedule_pending_ack_retries",
  "fitz_stream_append_sessions_active",
  "fitz_kv_transactions_active",
  "fitz_notice_subscriptions_active",
] as const;

function metricFamily(name: string, value: number, type = "gauge") {
  return {
    help: name,
    name,
    samples: [{ labels: {}, name, value }],
    type,
  };
}

function completeMetricsSnapshot(activityValue = 0, cumulativeFailureValue = 0) {
  const families = [
    ...currentActivityMetricNames.map((name, index) =>
      metricFamily(name, index === 0 ? activityValue : 0),
    ),
    metricFamily("fitz_queue_messages_dead_lettered", 0),
    metricFamily("fitz_rpc_request_timeouts_total", cumulativeFailureValue, "counter"),
  ];

  return {
    families,
    raw: families.map((family) => `${family.name} ${family.samples[0]?.value ?? 0}`).join("\n"),
  };
}

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
  it("does not promote cumulative domain counters to active overview issues", async () => {
    const { default: Home } = await import("@/pages/app/home");
    const cumulativeSystem = {
      ...systemOverview,
      diagnostics: healthyGlobalDiagnostics,
      domains: {
        ...systemOverview.domains,
        kv: {
          ...systemOverview.domains.kv,
          commitsFailedTotal: 8,
          invalidTransactionRejectsTotal: 3,
        },
        lease: {
          ...systemOverview.domains.lease,
          acquireTimeoutsTotal: 5,
          failureTotal: 4,
          waiterDepth: 0,
        },
        notice: {
          ...systemOverview.domains.notice,
          deliveryDropsTotal: 6,
          failureTotal: 2,
        },
        rpc: {
          ...systemOverview.domains.rpc,
          failureTotal: 7,
          requestTimeoutsTotal: 9,
        },
        schedule: {
          ...systemOverview.domains.schedule,
          ackFailuresTotal: 4,
          pendingFireClaims: 0,
        },
        stream: {
          ...systemOverview.domains.stream,
          appendConflictsTotal: 5,
          failureTotal: 3,
        },
      },
    };
    mocks.queryStates.system = queryState.fresh(cumulativeSystem, queryOptions());
    mocks.queryStates.topology = queryState.fresh(
      { ...emptyTopology, diagnostics: healthyGlobalDiagnostics },
      queryOptions(),
    );

    const root = await mountRoute("/admin/1", "/admin/{family}", Home);

    expect(root.textContent).toContain("No active issues");
    expect(root.textContent).not.toContain("RPC failures");
    expect(root.textContent).not.toContain("KV write pressure");
  });
  it("does not label a flowing lane with a historical pressure counter", async () => {
    const { default: Home } = await import("@/pages/app/home");
    mocks.queryStates.topology = queryState.fresh(
      {
        ...topologyOverview,
        diagnostics: healthyGlobalDiagnostics,
        lanes: [
          topologyAppLane("stream", "Stream", "flowing", [
            { key: "pressure", label: "Pressure", value: 9 },
          ]),
        ],
      },
      queryOptions(),
    );

    const root = await mountRoute("/admin/1", "/admin/{family}", Home);

    expect(root.textContent).toContain("1.00 act/sec");
    expect(root.textContent).not.toContain("Pressure 9");
  });
  it("marks overview health incomplete when a required source is unavailable", async () => {
    const { default: Home } = await import("@/pages/app/home");
    mocks.queryStates.system = queryState.error(
      new Error("system counters unavailable"),
      undefined,
      queryOptions(),
    );
    mocks.queryStates.topology = queryState.fresh(
      { ...emptyTopology, diagnostics: healthyGlobalDiagnostics },
      queryOptions(),
    );

    const root = await mountRoute("/admin/1", "/admin/{family}", Home);

    expect(root.textContent).toContain("Incomplete snapshot");
    expect(root.textContent).toContain("Issue status incomplete");
    expect(root.textContent).not.toContain("No active issues");
  });
  it("renders missing metrics as incomplete instead of zero", async () => {
    const { default: MetricsPage } = await import("@/pages/app/metrics");

    const root = await mountRoute("/admin/1/metrics", "/admin/{family}/metrics", MetricsPage);

    expect(root.textContent).toContain("Live state");
    expect(root.textContent).toContain("No known summary metrics");
    expect(root.textContent).not.toContain("Broker snapshot");
    expect(root.textContent).toContain("Incomplete");
    expect(root.textContent).not.toContain("Missing telemetry is not treated as zero");
    expect(root.textContent).not.toContain("Uptime0seconds");
    expect(root.textContent).toContain("Metric samples");
    expect(root.textContent).toContain("Showing 3 of 3 samples");
    const payloadTrigger = Array.from(root.querySelectorAll("button")).find(
      (button) => button.textContent === "View structured payload",
    );
    expect(payloadTrigger?.getAttribute("aria-expanded")).toBe("false");
    expect(root.querySelector(".resource-raw")).toBeNull();

    payloadTrigger?.click();
    await new Promise<void>((resolve) => queueMicrotask(() => resolve()));
    expect(payloadTrigger?.getAttribute("aria-expanded")).toBe("true");
    expect(root.querySelector(".resource-raw")).toBeTruthy();

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
      expect(window.location.search).toBe("?q=queue");

      const clearShortcut = Array.from(root.querySelectorAll("button")).find(
        (button) => button.textContent === "Clear filters",
      ) as HTMLButtonElement | undefined;

      expect(clearShortcut).toBeTruthy();
      clearShortcut?.click();
      await new Promise<void>((resolve) => queueMicrotask(() => resolve()));

      expect(root.textContent).toContain("Showing 3 of 3 samples");
      expect(window.location.search).toBe("");
    }
  });
  it("uses the metrics query parameter as the filter source", async () => {
    const { default: MetricsPage } = await import("@/pages/app/metrics");

    const root = await mountRoute("/admin/1/metrics?q=rpc", "/admin/{family}/metrics", MetricsPage);
    const filter = root.querySelector(
      'input[aria-label="Filter metrics"]',
    ) as HTMLInputElement | null;

    expect(filter?.value).toBe("rpc");
    expect(root.textContent).toContain("Showing 1 of 3 samples");
  });
  it("labels current work as activity while keeping cumulative failures historical", async () => {
    const { default: MetricsPage } = await import("@/pages/app/metrics");
    mocks.queryStates.metrics = queryState.fresh(completeMetricsSnapshot(4, 9), queryOptions());

    const root = await mountRoute("/admin/1/metrics", "/admin/{family}/metrics", MetricsPage);

    expect(root.textContent).toContain("Active");
    expect(root.textContent).not.toContain("Activity alone does not establish pressure");
    expect(root.textContent).toContain("Failures (1)");
    expect(root.textContent).not.toContain("Attention");
  });
  it("reports quiet when all required current gauges are observed at zero", async () => {
    const { default: MetricsPage } = await import("@/pages/app/metrics");
    mocks.queryStates.metrics = queryState.fresh(completeMetricsSnapshot(0, 9), queryOptions());

    const root = await mountRoute("/admin/1/metrics", "/admin/{family}/metrics", MetricsPage);

    expect(root.textContent).toContain("Quiet");
    expect(root.textContent).not.toContain("Cumulative counters below remain historical");
    expect(root.textContent).not.toContain("Attention");
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
    expect(root.textContent).toContain("Active");
    expect(root.textContent).toContain("Sessions");
    expect(root.textContent).toContain("Route families");
    expect(root.textContent).toContain("Transports");
    expect(root.textContent).toContain("Longest idle");
    expect(root.textContent).toContain("session-1");
    expect(root.textContent).toContain("2001:db8::1ff:fe23:4567:890a");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.activeSessions = queryState.fresh(
      {
        sessions: [
          {
            ...activeSessions.sessions[0],
            identityClaim: undefined,
            identityValue: undefined,
            idleSeconds: undefined,
            messagesReceived: undefined,
            messagesSent: undefined,
            subject: undefined,
          },
        ],
      },
      queryOptions(),
    );
    root = await mountRoute("/sessions", "/sessions", SessionsPage);

    expect(root.textContent).toContain("Unknown");
    expect(root.textContent).not.toContain("Unauthenticated");
    expect(root.textContent).not.toContain("Not resolved");
    expect(root.textContent).not.toContain("Attention");

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
    mocks.queryStates.currentSession = queryState.fresh({ username: "root" }, queryOptions());

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
    expect(root.textContent).toContain("Realms");
    expect(root.textContent).not.toContain("messages are visible");
    expect(root.textContent).not.toContain("Activity alone does not establish pressure");
    expect(root.querySelector('a[href="/admin/1/queue/default"]')).toBeTruthy();
  });
});
