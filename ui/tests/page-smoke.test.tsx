import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { cleanupApp, createSPA } from "@askrjs/askr/boot";
import type { Query } from "@askrjs/askr/data";
import type { RouteHandler } from "@askrjs/askr/router";
import { queryState } from "@askrjs/askr/testing";
import { dashboardDiagnostics as diagnostics, topologyOverview } from "./fixtures/topology";

type MutationState = {
  abort: ReturnType<typeof vi.fn>;
  error: Error | null;
  execute: ReturnType<typeof vi.fn>;
  pending: boolean;
  reset: ReturnType<typeof vi.fn>;
  result: boolean | null;
  status: "idle" | "pending" | "success" | "error";
};

const mocks = vi.hoisted(() => {
  const refresh = vi.fn(async () => undefined);
  const mutation: MutationState = {
    abort: vi.fn(),
    error: null,
    execute: vi.fn(async () => true),
    pending: false,
    reset: vi.fn(),
    result: null,
    status: "idle",
  };

  return {
    queryStates: {} as Record<string, Query<{}>>,
    refresh,
    mutation,
  };
});

function queryOptions() {
  return { refresh: mocks.refresh };
}

vi.mock("@/features/session/session-query", () => ({
  createActiveSessionsQuery: () => mocks.queryStates.activeSessions,
  createCurrentSessionQuery: () => mocks.queryStates.currentSession,
}));

vi.mock("@/features/session/session-mutation", () => ({
  createSignInMutation: () => mocks.mutation,
  createSignOutMutation: () => mocks.mutation,
}));

vi.mock("@/features/system/system-query", () => ({
  createSystemOverviewQuery: () => mocks.queryStates.system,
}));

vi.mock("@/features/topology/topology-query", () => ({
  createMessagingTopologyQuery: () => mocks.queryStates.topology,
}));

vi.mock("@/features/metrics/metrics-query", () => ({
  createMetricsOverviewQuery: () => mocks.queryStates.metrics,
}));

vi.mock("@/features/queue/queue-query", () => ({
  createQueueDeadLettersQuery: () => mocks.queryStates.queueDeadLetters,
  createQueueOverviewQuery: () => mocks.queryStates.queue,
}));

vi.mock("@/features/queue/queue-resource-query", () => ({
  createQueueResourceComparisonQuery: () => mocks.queryStates.queueComparison,
  createQueueResourceQuery: () => mocks.queryStates.queueResource,
  createQueueResourceTimelineQuery: () => mocks.queryStates.queueTimeline,
}));

vi.mock("@/features/queue/queue-actions", () => ({
  createPurgeQueueDeadLetterMutation: () => mocks.mutation,
  createReplayQueueDeadLetterMutation: () => mocks.mutation,
}));

vi.mock("@/features/kv/kv-query", () => ({
  createKvOverviewQuery: () => mocks.queryStates.kv,
}));

vi.mock("@/features/lease/lease-query", () => ({
  createLeaseOverviewQuery: () => mocks.queryStates.lease,
}));

vi.mock("@/features/notice/notice-query", () => ({
  createNoticeOverviewQuery: () => mocks.queryStates.notice,
}));

vi.mock("@/features/rpc/rpc-query", () => ({
  createRpcOverviewQuery: () => mocks.queryStates.rpc,
}));

vi.mock("@/features/schedule/schedule-query", () => ({
  createScheduleOverviewQuery: () => mocks.queryStates.schedule,
}));

vi.mock("@/features/stream/stream-query", () => ({
  createStreamOverviewQuery: () => mocks.queryStates.stream,
}));

vi.mock("@/features/resource/resource-query", () => ({
  createResourceInventoryQuery: () => mocks.queryStates.inventory,
  createResourceQuery: () => mocks.queryStates.resource,
}));

const realm = { realm: "default" };

const inventory = {
  domain: "kv",
  realms: [
    {
      areas: [
        {
          area: "ops",
          resources: ["primary"],
        },
      ],
      realm: "default",
    },
  ],
};

const queueOverview = {
  realms: [realm],
  stats: {
    inflightActive: 2,
    messagesDeadLettered: 0,
    messagesDelayed: 1,
    messagesPending: 3,
    messagesReady: 4,
    operationsPerSecond: 1.25,
  },
};

const kvOverview = {
  realms: [realm],
  stats: {
    commitsFailedTotal: 0,
    invalidTransactionRejectsTotal: 0,
    keysTotal: 12,
    operationsPerSecond: 2.5,
    rollbacksTotal: 0,
    transactionsActive: 1,
  },
};

const leaseOverview = {
  realms: [realm],
  stats: {
    acquireTimeoutsTotal: 0,
    forcedReleasesTotal: 0,
    invalidTokenRejectsTotal: 0,
    leasesActive: 3,
    oldestLeaseAgeSeconds: 42,
    operationsPerSecond: 1.5,
    waiterDepth: 0,
  },
};

const noticeOverview = {
  realms: [realm],
  stats: {
    publishesPerSecond: 0.75,
    deliveryDropsTotal: 0,
    routesActive: 0,
    wildcardLimitRejectsTotal: 0,
    subscriptionsActive: 7,
    maxRouteSubscribers: 0,
  },
};

const rpcOverview = {
  realms: [realm],
  stats: {
    invalidSequenceErrorsDroppedTotal: 0,
    invalidSequenceErrorsForwardedTotal: 0,
    invalidSequenceResponsesTotal: 0,
    operationsPerSecond: 2,
    pendingRoutesActive: 1,
    requestsPending: 1,
    requestTimeoutsTotal: 0,
    responsesDroppedClosedCallerTotal: 0,
    responsesMissingPendingTotal: 0,
    workersRegistered: 4,
  },
};

const scheduleOverview = {
  realms: [realm],
  stats: {
    ackFailuresTotal: 0,
    cancelPersistenceFailuresTotal: 0,
    createPersistenceFailuresTotal: 0,
    executionsPerMinute: 8,
    notifyFailuresTotal: 0,
    overdueNormalizationsTotal: 0,
    pendingFireClaims: 1,
    schedulesActive: 5,
    subscriptionsActive: 6,
    upsertPersistenceFailuresTotal: 0,
  },
};

const streamOverview = {
  realms: [realm],
  stats: {
    eventsTotal: 200,
    operationsPerSecond: 3.5,
    streamsActive: 8,
    subscriptionsActive: 9,
    watermarkLagBuckets: {
      caughtUp: 5,
      over100: 0,
      under10: 2,
      under100: 1,
    },
  },
};

const domainOverviews = [
  {
    assertText: "KV overview",
    queryKey: "kv",
    module: () => import("@/pages/app/kv"),
    path: "/kv",
    routePath: "/kv",
    emptyText: "No KV realms are currently visible.",
    errorText: "KV overview unavailable",
    loadingText: "Loading KV overview",
  },
  {
    assertText: "Lease overview",
    queryKey: "lease",
    module: () => import("@/pages/app/lease"),
    path: "/lease",
    routePath: "/lease",
    errorTitle: "Lease overview loading failure",
    emptyText: "No lease realms are currently visible.",
    errorText: "Lease overview loading failure",
    loadingText: "Loading lease overview snapshot...",
  },
  {
    assertText: "Notice overview",
    queryKey: "notice",
    module: () => import("@/pages/app/notice"),
    path: "/notice",
    routePath: "/notice",
    errorTitle: "Notice overview loading failure",
    emptyText: "No notice realms are currently visible.",
    errorText: "Notice overview loading failure",
    loadingText: "Loading notice overview snapshot...",
  },
  {
    assertText: "RPC overview",
    queryKey: "rpc",
    module: () => import("@/pages/app/rpc"),
    path: "/rpc",
    routePath: "/rpc",
    emptyText: "No RPC realms are currently visible.",
    errorTitle: "RPC overview loading failure",
    errorText: "RPC overview loading failure",
    loadingText: "Loading RPC overview snapshot...",
  },
  {
    assertText: "Schedule overview",
    queryKey: "schedule",
    module: () => import("@/pages/app/schedule"),
    path: "/schedule",
    routePath: "/schedule",
    emptyText: "No schedule realms are currently visible.",
    errorText: "Schedule overview unavailable",
    loadingText: "Loading schedule overview",
  },
  {
    assertText: "Stream overview",
    queryKey: "stream",
    module: () => import("@/pages/app/stream"),
    path: "/stream",
    routePath: "/stream",
    emptyText: "No stream realms are currently visible.",
    errorText: "Stream overview unavailable",
    loadingText: "Loading stream overview",
  },
  {
    assertText: "Queue overview",
    queryKey: "queue",
    module: () => import("@/pages/app/queue"),
    path: "/queue",
    routePath: "/queue",
    emptyText: "No queue realms are currently visible.",
    errorText: "Queue overview unavailable",
    loadingText: "Loading queue overview",
  },
];

const systemOverview = {
  broker: {
    connections: 2,
    messagesPerSecond: 3.25,
    realms: ["default"],
    sessions: 1,
    uptimeSeconds: 120,
  },
  diagnostics,
  domains: {
    kv: kvOverview.stats,
    lease: leaseOverview.stats,
    notice: {
      ...noticeOverview.stats,
      deliveryDropsTotal: 0,
      failureTotal: 0,
      requestsTotal: 1,
      successTotal: 1,
      unsubscribesTotal: 0,
      wildcardLimitRejectsTotal: 0,
    },
    queue: queueOverview.stats,
    rpc: {
      ...rpcOverview.stats,
      acksRejectedWrongWorkerTotal: 0,
      backpressureRejectsTotal: 0,
      duplicateCorrelationRejectsTotal: 0,
      pendingRoutesActive: 1,
      failureTotal: 0,
      requestTimeoutsTotal: 0,
      requestsTotal: 1,
      responsesDroppedClosedCallerTotal: 0,
      responsesMissingPendingTotal: 0,
      successTotal: 1,
      wrongWorkerRejectsTotal: 0,
    },
    schedule: {
      ...scheduleOverview.stats,
      ackFailuresTotal: 0,
      notifyFailuresTotal: 0,
      overdueNormalizationsTotal: 0,
    },
    stream: {
      ...streamOverview.stats,
      appendConflictsTotal: 0,
      failureTotal: 0,
      notifyDropsTotal: 0,
      requestsTotal: 1,
      successTotal: 1,
    },
  },
  fetchedAt: "2026-05-21T13:10:00.000Z",
  metrics: {
    lineCount: 1,
    lines: ["fitz_broker_up 1"],
    raw: "fitz_broker_up 1",
  },
};

const emptyTopology = {
  ...topologyOverview,
  connections: {
    ...topologyOverview.connections,
    items: [],
    total: 0,
    truncated: false,
  },
  lanes: [],
  sessionGroups: [],
};

const metricsOverview = {
  families: [
    {
      help: "Broker up",
      name: "fitz_broker_up",
      samples: [{ labels: {}, name: "fitz_broker_up", value: 1 }],
      type: "gauge",
    },
    {
      help: "Queue depth",
      name: "fitz_queue_depth_current",
      samples: [{ labels: { area: "primary" }, name: "fitz_queue_depth_current", value: 4 }],
      type: "gauge",
    },
    {
      help: "RPC request latency sum",
      name: "fitz_rpc_latency_histogram_sum",
      samples: [{ labels: { route: "all" }, name: "fitz_rpc_latency_histogram_sum", value: 12.5 }],
      type: "gauge",
    },
  ],
  raw: "fitz_broker_up 1\nfitz_queue_depth_current 4\nfitz_rpc_latency_histogram_sum 12.5",
};

const activeSessions = {
  sessions: [
    {
      connectedAt: "2026-05-21T13:00:00Z",
      idleSeconds: 12,
      identityClaim: "tid",
      identityValue: "default",
      key: "session-1",
      messagesReceived: 2,
      messagesSent: 3,
      remoteAddress: "127.0.0.1",
      routeFamily: 1,
      sessionId: "session-1",
      subject: "user:1",
      transport: "ws",
    },
    {
      connectedAt: "2026-05-21T13:01:00Z",
      idleSeconds: 45,
      identityClaim: "tenant",
      identityValue: "ops",
      key: "session-2",
      messagesReceived: 4,
      messagesSent: 8,
      remoteAddress: "2001:db8::1ff:fe23:4567:890a",
      routeFamily: 2,
      sessionId: "session-long-id-2",
      subject: "user:2",
      transport: "http",
    },
  ],
};

const queueResource = {
  deadLetters: [],
  detail: {
    area: "ops",
    messagesDeadLettered: 0,
    messagesDelayed: 1,
    messagesInflight: 2,
    messagesReady: 3,
    messagesTotal: 6,
    oldestMessageAgeSeconds: 30,
    realm: "default",
    resource: "primary",
  },
  inflight: [],
  timeline: {
    derived: false,
    events: [],
    limit: 8,
    realm: "default",
    area: "ops",
    resource: "primary",
  },
};

const queueComparison = {
  comparisonMode: "resource",
  delta: {
    ageSeconds: 0,
    backlog: 0,
    deadLetters: 0,
    delayed: 0,
    inflight: 0,
    ready: 0,
    recentTransitionCount: 0,
    waiters: 0,
  },
  derived: false,
  left: {
    metrics: {
      ageSeconds: 30,
      backlog: 3,
      deadLetters: 0,
      delayed: 1,
      inflight: 2,
      ready: 3,
      recentTransitionCount: 0,
      waiters: 0,
    },
    scope: {
      area: "ops",
      realm: "default",
      resource: "primary",
    },
  },
  right: {
    metrics: {
      ageSeconds: 30,
      backlog: 3,
      deadLetters: 0,
      delayed: 1,
      inflight: 2,
      ready: 3,
      recentTransitionCount: 0,
      waiters: 0,
    },
    scope: {
      area: "ops",
      realm: "default",
      resource: "secondary",
    },
  },
  summary: "Snapshots match",
};

const resourceDetail = {
  comparison: null,
  detailMetrics: [
    { label: "Keys", value: 12 },
    { label: "Ops / sec", value: "2.50" },
  ],
  domain: "kv",
  raw: {
    detail: {},
  },
  ref: {
    area: "ops",
    realm: "default",
    resource: "primary",
  },
  related: [],
  timeline: {
    derived: false,
    events: [],
  },
};

type DomainOverviewFixture = {
  realms: Array<{ realm: string }>;
  stats: {
    [key: string]: number | { [key: string]: number };
  };
};

const domainOverviewDataByKey: Record<string, DomainOverviewFixture> = {
  kv: kvOverview,
  lease: leaseOverview,
  notice: noticeOverview,
  rpc: rpcOverview,
  schedule: scheduleOverview,
  stream: streamOverview,
  queue: queueOverview,
};

function resetQueries() {
  mocks.queryStates.currentSession = queryState.fresh({ username: "admin" }, queryOptions());
  mocks.queryStates.activeSessions = queryState.fresh(activeSessions, queryOptions());
  mocks.queryStates.system = queryState.fresh(systemOverview, queryOptions());
  mocks.queryStates.topology = queryState.fresh(topologyOverview, queryOptions());
  mocks.queryStates.metrics = queryState.fresh(metricsOverview, queryOptions());
  mocks.queryStates.queue = queryState.fresh(queueOverview, queryOptions());
  mocks.queryStates.queueDeadLetters = queryState.fresh([], queryOptions());
  mocks.queryStates.queueResource = queryState.fresh(queueResource, queryOptions());
  mocks.queryStates.queueTimeline = queryState.fresh(queueResource.timeline, queryOptions());
  mocks.queryStates.queueComparison = queryState.fresh(queueComparison, queryOptions());
  mocks.queryStates.kv = queryState.fresh(kvOverview, queryOptions());
  mocks.queryStates.lease = queryState.fresh(leaseOverview, queryOptions());
  mocks.queryStates.notice = queryState.fresh(noticeOverview, queryOptions());
  mocks.queryStates.rpc = queryState.fresh(rpcOverview, queryOptions());
  mocks.queryStates.schedule = queryState.fresh(scheduleOverview, queryOptions());
  mocks.queryStates.stream = queryState.fresh(streamOverview, queryOptions());
  mocks.queryStates.inventory = queryState.fresh(inventory, queryOptions());
  mocks.queryStates.resource = queryState.fresh(resourceDetail, queryOptions());
  mocks.mutation.error = null;
  mocks.mutation.pending = false;
  mocks.mutation.result = null;
}

async function mountRoute(path: string, routePath: string, handler: RouteHandler) {
  cleanupApp("app");
  document.body.innerHTML = '<div id="app"></div>';
  window.history.pushState({}, "", path);

  const root = document.getElementById("app");
  if (!root) {
    throw new Error("Missing test app root");
  }

  await createSPA({
    root,
    routes: [{ handler, path: routePath }],
  });

  await new Promise<void>((resolve) => queueMicrotask(() => resolve()));
  return root;
}

afterEach(() => {
  cleanupApp("app");
  document.body.innerHTML = "";
  vi.clearAllMocks();
});

beforeEach(() => {
  resetQueries();
});

describe("admin page smoke tests", () => {
  it("mounts every authenticated route with success data", async () => {
    const pages = [
      {
        assertText: "Broker status",
        module: () => import("@/pages/app/home"),
        path: "/",
        routePath: "/",
      },
      {
        assertText: "Active sessions",
        module: () => import("@/pages/app/sessions"),
        path: "/sessions",
        routePath: "/sessions",
      },
      {
        assertText: "Metrics explorer",
        module: () => import("@/pages/app/metrics"),
        path: "/admin/metrics",
        routePath: "/admin/metrics",
      },
      {
        assertText: "Queue overview",
        module: () => import("@/pages/app/queue"),
        path: "/queue",
        routePath: "/queue",
      },
      {
        assertText: "Queue resource inspection",
        module: () => import("@/pages/app/queue-resource"),
        path: "/queue/default/ops/primary",
        routePath: "/queue/{realm}/{area}/{resource}",
      },
      {
        assertText: "KV overview",
        module: () => import("@/pages/app/kv"),
        path: "/kv",
        routePath: "/kv",
      },
      {
        assertText: "Lease overview",
        module: () => import("@/pages/app/lease"),
        path: "/lease",
        routePath: "/lease",
      },
      {
        assertText: "Notice overview",
        module: () => import("@/pages/app/notice"),
        path: "/notice",
        routePath: "/notice",
      },
      {
        assertText: "RPC overview",
        module: () => import("@/pages/app/rpc"),
        path: "/rpc",
        routePath: "/rpc",
      },
      {
        assertText: "Schedule overview",
        module: () => import("@/pages/app/schedule"),
        path: "/schedule",
        routePath: "/schedule",
      },
      {
        assertText: "Stream overview",
        module: () => import("@/pages/app/stream"),
        path: "/stream",
        routePath: "/stream",
      },
      {
        assertText: "resource inspection",
        module: () => import("@/pages/app/resource-detail"),
        path: "/kv/default/ops/primary",
        routePath: "/kv/{realm}/{area}/{resource}",
      },
    ];

    for (const page of pages) {
      const { default: Component } = await page.module();
      const root = await mountRoute(page.path, page.routePath, Component);

      expect(root.textContent).toContain(page.assertText);
      expect(root.textContent?.trim().length).toBeGreaterThan(0);
      expect(root.querySelector('[data-slot="shell"]')).toBeNull();
      expect(root.querySelectorAll("main#main-content")).toHaveLength(1);

      cleanupApp(root);
      document.body.innerHTML = "";
    }
  }, 15000);

  it("renders all domain overviews with the shared frame contract", async () => {
    for (const page of domainOverviews) {
      const { default: Component } = await page.module();

      const root = await mountRoute(page.path, page.routePath, Component);
      const text = root.textContent ?? "";

      expect(root.querySelectorAll("main#main-content")).toHaveLength(1);
      expect(text).toContain(page.assertText);
      expect(text).toContain("Refresh");
      expect(text).toContain("Live");

      cleanupApp(root);
      document.body.innerHTML = "";
    }
  });

  it("renders domain overview loading states consistently", async () => {
    for (const page of domainOverviews) {
      mocks.queryStates[page.queryKey] = queryState.loading(queryOptions());

      const { default: Component } = await page.module();
      const root = await mountRoute(page.path, page.routePath, Component);

      expect(root.textContent).toContain(page.loadingText);
      expect(root.querySelectorAll("main#main-content")).toHaveLength(1);

      cleanupApp(root);
      document.body.innerHTML = "";
    }
  });

  it("renders domain overview error states with page-specific framing", async () => {
    for (const page of domainOverviews) {
      mocks.queryStates[page.queryKey] = queryState.error(
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

  it("renders lease risk and ownership-pressure priority metrics", async () => {
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
    const labels = [
      "Active leases",
      "Waiters",
      "Oldest lease age",
      "Ownership pressure",
      "Acquire timeouts",
      "Forced releases",
      "Token rejects",
    ];

    let cursor = -1;
    for (const label of labels) {
      const index = text.indexOf(label, cursor + 1);
      expect(index).toBeGreaterThan(cursor);
      cursor = index;
    }

    expect(text).toContain("1h");
    expect(text).toContain("Attention");
    expect(text).toContain("acquire timeout");
  });

  it("renders domain overviews with empty realm tables", async () => {
    for (const page of domainOverviews) {
      const baseOverview = domainOverviewDataByKey[page.queryKey];
      if (!baseOverview) {
        continue;
      }

      mocks.queryStates[page.queryKey] = queryState.fresh(
        {
          ...baseOverview,
          realms: [],
        },
        queryOptions(),
      );

      const { default: Component } = await page.module();
      const root = await mountRoute(page.path, page.routePath, Component);

      expect(root.textContent).toContain(page.emptyText);

      cleanupApp(root);
      document.body.innerHTML = "";
    }
  });

  it("renders notice risk and fanout-priority metrics", async () => {
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
    const labels = [
      "Active subscriptions",
      "Publish rate",
      "Delivery drops",
      "Wildcard limit rejects",
    ];

    let cursor = -1;
    for (const label of labels) {
      const index = text.indexOf(label, cursor + 1);
      expect(index).toBeGreaterThan(cursor);
      cursor = index;
    }

    expect(text).toContain("2");
    expect(text).toContain("1");
    expect(text).toContain("Attention");
    expect(text).toContain("live fanout");
  });

  it("renders rpc pressure-first health and risk signals", async () => {
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
    const labels = [
      "Requests pending",
      "Workers registered",
      "Pending routes active",
      "Ops / sec",
      "Request timeouts",
      "Failure responses",
    ];

    let cursor = -1;
    for (const label of labels) {
      const index = text.indexOf(label, cursor + 1);
      expect(index).toBeGreaterThan(cursor);
      cursor = index;
    }

    expect(text).toContain("Attention");
    expect(text).toContain("Response reliability");
  });

  it("renders the status-first dashboard sections", async () => {
    const { default: Home } = await import("@/pages/app/home");

    const root = await mountRoute("/", "/", Home);
    const text = root.textContent ?? "";

    expect(text).toContain("Broker status");
    expect(text).toContain("Healthy");
    expect(text).toContain("Messaging flow");
    expect(text).toContain("Connected sessions");
    expect(text).toContain("Next: Check queue");
    expect(text).toContain("Fitz broker");
    expect(text).toContain("Consumers and observers");
    expect(text).toContain("Top scoped resources");
    expect(text).toContain("Visible connections");
    expect(text).toContain("Domain signals");
    expect(text).toContain("Open page");
    expect(text).toContain("Flow inspector");
    expect(text).toContain("Durable work delivery");
    expect(text).toContain("Connected sessions");
    expect(text).toContain("Current broker behavior");
    expect(text).toContain("Attention");
  });

  it("keeps dashboard behavior visible while refresh is in flight", async () => {
    const { default: Home } = await import("@/pages/app/home");

    mocks.queryStates.topology = queryState.refreshing(topologyOverview, queryOptions());

    const root = await mountRoute("/", "/", Home);
    const text = root.textContent ?? "";

    expect(text).toContain("Refreshing");
    expect(text).toContain("Domain signals");
    expect(text).toContain("Open page");
    expect(text).toContain("Durable work delivery");
    expect(text).toContain("Queue");
  });

  it("renders the empty dashboard state when no lanes are visible", async () => {
    const { default: Home } = await import("@/pages/app/home");

    mocks.queryStates.topology = queryState.fresh(emptyTopology, queryOptions());

    const root = await mountRoute("/", "/", Home);
    const text = root.textContent ?? "";

    expect(text).toContain("Broker status");
    expect(text).toContain("No domain lanes are visible yet");
    expect(text).toContain("Domain workspaces");
    expect(text).toContain(
      "Open a domain when you need a narrower view of resources and live counters.",
    );
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

    mocks.queryStates.queue = queryState.loading(queryOptions());
    let root = await mountRoute("/queue", "/queue", QueuePage);
    expect(root.textContent).toContain("Loading queue overview");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.queue = queryState.error(
      new Error("Queue unavailable"),
      undefined,
      queryOptions(),
    );
    root = await mountRoute("/queue", "/queue", QueuePage);
    expect(root.textContent).toContain("Queue unavailable");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.queue = queryState.fresh(
      {
        ...queueOverview,
        realms: [],
      },
      queryOptions(),
    );
    root = await mountRoute("/queue", "/queue", QueuePage);
    expect(root.textContent).toContain("No queue realms are currently visible");
  });

  it("keeps queue overview content visible while refresh is in flight", async () => {
    const { default: QueuePage } = await import("@/pages/app/queue");

    mocks.queryStates.queue = queryState.refreshing(queueOverview, queryOptions());

    const root = await mountRoute("/queue", "/queue", QueuePage);

    expect(root.textContent).toContain("Refreshing");
    expect(root.textContent).toContain("Scope summary");
    expect(root.textContent).toContain("Queue metrics");
  });

  it("mounts queue comparison and generic resource comparison flows", async () => {
    const { default: QueueResourcePage } = await import("@/pages/app/queue-resource");
    let root = await mountRoute(
      "/queue/default/ops/primary?againstRealm=default&againstArea=ops&againstResource=secondary",
      "/queue/{realm}/{area}/{resource}",
      QueueResourcePage,
    );

    expect(root.textContent).toContain("Comparison summary");
    expect(root.textContent).toContain("Current scope");
    expect(root.textContent).toContain("Target scope");
    expect(root.textContent).toContain("Difference");
    expect(root.textContent).toContain("Snapshots match");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.resource = queryState.fresh(
      {
        ...resourceDetail,
        comparison: {
          metrics: [{ label: "Delta", value: 0 }],
          summary: "No material difference",
        },
      },
      queryOptions(),
    );

    const { default: ResourceDetailPage } = await import("@/pages/app/resource-detail");
    root = await mountRoute(
      "/kv/default/ops/primary?againstRealm=default&againstArea=ops&againstResource=secondary",
      "/kv/{realm}/{area}/{resource}",
      ResourceDetailPage,
    );

    expect(root.textContent).toContain("Comparison details");
    expect(root.textContent).toContain("Matched");
    expect(root.textContent).toContain("No material difference");
  });

  it("opens an accessible queue dead-letter confirmation dialog", async () => {
    const { default: QueueResourcePage } = await import("@/pages/app/queue-resource");
    mocks.queryStates.queueResource = queryState.fresh(
      {
        ...queueResource,
        deadLetters: [
          {
            attempts: 2,
            deadLetteredAt: "2026-05-21T13:05:00Z",
            family: 1,
            messageId: 42,
            reason: "handler failed",
          },
        ],
      },
      queryOptions(),
    );

    const root = await mountRoute(
      "/queue/default/ops/primary",
      "/queue/{realm}/{area}/{resource}",
      QueueResourcePage,
    );
    const replay = Array.from(root.querySelectorAll("button")).find(
      (button) => button.textContent === "Replay",
    );

    expect(replay).toBeDefined();

    replay?.click();
    await new Promise<void>((resolve) => queueMicrotask(() => resolve()));

    expect(root.textContent).toContain("Replay dead-letter message?");
    expect(root.textContent).toContain("Replay message 42 in default / ops / primary.");
    expect(root.querySelector('[role="alertdialog"]')).toBeTruthy();
  });

  it("uses mutation-owned login pending and error states", async () => {
    const { default: Login } = await import("@/pages/auth/login");

    mocks.mutation.pending = true;
    let root = await mountRoute("/login", "/login", Login);

    expect(root.textContent).toContain("Signing in...");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.mutation.pending = false;
    mocks.mutation.error = new Error("Bad credentials");
    root = await mountRoute("/login", "/login", Login);

    expect(root.textContent).toContain("Bad credentials");
  });

  it("uses mutation-owned logout pending, success, and error states", async () => {
    const { default: Logout } = await import("@/pages/auth/logout");

    mocks.mutation.execute.mockImplementationOnce(() => new Promise<void>(() => {}));

    let root = await mountRoute("/logout", "/logout", Logout);

    expect(root.textContent).toContain("Signing out");
    expect(root.textContent).toContain("Clearing your session.");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.mutation.execute.mockResolvedValueOnce(undefined);
    root = await mountRoute("/logout", "/logout", Logout);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));

    expect(root.textContent).toContain("Signed out");
    expect(root.textContent).toContain("Go to sign in");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.mutation.execute.mockRejectedValueOnce(new Error("Logout failed"));
    root = await mountRoute("/logout", "/logout", Logout);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));

    expect(root.textContent).toContain("Sign out failed");
    expect(root.textContent).toContain("Logout failed");
    expect(root.textContent).toContain("Try again");
  });
});
