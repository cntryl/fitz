import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { cleanupApp, createSPA } from "@askrjs/askr/boot";
import type { RouteHandler } from "@askrjs/askr/router";

type QueryState<T> = {
  data: T | null;
  error: unknown | null;
  loading: boolean;
  refreshing: boolean;
  stale: boolean;
  consistency: "fresh" | "stale" | "refreshing" | "pending-write";
  refresh: () => Promise<void>;
};

const mocks = vi.hoisted(() => {
  const refresh = vi.fn(async () => undefined);
  const mutation = {
    abort: vi.fn(),
    error: null,
    execute: vi.fn(async () => true),
    pending: false,
    reset: vi.fn(),
    result: null,
    status: "idle" as const,
  };

  return {
    queryStates: {} as Record<string, QueryState<unknown>>,
    refresh,
    mutation,
  };
});

function makeQuery<T>(data: T | null, overrides: Partial<QueryState<T>> = {}): QueryState<T> {
  return {
    consistency: "fresh",
    data,
    error: null,
    loading: false,
    refresh: mocks.refresh,
    refreshing: false,
    stale: false,
    ...overrides,
  };
}

vi.mock("@/features/session/session-query", () => ({
  createActiveSessionsQuery: () => mocks.queryStates.activeSessions,
  createCurrentSessionQuery: () => mocks.queryStates.currentSession,
}));

vi.mock("@/features/session/session-mutation", () => ({
  createSignInMutation: () => mocks.mutation,
  createSignOutMutation: () => mocks.mutation,
}));

vi.mock("@/features/system/health-query", () => ({
  createHealthSummaryQuery: () => mocks.queryStates.health,
}));

vi.mock("@/features/system/system-query", () => ({
  createSystemOverviewQuery: () => mocks.queryStates.system,
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

const health = {
  liveness: "ok",
  readiness: "ok",
  startup: "ok",
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
    keysTotal: 12,
    operationsPerSecond: 2.5,
    transactionsActive: 1,
  },
};

const leaseOverview = {
  realms: [realm],
  stats: {
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
    subscriptionsActive: 7,
  },
};

const rpcOverview = {
  realms: [realm],
  stats: {
    operationsPerSecond: 2,
    requestsPending: 1,
    workersRegistered: 4,
  },
};

const scheduleOverview = {
  realms: [realm],
  stats: {
    executionsPerMinute: 8,
    pendingFireClaims: 1,
    schedulesActive: 5,
    subscriptionsActive: 6,
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

const diagnostics = {
  hotspots: [
    {
      area: "ops",
      current_stage: "healthy",
      domain: "queue",
      likely_bottleneck: null,
      realm: "default",
      resource: "primary",
      severity: "informational",
    },
  ],
  incident_summary: {
    explanation: "No active pressure detected",
    recommended_next_query: "Check queue",
    status: "healthy",
    title: "Healthy",
  },
  top_bottleneck: null,
};

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
  healthStatus: "ok",
  metrics: {
    lineCount: 1,
    lines: ["fitz_broker_up 1"],
    raw: "fitz_broker_up 1",
  },
};

const metricsOverview = {
  families: [
    {
      help: "Broker up",
      name: "fitz_broker_up",
      samples: [{ labels: {}, name: "fitz_broker_up", value: 1 }],
      type: "gauge",
    },
  ],
  raw: "fitz_broker_up 1",
};

const activeSessions = {
  realm: null,
  sessions: [
    {
      connectedAt: "2026-05-21T13:00:00Z",
      idleSeconds: 12,
      key: "session-1",
      messagesReceived: 2,
      messagesSent: 3,
      realm: "default",
      remoteAddress: "127.0.0.1",
      sessionId: "session-1",
      transport: "ws",
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

function resetQueries() {
  mocks.queryStates.currentSession = makeQuery({ username: "admin" });
  mocks.queryStates.activeSessions = makeQuery(activeSessions);
  mocks.queryStates.health = makeQuery(health);
  mocks.queryStates.system = makeQuery(systemOverview);
  mocks.queryStates.metrics = makeQuery(metricsOverview);
  mocks.queryStates.queue = makeQuery(queueOverview);
  mocks.queryStates.queueDeadLetters = makeQuery([]);
  mocks.queryStates.queueResource = makeQuery(queueResource);
  mocks.queryStates.queueTimeline = makeQuery(queueResource.timeline);
  mocks.queryStates.queueComparison = makeQuery(queueComparison);
  mocks.queryStates.kv = makeQuery(kvOverview);
  mocks.queryStates.lease = makeQuery(leaseOverview);
  mocks.queryStates.notice = makeQuery(noticeOverview);
  mocks.queryStates.rpc = makeQuery(rpcOverview);
  mocks.queryStates.schedule = makeQuery(scheduleOverview);
  mocks.queryStates.stream = makeQuery(streamOverview);
  mocks.queryStates.inventory = makeQuery(inventory);
  mocks.queryStates.resource = makeQuery(resourceDetail);
}

async function mountRoute(path: string, routePath: string, handler: RouteHandler) {
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
        assertText: "Welcome, admin",
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
        path: "/metrics",
        routePath: "/metrics",
      },
      {
        assertText: "Queue overview",
        module: () => import("@/pages/app/queue"),
        path: "/queue",
        routePath: "/queue",
      },
      {
        assertText: "Resource drill-down",
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
        assertText: "primary",
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

      cleanupApp(root);
      document.body.innerHTML = "";
    }
  });

  it("mounts representative loading, error, and empty states", async () => {
    const { default: QueuePage } = await import("@/pages/app/queue");

    mocks.queryStates.queue = makeQuery(null, { loading: true });
    let root = await mountRoute("/queue", "/queue", QueuePage);
    expect(root.textContent).toContain("Loading queue overview");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.queue = makeQuery(null, { error: new Error("Queue unavailable") });
    root = await mountRoute("/queue", "/queue", QueuePage);
    expect(root.textContent).toContain("Queue unavailable");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.queue = makeQuery({
      ...queueOverview,
      realms: [],
    });
    root = await mountRoute("/queue", "/queue", QueuePage);
    expect(root.textContent).toContain("No queue realms are currently visible");
  });

  it("mounts queue comparison and generic resource comparison flows", async () => {
    const { default: QueueResourcePage } = await import("@/pages/app/queue-resource");
    let root = await mountRoute(
      "/queue/default/ops/primary?againstRealm=default&againstArea=ops&againstResource=secondary",
      "/queue/{realm}/{area}/{resource}",
      QueueResourcePage,
    );

    expect(root.textContent).toContain("Snapshots match");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.resource = makeQuery({
      ...resourceDetail,
      comparison: {
        metrics: [{ label: "Delta", value: 0 }],
        summary: "No material difference",
      },
    });

    const { default: ResourceDetailPage } = await import("@/pages/app/resource-detail");
    root = await mountRoute(
      "/kv/default/ops/primary?againstRealm=default&againstArea=ops&againstResource=secondary",
      "/kv/{realm}/{area}/{resource}",
      ResourceDetailPage,
    );

    expect(root.textContent).toContain("Compare: No material difference");
  });
});
