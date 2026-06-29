import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { cleanupApp, createSPA } from "@askrjs/askr/boot";
import type { Query } from "@askrjs/askr/data";
import type { RouteHandler } from "@askrjs/askr/router";
import { queryState } from "@askrjs/askr/testing";
import {
  dashboardDiagnostics as diagnostics,
  healthyGlobalDiagnostics,
  topologyAppLane,
  topologyOverview,
} from "./fixtures/topology";

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
  createQueueAreaQuery: () => mocks.queryStates.queueArea,
  createQueueOverviewQuery: () => mocks.queryStates.queue,
  createQueueRealmQuery: () => mocks.queryStates.queueRealm,
  createQueueInventoryQuery: () => mocks.queryStates.queueInventory,
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

vi.mock("@/features/kv/kv-rows-query", () => ({
  createKvRowsQuery: () => mocks.queryStates.kvRows,
}));

vi.mock("@/features/lease/lease-query", () => ({
  createLeaseOverviewQuery: () => mocks.queryStates.lease,
  createLeaseRealmQuery: () => mocks.queryStates.leaseRealm,
  createLeaseAreaQuery: () => mocks.queryStates.leaseArea,
  createLeaseResourceRowsQuery: () => mocks.queryStates.leaseResourceRows,
}));

vi.mock("@/features/notice/notice-query", () => ({
  createNoticeOverviewQuery: () => mocks.queryStates.notice,
  createNoticeRealmQuery: () => mocks.queryStates.noticeRealm,
  createNoticeAreaQuery: () => mocks.queryStates.noticeArea,
  createNoticeResourceRowsQuery: () => mocks.queryStates.noticeResourceRows,
  createNoticeOperationRowsQuery: () => mocks.queryStates.noticeOperationRows,
}));

vi.mock("@/features/rpc/rpc-query", () => ({
  createRpcAreaQuery: () => mocks.queryStates.rpcArea,
  createRpcOverviewQuery: () => mocks.queryStates.rpc,
  createRpcOperationQuery: () => mocks.queryStates.rpcOperation,
  createRpcRealmQuery: () => mocks.queryStates.rpcRealm,
  createRpcResourceQuery: () => mocks.queryStates.rpcResource,
}));

vi.mock("@/features/schedule/schedule-query", () => ({
  createScheduleAreaQuery: () => mocks.queryStates.scheduleArea,
  createScheduleExecutionObservationsQuery: () => mocks.queryStates.scheduleExecutionObservations,
  createScheduleMissedHandoffsQuery: () => mocks.queryStates.scheduleMissedHandoffs,
  createScheduleOverviewQuery: () => mocks.queryStates.schedule,
  createScheduleRealmQuery: () => mocks.queryStates.scheduleRealm,
  createScheduleResourceQuery: () => mocks.queryStates.scheduleResource,
}));

vi.mock("@/features/stream/stream-query", () => ({
  createStreamAreaQuery: () => mocks.queryStates.streamArea,
  createStreamOverviewQuery: () => mocks.queryStates.stream,
  createStreamRealmQuery: () => mocks.queryStates.streamRealm,
  createStreamResourceQuery: () => mocks.queryStates.streamResource,
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

const kvRows = {
  area: "ops",
  hasMore: false,
  items: [
    {
      key: {
        base64: "dXNlcjox",
        lenBytes: 6,
        utf8: "user:1",
      },
      value: {
        base64: "YWxpY2U=",
        lenBytes: 5,
        utf8: "alice",
      },
    },
  ],
  limit: 50,
  nextCursor: null,
  realm: "default",
  resource: "primary",
  routeFamily: 1,
  startsWith: {
    base64: "",
    lenBytes: 0,
    utf8: "",
  },
};

const queueOverview = {
  realms: [
    {
      areaCount: 1,
      completeSuccessTotal: 3,
      enqueueSuccessTotal: 8,
      inRatePerSecond: 1.5,
      messagesDeadLettered: 0,
      messagesDelayed: 1,
      messagesInflight: 2,
      messagesReady: 4,
      messagesTotal: 7,
      oldestBacklogAgeSeconds: 17,
      outRatePerSecond: 0.5,
      queueCount: 1,
      realm: "default",
      status: "falling_behind",
      subscriptionsActive: 2,
    },
  ],
  stats: {
    inflightActive: 2,
    messagesDeadLettered: 0,
    messagesDelayed: 1,
    messagesPending: 3,
    messagesReady: 4,
    oldestBacklogAgeSeconds: 17,
    operationsPerSecond: 1.25,
  },
};

const queueInventory = {
  domain: "queue",
  realms: [
    {
      realm: "default",
      areas: [
        {
          area: "ops",
          resources: ["primary"],
        },
      ],
    },
  ],
};

const queueAreaRow = {
  area: "ops",
  completeSuccessTotal: 3,
  enqueueSuccessTotal: 8,
  inRatePerSecond: 1.5,
  messagesDeadLettered: 0,
  messagesDelayed: 1,
  messagesInflight: 2,
  messagesReady: 4,
  messagesTotal: 7,
  oldestBacklogAgeSeconds: 17,
  outRatePerSecond: 0.5,
  queueCount: 1,
  realm: "default",
  status: "falling_behind",
  subscriptionsActive: 2,
};

const queueResourceRow = {
  area: "ops",
  completeSuccessTotal: 3,
  enqueueSuccessTotal: 8,
  familyCount: 1,
  inRatePerSecond: 1.5,
  messagesDeadLettered: 0,
  messagesDelayed: 1,
  messagesInflight: 2,
  messagesReady: 4,
  messagesTotal: 7,
  oldestBacklogAgeSeconds: 17,
  outRatePerSecond: 0.5,
  realm: "default",
  resource: "primary",
  status: "falling_behind",
  subscriptionsActive: 2,
};

const queueRealmDetail = {
  ...queueOverview.realms[0],
  areas: [queueAreaRow],
  queues: [queueResourceRow],
};

const queueAreaDetail = {
  ...queueAreaRow,
  queues: [queueResourceRow],
};

const kvOverview = {
  realms: [realm],
  stats: {
    commitsFailedTotal: 0,
    invalidTransactionRejectsTotal: 0,
    keysTotal: 12,
    operationsPerSecond: 2.5,
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

const noticeRealmInventory = {
  areas: [
    {
      area: "ops",
      realm: "default",
      resources: ["primary"],
    },
  ],
  realm: "default",
};

const noticeAreaInventory = {
  area: "ops",
  realm: "default",
  resources: ["primary"],
};

const noticeResourceRows = {
  area: "ops",
  limit: 50,
  operations: [
    {
      operation: "GetStatus",
      activeSubscribers: 2,
      rollingMessageCount: 18,
      latencyMs: null,
    },
  ],
  realm: "default",
  resource: "primary",
  routeFamily: 7,
};

const noticeOperationRows = {
  area: "ops",
  limit: 50,
  observations: [
    {
      area: "ops",
      notificationsReceived: 12,
      publishesPerMinute: 30,
      publishesTotal: 120,
      realm: "default",
      resource: "primary",
      route: "GetStatus",
      sessionId: "session-1",
      status: "open",
      subscriptionId: 11,
    },
    {
      area: "ops",
      notificationsReceived: 8,
      publishesPerMinute: 11,
      publishesTotal: 45,
      realm: "default",
      resource: "primary",
      route: "GetStatus",
      sessionId: "session-2",
      status: "open",
      subscriptionId: 12,
    },
  ],
  realm: "default",
  routeFamily: 7,
};

const rpcOverview = {
  realms: [realm],
  stats: {
    failureTotal: 0,
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

const rpcRealm = {
  areas: [{ area: "ops", realm: "default", resources: ["primary"] }],
  realm: "default",
};

const rpcArea = {
  area: "ops",
  realm: "default",
  resources: ["primary"],
};

const rpcResource = {
  area: "ops",
  operations: [
    {
      averageLatencyMs: 12,
      operation: "GetStatus",
      pendingRequests: 1,
      requestsHandled: 9,
      workers: 2,
    },
  ],
  realm: "default",
  resource: "primary",
};

const rpcOperation = {
  calls: {
    limit: 50,
    observations: [
      {
        age_seconds: null,
        area: "ops",
        average_latency_ms: 12,
        correlation_id: null,
        operation: "GetStatus",
        realm: "default",
        registered_at: "2026-05-21T13:00:00.000Z",
        requests_handled: 9,
        resource: "primary",
        route: "rpc://default/ops/primary/GetStatus",
        route_family: 7,
        state: "worker_registered",
        submitted_at: null,
        worker_session_id: "worker-1",
      },
    ],
    route_family: 7,
  },
  detail: {
    area: "ops",
    diagnostics: healthyGlobalDiagnostics,
    operation: "GetStatus",
    realm: "default",
    requests_pending: 1,
    resource: "primary",
    slowest_worker_average_latency_ms: 12,
    worker_latency_buckets: {
      over_100ms: 0,
      under_100ms: 1,
      under_25ms: 1,
      under_5ms: 0,
    },
    workers_registered: 2,
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

const scheduleRealm = {
  areas: [{ area: "ops", resources: ["primary"] }],
  realm: "default",
  resourceCount: 1,
};

const scheduleArea = {
  area: "ops",
  realm: "default",
  resourceCount: 1,
  resources: ["primary"],
};

const scheduleResource = {
  detail: {
    area: "ops",
    cron: "*/5 * * * *",
    diagnostics: healthyGlobalDiagnostics,
    enabled: true,
    executions_total: 42,
    next_run: "2026-05-21T13:05:00.000Z",
    realm: "default",
    resource: "primary",
  },
  executionObservations: {
    area: "ops",
    limit: 20,
    observations: [
      {
        area: "ops",
        cron: "*/5 * * * *",
        executions_total: 42,
        last_run: "2026-05-21T13:00:00.000Z",
        next_run: "2026-05-21T13:05:00.000Z",
        operation: "handoff",
        realm: "default",
        resource: "primary",
        route_family: 7,
        status: "observed",
      },
    ],
    realm: "default",
    resource: "primary",
    route_family: 7,
  },
  missedHandoffs: {
    limit: 20,
    observations: [
      {
        age_seconds: 90,
        area: "ops",
        claimed_at: "2026-05-21T12:59:30.000Z",
        fire_at: "2026-05-21T12:59:00.000Z",
        fire_ms: 1780001940000,
        operation: "handoff",
        realm: "default",
        resource: "primary",
        route_family: 7,
        status: "pending",
      },
    ],
    route_family: 7,
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

const streamRealm = {
  areaCount: 1,
  areas: [{ area: "ops", resources: ["events"] }],
  familyWatermarks: [{ family: 7, watermark: 10 }],
  realm: "default",
  resourceCount: 1,
};

const streamArea = {
  area: "ops",
  familyWatermarks: [{ family: 7, watermark: 10 }],
  realm: "default",
  resourceCount: 1,
  resources: ["events"],
};

const streamResource = {
  detail: {
    area: "ops",
    diagnostics: healthyGlobalDiagnostics,
    offset: 0,
    realm: "default",
    resource: "events",
    sessions_active: 1,
    size_bytes: 128,
    watermark: 10,
  },
  records: {
    area: "ops",
    from_offset: 0,
    has_more: false,
    limit: 50,
    realm: "default",
    records: [
      {
        area: "ops",
        area_offset: 0,
        body: { base64: "eyJvayI6dHJ1ZX0=", len_bytes: 11, utf8: '{"ok":true}' },
        created_at_ms: 1780000000000,
        metadata: null,
        realm: "default",
        realm_offset: 0,
        resource: "events",
        resource_offset: 0,
        route_family: 7,
      },
    ],
    resource: "events",
    route_family: 7,
  },
};

const domainOverviews = [
  {
    assertText: "KV overview",
    queryKey: "kv",
    module: () => import("@/pages/app/kv"),
    path: "/kv",
    routePath: "/kv",
    emptyText: "No visible KV resources at the current level.",
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
    errorTitle: "Schedule overview loading failure",
    errorText: "Schedule overview loading failure",
    loadingText: "Loading schedule overview snapshot...",
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
    emptyText: "No visible queues at the current level.",
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
    completeSuccessTotal: 3,
    enqueueSuccessTotal: 8,
    inRatePerSecond: 1.5,
    messagesDeadLettered: 0,
    messagesDelayed: 1,
    messagesInflight: 2,
    messagesReady: 3,
    messagesTotal: 6,
    oldestBacklogAgeSeconds: 30,
    oldestMessageAgeSeconds: 30,
    outRatePerSecond: 0.5,
    realm: "default",
    resource: "primary",
    status: "falling_behind",
    subscriptionsActive: 2,
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

const leaseRealm = {
  areas: [
    {
      area: "ops",
      realm: "default",
      resources: ["primary"],
    },
  ],
  realm: "default",
};

const leaseArea = {
  area: "ops",
  realm: "default",
  resources: ["primary"],
};

function leaseResourceRowsFixture(expiresOffsetSeconds = 120) {
  const expiresAt = new Date(Date.now() + expiresOffsetSeconds * 1000).toISOString();

  return {
    items: [
      {
        acquiredAt: "2026-05-21T13:00:00.000Z",
        ageSeconds: 12,
        area: "ops",
        expiresAt,
        ownerId: "owner-lease-primary",
        ownerSessionId: "session-lease-primary",
        pendingWaiters: 4,
        queuedToken: 12,
        realm: "default",
        resource: "primary",
        routeFamily: 7,
        state: "owned",
      },
    ],
    limit: 50,
    routeFamily: 7,
  };
}

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
    limit: 10,
    area: "ops",
    realm: "default",
    resource: "primary",
  },
};

const genericResourceRoutes = [
  {
    assertText: "RPC resource inspection",
    domain: "rpc",
    path: "/rpc/default/ops/primary",
    routePath: "/rpc/{realm}/{area}/{resource}",
    module: () => import("@/pages/app/resource-detail"),
  },
  {
    assertText: "Stream resource inspection",
    domain: "stream",
    path: "/stream/default/ops/primary",
    routePath: "/stream/{realm}/{area}/{resource}",
    module: () => import("@/pages/app/resource-detail"),
  },
];

const scheduleHierarchyRoutes = [
  {
    assertText: "default",
    path: "/schedule/default",
    routePath: "/schedule/{realm}",
    module: () => import("@/pages/app/schedule"),
  },
  {
    assertText: "ops",
    path: "/schedule/default/ops",
    routePath: "/schedule/{realm}/{area}",
    module: () => import("@/pages/app/schedule"),
  },
  {
    assertText: "Schedule resource inspection",
    path: "/schedule/default/ops/primary",
    routePath: "/schedule/{realm}/{area}/{resource}",
    module: () => import("@/pages/app/schedule-resource"),
  },
];

const noticeHierarchyRoutes = [
  {
    assertText: "Notice operations",
    domain: "notice",
    path: "/notice/default/ops/primary",
    routePath: "/notice/{realm}/{area}/{resource}",
    module: () => import("@/pages/app/notice"),
  },
  {
    assertText: "GetStatus",
    domain: "notice",
    path: "/notice/default/ops/primary/GetStatus",
    routePath: "/notice/{realm}/{area}/{resource}/{operation}",
    module: () => import("@/pages/app/notice-operation"),
  },
];

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
  mocks.queryStates.queueArea = queryState.fresh(queueAreaDetail, queryOptions());
  mocks.queryStates.queueDeadLetters = queryState.fresh([], queryOptions());
  mocks.queryStates.queueInventory = queryState.fresh(queueInventory, queryOptions());
  mocks.queryStates.queueRealm = queryState.fresh(queueRealmDetail, queryOptions());
  mocks.queryStates.queueResource = queryState.fresh(queueResource, queryOptions());
  mocks.queryStates.queueTimeline = queryState.fresh(queueResource.timeline, queryOptions());
  mocks.queryStates.queueComparison = queryState.fresh(queueComparison, queryOptions());
  mocks.queryStates.kv = queryState.fresh(kvOverview, queryOptions());
  mocks.queryStates.lease = queryState.fresh(leaseOverview, queryOptions());
  mocks.queryStates.leaseRealm = queryState.fresh(leaseRealm, queryOptions());
  mocks.queryStates.leaseArea = queryState.fresh(leaseArea, queryOptions());
  mocks.queryStates.leaseResourceRows = queryState.fresh(
    leaseResourceRowsFixture(),
    queryOptions(),
  );
  mocks.queryStates.notice = queryState.fresh(noticeOverview, queryOptions());
  mocks.queryStates.noticeRealm = queryState.fresh(noticeRealmInventory, queryOptions());
  mocks.queryStates.noticeArea = queryState.fresh(noticeAreaInventory, queryOptions());
  mocks.queryStates.noticeResourceRows = queryState.fresh(noticeResourceRows, queryOptions());
  mocks.queryStates.noticeOperationRows = queryState.fresh(noticeOperationRows, queryOptions());
  mocks.queryStates.rpc = queryState.fresh(rpcOverview, queryOptions());
  mocks.queryStates.rpcRealm = queryState.fresh(rpcRealm, queryOptions());
  mocks.queryStates.rpcArea = queryState.fresh(rpcArea, queryOptions());
  mocks.queryStates.rpcResource = queryState.fresh(rpcResource, queryOptions());
  mocks.queryStates.rpcOperation = queryState.fresh(rpcOperation, queryOptions());
  mocks.queryStates.schedule = queryState.fresh(scheduleOverview, queryOptions());
  mocks.queryStates.scheduleRealm = queryState.fresh(scheduleRealm, queryOptions());
  mocks.queryStates.scheduleArea = queryState.fresh(scheduleArea, queryOptions());
  mocks.queryStates.scheduleResource = queryState.fresh(scheduleResource, queryOptions());
  mocks.queryStates.scheduleExecutionObservations = queryState.fresh(
    scheduleResource.executionObservations,
    queryOptions(),
  );
  mocks.queryStates.scheduleMissedHandoffs = queryState.fresh(
    scheduleResource.missedHandoffs,
    queryOptions(),
  );
  mocks.queryStates.stream = queryState.fresh(streamOverview, queryOptions());
  mocks.queryStates.streamRealm = queryState.fresh(streamRealm, queryOptions());
  mocks.queryStates.streamArea = queryState.fresh(streamArea, queryOptions());
  mocks.queryStates.streamResource = queryState.fresh(streamResource, queryOptions());
  mocks.queryStates.inventory = queryState.fresh(inventory, queryOptions());
  mocks.queryStates.resource = queryState.fresh(resourceDetail, queryOptions());
  mocks.queryStates.kvRows = queryState.fresh(kvRows, queryOptions());
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
        assertText: "Fitz status",
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
        assertText: "Diagnostics",
        module: () => import("@/pages/app/diagnostics"),
        path: "/diagnostics",
        routePath: "/diagnostics",
      },
      {
        assertText: "Queue overview",
        module: () => import("@/pages/app/queue"),
        path: "/queue",
        routePath: "/queue",
      },
      {
        assertText: "Realm totals",
        module: () => import("@/pages/app/queue"),
        path: "/queue/default",
        routePath: "/queue/{realm}",
      },
      {
        assertText: "Area totals",
        module: () => import("@/pages/app/queue"),
        path: "/queue/default/ops",
        routePath: "/queue/{realm}/{area}",
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
        assertText: "KV realm default",
        module: () => import("@/pages/app/kv"),
        path: "/kv/default",
        routePath: "/kv/{realm}",
      },
      {
        assertText: "KV area ops",
        module: () => import("@/pages/app/kv"),
        path: "/kv/default/ops",
        routePath: "/kv/{realm}/{area}",
      },
      {
        assertText: "KV resource",
        module: () => import("@/pages/app/kv-resource"),
        path: "/admin/1/kv/default/ops/primary",
        routePath: "/admin/{family}/kv/{realm}/{area}/{resource}",
      },
      {
        assertText: "Lease overview",
        module: () => import("@/pages/app/lease"),
        path: "/lease",
        routePath: "/lease",
      },
      {
        assertText: "default",
        module: () => import("@/pages/app/lease"),
        path: "/lease/default",
        routePath: "/lease/{realm}",
      },
      {
        assertText: "ops",
        module: () => import("@/pages/app/lease"),
        path: "/lease/default/ops",
        routePath: "/lease/{realm}/{area}",
      },
      {
        assertText: "primary",
        module: () => import("@/pages/app/lease-resource"),
        path: "/lease/default/ops/primary",
        routePath: "/lease/{realm}/{area}/{resource}",
      },
      {
        assertText: "Notice overview",
        module: () => import("@/pages/app/notice"),
        path: "/notice",
        routePath: "/notice",
      },
      {
        assertText: "default",
        module: () => import("@/pages/app/notice"),
        path: "/notice/default",
        routePath: "/notice/{realm}",
      },
      {
        assertText: "ops",
        module: () => import("@/pages/app/notice"),
        path: "/notice/default/ops",
        routePath: "/notice/{realm}/{area}",
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
    ];

    for (const page of genericResourceRoutes) {
      const { default: Component } = await page.module();
      const root = await mountRoute(page.path, page.routePath, Component);

      expect(root.textContent).toContain(page.assertText);
      expect(root.textContent).toContain(`Scope: default / ops / primary`);
      expect(root.querySelectorAll("main#main-content")).toHaveLength(1);

      cleanupApp(root);
      document.body.innerHTML = "";
    }

    for (const page of noticeHierarchyRoutes) {
      const { default: Component } = await page.module();
      const root = await mountRoute(page.path, page.routePath, Component);

      expect(root.textContent).toContain(page.assertText);
      expect(root.querySelectorAll("main#main-content")).toHaveLength(1);

      cleanupApp(root);
      document.body.innerHTML = "";
    }

    for (const page of scheduleHierarchyRoutes) {
      const { default: Component } = await page.module();
      const root = await mountRoute(page.path, page.routePath, Component);

      expect(root.textContent).toContain(page.assertText);
      expect(root.querySelectorAll("main#main-content")).toHaveLength(1);

      cleanupApp(root);
      document.body.innerHTML = "";
    }

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
      expect(text).toMatch(/Live|Healthy|Quiet|Pressure|Attention/);
      expect(text).toContain("Operator guide");

      cleanupApp(root);
      document.body.innerHTML = "";
    }
  });

  it("covers generic detail route loading and error states", async () => {
    for (const page of genericResourceRoutes) {
      mocks.queryStates.resource = queryState.loading(queryOptions());

      const { default: Component } = await page.module();
      const root = await mountRoute(page.path, page.routePath, Component);

      expect(root.textContent).toContain(`Loading ${page.domain.toUpperCase()} resource...`);
      expect(root.textContent).toContain(`Scope: default / ops / primary`);

      cleanupApp(root);
      document.body.innerHTML = "";

      const loadError = `${page.domain} unavailable`;
      mocks.queryStates.resource = queryState.error(
        new Error(loadError),
        undefined,
        queryOptions(),
      );

      const errorRoot = await mountRoute(page.path, page.routePath, Component);

      expect(errorRoot.textContent).toContain(loadError);
      expect(errorRoot.textContent).toContain("Unable to load");

      cleanupApp(errorRoot);
      document.body.innerHTML = "";
    }

    mocks.queryStates.resource = queryState.fresh(resourceDetail, queryOptions());
  });

  it("covers notice drill-down loading and error states", async () => {
    const { default: NoticePage } = await import("@/pages/app/notice");
    const { default: NoticeOperationPage } = await import("@/pages/app/notice-operation");

    mocks.queryStates.notice = queryState.loading(queryOptions());
    const noticeOverviewRoot = await mountRoute("/notice", "/notice", NoticePage);
    expect(noticeOverviewRoot.textContent).toContain("Loading notice overview snapshot...");
    cleanupApp(noticeOverviewRoot);
    document.body.innerHTML = "";

    mocks.queryStates.notice = queryState.error(
      new Error("Notice unavailable"),
      undefined,
      queryOptions(),
    );
    const noticeOverviewError = await mountRoute("/notice", "/notice", NoticePage);
    expect(noticeOverviewError.textContent).toContain("Notice overview loading failure");
    cleanupApp(noticeOverviewError);
    document.body.innerHTML = "";

    mocks.queryStates.notice = queryState.fresh(noticeOverview, queryOptions());
    mocks.queryStates.noticeRealm = queryState.loading(queryOptions());
    const noticeRealmRoot = await mountRoute("/notice/default", "/notice/{realm}", NoticePage);
    expect(noticeRealmRoot.textContent).toContain("Loading notice realm...");
    cleanupApp(noticeRealmRoot);
    document.body.innerHTML = "";

    mocks.queryStates.noticeRealm = queryState.fresh(noticeRealmInventory, queryOptions());
    mocks.queryStates.noticeArea = queryState.loading(queryOptions());
    const noticeAreaRoot = await mountRoute(
      "/notice/default/ops",
      "/notice/{realm}/{area}",
      NoticePage,
    );
    expect(noticeAreaRoot.textContent).toContain("Loading notice area...");
    cleanupApp(noticeAreaRoot);
    document.body.innerHTML = "";

    mocks.queryStates.noticeArea = queryState.fresh(noticeAreaInventory, queryOptions());
    mocks.queryStates.noticeResourceRows = queryState.loading(queryOptions());
    const noticeResourceRoot = await mountRoute(
      "/notice/default/ops/primary",
      "/notice/{realm}/{area}/{resource}",
      NoticePage,
    );
    expect(noticeResourceRoot.textContent).toContain("Loading notice operation rows...");
    cleanupApp(noticeResourceRoot);
    document.body.innerHTML = "";

    mocks.queryStates.noticeResourceRows = queryState.fresh(noticeResourceRows, queryOptions());
    mocks.queryStates.noticeOperationRows = queryState.loading(queryOptions());
    const noticeOperationRoot = await mountRoute(
      "/notice/default/ops/primary/GetStatus",
      "/notice/{realm}/{area}/{resource}/{operation}",
      NoticeOperationPage,
    );
    expect(noticeOperationRoot.textContent).toContain("Loading notice operation deliveries...");
    cleanupApp(noticeOperationRoot);
    document.body.innerHTML = "";

    mocks.queryStates.notice = queryState.fresh(noticeOverview, queryOptions());
    mocks.queryStates.noticeRealm = queryState.fresh(noticeRealmInventory, queryOptions());
    mocks.queryStates.noticeArea = queryState.fresh(noticeAreaInventory, queryOptions());
    mocks.queryStates.noticeResourceRows = queryState.fresh(noticeResourceRows, queryOptions());
    mocks.queryStates.noticeOperationRows = queryState.fresh(noticeOperationRows, queryOptions());
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
    expect(text).toContain("Ephemeral ownership");
    expect(text).toContain("Broker-local owners");
    expect(text).toContain("explicit reacquire");
  });

  it("renders kv state and transactional-pressure priority metrics", async () => {
    mocks.queryStates.kv = queryState.fresh(
      {
        ...kvOverview,
        stats: {
          ...kvOverview.stats,
          keysTotal: 180,
          transactionsActive: 4,
          operationsPerSecond: 4.25,
          commitsFailedTotal: 2,
          invalidTransactionRejectsTotal: 3,
        },
      },
      queryOptions(),
    );

    const { default: KvPage } = await import("@/pages/app/kv");
    const root = await mountRoute("/kv", "/kv", KvPage);
    const text = root.textContent ?? "";
    const labels = [
      "Keys total",
      "Active transactions",
      "Ops / sec",
      "Commit failures",
      "Invalid transaction rejects",
    ];

    let cursor = -1;
    for (const label of labels) {
      const index = text.indexOf(label, cursor + 1);
      expect(index).toBeGreaterThan(cursor);
      cursor = index;
    }

    expect(text).toContain("2");
    expect(text).toContain("3");
    expect(text).toContain("Active transactions");
    expect(text).toContain("Attention");
    expect(text).toContain("authoritative");
    expect(text).toContain("broker-local");
    expect(text).toContain("KV Resources");
    expect(text).toContain("Realm");
    expect(text).toContain("Resources");
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
      if (page.queryKey === "kv") {
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

    mocks.queryStates.lease = queryState.fresh(
      {
        ...leaseOverview,
        realms: [],
      },
      queryOptions(),
    );
    let root = await mountRoute("/lease", "/lease", LeasePage);
    expect(root.textContent).toContain("No lease realms are currently visible.");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.leaseRealm = queryState.fresh({ ...leaseRealm, areas: [] }, queryOptions());
    root = await mountRoute("/lease/default", "/lease/{realm}", LeasePage);
    expect(root.textContent).toContain("No visible lease areas at the current level.");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.leaseArea = queryState.fresh({ ...leaseArea, resources: [] }, queryOptions());
    root = await mountRoute("/lease/default/ops", "/lease/{realm}/{area}", LeasePage);
    expect(root.textContent).toContain("No visible lease resources at the current level.");

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
      "/lease/default/ops/primary",
      "/lease/{realm}/{area}/{resource}",
      LeaseResourcePage,
    );
    expect(root.textContent).toContain("No visible lease ownership rows at the current level.");
  });

  it("renders notice empty states at each hierarchy scope", async () => {
    const { default: NoticePage } = await import("@/pages/app/notice");
    const { default: NoticeOperationPage } = await import("@/pages/app/notice-operation");

    mocks.queryStates.notice = queryState.fresh({ ...noticeOverview, realms: [] }, queryOptions());
    let root = await mountRoute("/notice", "/notice", NoticePage);
    expect(root.textContent).toContain("No notice realms are currently visible.");
    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.noticeRealm = queryState.fresh(
      { ...noticeRealmInventory, areas: [] },
      queryOptions(),
    );
    root = await mountRoute("/notice/default", "/notice/{realm}", NoticePage);
    expect(root.textContent).toContain("No visible notice areas at the current level.");
    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.noticeArea = queryState.fresh(
      { ...noticeAreaInventory, resources: [] },
      queryOptions(),
    );
    root = await mountRoute("/notice/default/ops", "/notice/{realm}/{area}", NoticePage);
    expect(root.textContent).toContain("No visible notice resources at the current level.");
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
      "/notice/default/ops/primary",
      "/notice/{realm}/{area}/{resource}",
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
      "/notice/default/ops/primary/GetStatus",
      "/notice/{realm}/{area}/{resource}/{operation}",
      NoticeOperationPage,
    );
    expect(root.textContent).toContain("No matching notice deliveries are currently visible.");
    cleanupApp(root);
    document.body.innerHTML = "";
  });

  it("updates lease ownership remaining time on the lease resource page", async () => {
    const leaseExpiresAt = new Date(Date.now() + 3000).toISOString();
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
      "/lease/default/ops/primary",
      "/lease/{realm}/{area}/{resource}",
      LeaseResourcePage,
    );
    const initialRemaining = root
      .querySelector("tbody tr")
      ?.querySelectorAll("td")[5]
      ?.textContent?.trim();
    await new Promise<void>((resolve) => setTimeout(resolve, 1200));
    await new Promise<void>((resolve) => queueMicrotask(() => resolve()));

    const updatedRemaining = root
      .querySelector("tbody tr")
      ?.querySelectorAll("td")[5]
      ?.textContent?.trim();

    expect(initialRemaining).toBeTruthy();
    expect(updatedRemaining).toBeTruthy();
    expect(updatedRemaining).not.toBe(initialRemaining);
    expect(root.textContent).toContain("not crash-safe continuity");
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
    expect(text).toContain("Communication flow");
    expect(text).toContain("Existing API");
    expect(text).toContain("Failure signals");
    expect(text).toContain("No matching notice routes");
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

  it("renders schedule pressure-first health and failure metrics", async () => {
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
    const labels = [
      "Is anyone listening?",
      "When is the next run?",
      "Pending fire claims",
      "Oldest pending claim age",
      "Persistence failures",
      "Handoff failures",
    ];

    let cursor = -1;
    for (const label of labels) {
      const index = text.indexOf(label, cursor + 1);
      expect(index).toBeGreaterThan(cursor);
      cursor = index;
    }

    expect(text).toContain("Attention");
    expect(text).toContain("Schedule does not imply durable downstream delivery.");
    expect(text).toContain("Persisted timing claims awaiting live handoff");
    expect(text).toContain("9 live subscription(s)");
    expect(text).toContain("Open a schedule resource");
    expect(text).toContain("Schedule realms");
  });

  it("renders schedule realm, area, and resource drill-down pages", async () => {
    const { default: SchedulePage } = await import("@/pages/app/schedule");
    const realmRoot = await mountRoute("/schedule/default", "/schedule/{realm}", SchedulePage);
    expect(realmRoot.textContent).toContain("Schedule realm");
    expect(realmRoot.textContent).toContain("Schedule areas");
    expect(realmRoot.textContent).toContain("ops");
    cleanupApp(realmRoot);
    document.body.innerHTML = "";

    const areaRoot = await mountRoute(
      "/schedule/default/ops",
      "/schedule/{realm}/{area}",
      SchedulePage,
    );
    expect(areaRoot.textContent).toContain("Schedule area");
    expect(areaRoot.textContent).toContain("Schedule resources");
    expect(areaRoot.textContent).toContain("primary");
    cleanupApp(areaRoot);
    document.body.innerHTML = "";

    const { default: ScheduleResourcePage } = await import("@/pages/app/schedule-resource");
    const resourceRoot = await mountRoute(
      "/schedule/default/ops/primary",
      "/schedule/{realm}/{area}/{resource}",
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

  it("renders stream replay-priority metrics and readable lag buckets", async () => {
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
    const labels = [
      "Events total",
      "Active streams",
      "Active subscriptions",
      "Watermark lag",
      "Ops / sec",
    ];

    let cursor = -1;
    for (const label of labels) {
      const index = text.indexOf(label, cursor + 1);
      expect(index).toBeGreaterThan(cursor);
      cursor = index;
    }

    expect(text).toContain("behind 100+");
    expect(text).toContain("Attention");
    expect(text).toContain("Durable stream history");
    expect(text).toContain("live subscriptions");
    expect(text).toContain("Stream metrics");
    expect(text).toContain("Stream realms");
  });

  it("renders Stream drill-down rollups and committed resource records", async () => {
    const { default: StreamPage } = await import("@/pages/app/stream");
    const { default: StreamResourcePage } = await import("@/pages/app/stream-resource");

    let root = await mountRoute("/stream/default", "/stream/{realm}", StreamPage);
    let text = root.textContent ?? "";
    expect(text).toContain("Stream realm");
    expect(text).toContain("Stream areas");
    expect(text).toContain("Family watermarks");

    root = await mountRoute("/stream/default/ops", "/stream/{realm}/{area}", StreamPage);
    text = root.textContent ?? "";
    expect(text).toContain("Stream area");
    expect(text).toContain("Stream resources");
    expect(text).toContain("events");

    root = await mountRoute(
      "/stream/default/ops/events",
      "/stream/{realm}/{area}/{resource}",
      StreamResourcePage,
    );
    text = root.textContent ?? "";
    expect(text).toContain("Stream resource");
    expect(text).toContain("From offset");
    expect(text).toContain("Stream resource metrics");
    expect(text).toContain('{"ok":true}');
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
    expect(text).toContain("Pending work is in-memory");
    expect(text).toContain("pending requests");
    expect(text).toContain("Communication flow");
    expect(text).toContain("Existing API");
    expect(text).toContain("Failure signals");
    expect(text).toContain("1 matching RPC route");
    expect(text).toContain("Open operations");
  });

  it("renders RPC drill-down resource and operation pages", async () => {
    const { default: RpcPage } = await import("@/pages/app/rpc");
    const { default: RpcResourcePage } = await import("@/pages/app/rpc-resource");
    const { default: RpcOperationPage } = await import("@/pages/app/rpc-operation");

    let root = await mountRoute("/rpc/default", "/rpc/{realm}", RpcPage);
    let text = root.textContent ?? "";
    expect(text).toContain("RPC realm");
    expect(text).toContain("RPC areas");
    expect(text).toContain("ops");

    root = await mountRoute("/rpc/default/ops", "/rpc/{realm}/{area}", RpcPage);
    text = root.textContent ?? "";
    expect(text).toContain("RPC area");
    expect(text).toContain("RPC resources");
    expect(text).toContain("primary");

    root = await mountRoute(
      "/rpc/default/ops/primary",
      "/rpc/{realm}/{area}/{resource}",
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
      "/rpc/default/ops/primary/GetStatus",
      "/rpc/{realm}/{area}/{resource}/{operation}",
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
    expect(text).toContain("Prometheus metrics");
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
    expect(root.textContent).toContain("No visible queues at the current level");
  });

  it("keeps queue overview content visible while refresh is in flight", async () => {
    const { default: QueuePage } = await import("@/pages/app/queue");

    mocks.queryStates.queue = queryState.refreshing(queueOverview, queryOptions());

    const root = await mountRoute("/queue", "/queue", QueuePage);

    expect(root.textContent).toContain("Refreshing");
    expect(root.textContent).toContain("Scope summary");
    expect(root.textContent).toContain("Queue realms");
    expect(root.textContent).toContain("Queue totals");
    expect(root.textContent).toContain("Durable work posture");
    expect(root.textContent).toContain("Subscriptions");
    expect(root.textContent).toContain("Falling behind");
  });

  it("renders queue hierarchy links for realm, area, and queue rows", async () => {
    const { default: QueuePage } = await import("@/pages/app/queue");

    let root = await mountRoute("/admin/1/queue", "/admin/{family}/queue", QueuePage);
    expect(root.querySelector('a[href="/admin/1/queue/default"]')?.textContent).toContain(
      "default",
    );

    cleanupApp(root);
    document.body.innerHTML = "";

    root = await mountRoute("/admin/1/queue/default", "/admin/{family}/queue/{realm}", QueuePage);
    expect(root.textContent).toContain("Realm totals");
    expect(root.querySelector('a[href="/admin/1/queue/default/ops"]')?.textContent).toContain(
      "ops",
    );
    expect(
      root.querySelector('a[href="/admin/1/queue/default/ops/primary"]')?.textContent,
    ).toContain("primary");

    cleanupApp(root);
    document.body.innerHTML = "";

    root = await mountRoute(
      "/admin/1/queue/default/ops",
      "/admin/{family}/queue/{realm}/{area}",
      QueuePage,
    );
    expect(root.textContent).toContain("Area totals");
    expect(
      root.querySelector('a[href="/admin/1/queue/default/ops/primary"]')?.textContent,
    ).toContain("primary");
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
    expect(root.textContent).toContain("Point-in-time durable backlog");
    expect(root.textContent).toContain("Snapshots match");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.resource = queryState.fresh(
      {
        ...resourceDetail,
        comparison: {
          comparisonMode: "resource",
          derived: false,
          metrics: [{ label: "Delta", value: 0 }],
          leftScope: {
            area: "ops",
            realm: "default",
            resource: "primary",
          },
          rightScope: {
            area: "ops",
            realm: "default",
            resource: "secondary",
          },
          summary: "No material difference",
        },
      },
      queryOptions(),
    );

    const { default: KvResourcePage } = await import("@/pages/app/kv-resource");
    root = await mountRoute(
      "/admin/1/kv/default/ops/primary?startsWith=user%3A",
      "/admin/{family}/kv/{realm}/{area}/{resource}",
      KvResourcePage,
    );

    expect(root.textContent).toContain("Key preview");
    expect(root.textContent).toContain("user:1");
    expect(root.textContent).toContain("alice");
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
