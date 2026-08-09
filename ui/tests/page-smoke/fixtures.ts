import {
  dashboardDiagnostics as diagnostics,
  healthyGlobalDiagnostics,
  topologyAppLane,
  topologyOverview,
} from "../fixtures/topology";

export { diagnostics, healthyGlobalDiagnostics, topologyAppLane, topologyOverview };

export const realm = { realm: "default" };

export const inventory = {
  domain: "kv",
  realms: [
    {
      areas: [
        {
          area: "ops",
          resourceEntries: [
            {
              estimateComplete: true,
              estimatedRecordCount: 300,
              estimatedStorageBytes: 16_384,
              readLatencyAvgMs: 2.4,
              readLatencyP95Ms: 12.5,
              resource: "primary",
              transactionsActive: 1,
              writeLatencyAvgMs: 4.1,
              writeLatencyP95Ms: 18.3,
            },
          ],
          resources: ["primary"],
        },
      ],
      realm: "default",
    },
  ],
};

export const kvRows = {
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

export const queueOverview = {
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

export const queueInventory = {
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

export const queueAreaRow = {
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

export const queueResourceRow = {
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

export const queueRealmDetail = {
  ...queueOverview.realms[0],
  areas: [queueAreaRow],
  queues: [queueResourceRow],
};

export const queueAreaDetail = {
  ...queueAreaRow,
  queues: [queueResourceRow],
};

export const kvOverview = {
  realms: [realm],
  stats: {
    commitsFailedTotal: 0,
    invalidTransactionRejectsTotal: 0,
    keysTotal: 12,
    operationsPerSecond: 2.5,
    transactionsActive: 1,
  },
};

export const leaseOverview = {
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

export const noticeOverview = {
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

export const noticeRealmInventory = {
  areas: [
    {
      area: "ops",
      realm: "default",
      resources: ["primary"],
    },
  ],
  realm: "default",
};

export const noticeAreaInventory = {
  area: "ops",
  realm: "default",
  resources: ["primary"],
};

export const noticeResourceRows = {
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

export const noticeOperationRows = {
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

export const rpcOverview = {
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

export const rpcRealm = {
  areas: [{ area: "ops", realm: "default", resources: ["primary"] }],
  realm: "default",
};

export const rpcArea = {
  area: "ops",
  realm: "default",
  resources: ["primary"],
};

export const rpcResource = {
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

export const rpcOperation = {
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

export const scheduleOverview = {
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

export const scheduleRealm = {
  areas: [{ area: "ops", resources: ["primary"] }],
  realm: "default",
  resourceCount: 1,
};

export const scheduleArea = {
  area: "ops",
  realm: "default",
  resourceCount: 1,
  resources: ["primary"],
};

export const scheduleResource = {
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

export const streamOverview = {
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

export const streamRealm = {
  areaCount: 1,
  areas: [{ area: "ops", resources: ["events"] }],
  familyWatermarks: [{ family: 7, watermark: 10 }],
  realm: "default",
  resourceCount: 1,
};

export const streamArea = {
  area: "ops",
  familyWatermarks: [{ family: 7, watermark: 10 }],
  realm: "default",
  resourceCount: 1,
  resources: ["events"],
};

export const streamResource = {
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

export const domainOverviews = [
  {
    assertText: "KV tables",
    inventoryKey: "inventory",
    module: () => import("@/pages/app/kv"),
    path: "/kv",
    routePath: "/kv",
    emptyText: "No KV tables are currently visible.",
    errorText: "KV inventory unavailable",
    errorTitle: "Unable to load KV tables",
    loadingText: "Loading KV tables",
    resourceHref: "/admin/1/kv/default/ops/primary",
    statLabels: ["Domain keys", "Active txns", "Ops / sec"],
  },
  {
    assertText: "Lease inventory",
    inventoryKey: "inventory",
    module: () => import("@/pages/app/lease"),
    path: "/lease",
    routePath: "/lease",
    errorTitle: "Unable to load lease inventory",
    emptyText: "No lease resources are currently visible.",
    errorText: "Lease inventory unavailable",
    loadingText: "Loading lease inventory...",
    resourceHref: "/admin/1/lease/default/ops/primary",
    statLabels: ["Active leases", "Waiters", "Oldest lease"],
  },
  {
    assertText: "Notice inventory",
    inventoryKey: "inventory",
    module: () => import("@/pages/app/notice"),
    path: "/notice",
    routePath: "/notice",
    errorTitle: "Unable to load notice inventory",
    emptyText: "No notice resources are currently visible.",
    errorText: "Notice inventory unavailable",
    loadingText: "Loading notice inventory...",
    resourceHref: "/admin/1/notice/default/ops/primary",
    statLabels: ["Subscriptions", "Active operation routes", "Publishes / sec"],
  },
  {
    assertText: "RPC inventory",
    inventoryKey: "inventory",
    module: () => import("@/pages/app/rpc"),
    path: "/rpc",
    routePath: "/rpc",
    emptyText: "No RPC resources are currently visible.",
    errorTitle: "Unable to load RPC inventory",
    errorText: "RPC inventory unavailable",
    loadingText: "Loading RPC inventory...",
    resourceHref: "/admin/1/rpc/default/ops/primary",
    statLabels: ["Pending", "Workers", "Ops / sec"],
  },
  {
    assertText: "Schedule inventory",
    inventoryKey: "inventory",
    module: () => import("@/pages/app/schedule"),
    path: "/schedule",
    routePath: "/schedule",
    emptyText: "No schedule resources are currently visible.",
    errorTitle: "Unable to load schedule inventory",
    errorText: "Schedule inventory unavailable",
    loadingText: "Loading schedule inventory...",
    resourceHref: "/admin/1/schedule/default/ops/primary",
    statLabels: ["Active", "Pending claims", "Handoffs / min"],
  },
  {
    assertText: "Stream inventory",
    inventoryKey: "inventory",
    module: () => import("@/pages/app/stream"),
    path: "/stream",
    routePath: "/stream",
    emptyText: "No stream resources are currently visible.",
    errorTitle: "Unable to load stream inventory",
    errorText: "Stream inventory unavailable",
    loadingText: "Loading stream inventory",
    resourceHref: "/admin/1/stream/default/ops/primary",
    statLabels: ["Committed events", "Streams", "Subscriptions"],
  },
  {
    assertText: "Queue inventory",
    inventoryKey: "queueInventory",
    module: () => import("@/pages/app/queue"),
    path: "/queue",
    routePath: "/queue",
    emptyText: "No queue resources are currently visible.",
    errorTitle: "Unable to load queue inventory",
    errorText: "Queue inventory unavailable",
    loadingText: "Loading queue inventory",
    resourceHref: "/admin/1/queue/default/ops/primary",
    statLabels: ["Ready", "In flight", "Dead letters"],
  },
];

export const systemOverview = {
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
    lease: {
      ...leaseOverview.stats,
      failureTotal: 0,
      requestsTotal: 1,
      successTotal: 1,
    },
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

export const emptyTopology = {
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

export const metricsOverview = {
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

export const activeSessions = {
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

export const queueResource = {
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

export const leaseRealm = {
  areas: [
    {
      area: "ops",
      realm: "default",
      resources: ["primary"],
    },
  ],
  realm: "default",
};

export const leaseArea = {
  area: "ops",
  realm: "default",
  resources: ["primary"],
};

export function leaseResourceRowsFixture(expiresOffsetSeconds = 120) {
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

export const scheduleHierarchyRoutes = [
  {
    assertText: "Schedule inventory",
    path: "/schedule/default",
    routePath: "/schedule/{realm}",
    module: () => import("@/pages/app/schedule"),
  },
  {
    assertText: "Schedule inventory",
    path: "/schedule/default/ops",
    routePath: "/schedule/{realm}/{area}",
    module: () => import("@/pages/app/schedule"),
  },
  {
    assertText: "Schedule resource inspection",
    path: "/admin/1/schedule/default/ops/primary",
    routePath: "/admin/{family}/schedule/{realm}/{area}/{resource}",
    module: () => import("@/pages/app/schedule-resource"),
  },
];

export const noticeHierarchyRoutes = [
  {
    assertText: "Notice operations",
    domain: "notice",
    path: "/admin/1/notice/default/ops/primary",
    routePath: "/admin/{family}/notice/{realm}/{area}/{resource}",
    module: () => import("@/pages/app/notice"),
  },
  {
    assertText: "GetStatus",
    domain: "notice",
    path: "/admin/1/notice/default/ops/primary/GetStatus",
    routePath: "/admin/{family}/notice/{realm}/{area}/{resource}/{operation}",
    module: () => import("@/pages/app/notice-operation"),
  },
];
