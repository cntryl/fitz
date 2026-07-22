import { beforeEach, vi } from "vite-plus/test";

const mocks = vi.hoisted(() => ({
  apiv1: {
    compareQueueResourceSnapshots: vi.fn(),
    getQueueArea: vi.fn(),
    getQueueRealm: vi.fn(),
    getQueueResource: vi.fn(),
    getQueueStats: vi.fn(),
    getScheduleResource: vi.fn(),
    listScheduleExecutionObservations: vi.fn(),
    listScheduleAreas: vi.fn(),
    listQueueAreas: vi.fn(),
    listQueueDeadLetters: vi.fn(),
    listQueueInflightEntries: vi.fn(),
    listQueueRealms: vi.fn(),
    listQueueResources: vi.fn(),
    listQueueResourceEvents: vi.fn(),
    listKvAreas: vi.fn(),
    listKvRealms: vi.fn(),
    listKvTransactions: vi.fn(),
    listKvResourceEvents: vi.fn(),
    listKvResources: vi.fn(),
    getKvCommittedValue: vi.fn(),
    getKvResource: vi.fn(),
    getLeaseStats: vi.fn(),
    listLeaseAreas: vi.fn(),
    listLeaseRealms: vi.fn(),
    listLeaseResources: vi.fn(),
    listScheduleResources: vi.fn(),
    readStreamResourceRecords: vi.fn(),
    searchAdminState: vi.fn(),
    searchLeaseOwnership: vi.fn(),
    listNoticeAreas: vi.fn(),
    listNoticeResources: vi.fn(),
    getRpcOperation: vi.fn(),
    getRpcResource: vi.fn(),
    listRpcAreas: vi.fn(),
    listRpcResources: vi.fn(),
    searchNoticeDeliveries: vi.fn(),
    searchRpcCalls: vi.fn(),
    searchScheduleMissedHandoffs: vi.fn(),
    searchStreamRecords: vi.fn(),
    getStreamAreaWatermarks: vi.fn(),
    getStreamRealmWatermarks: vi.fn(),
    getStreamResource: vi.fn(),
    listStreamAreas: vi.fn(),
    listStreamResources: vi.fn(),
    scanKvCommittedPrefix: vi.fn(),
  },
}));

export function serviceContractMocks() {
  return mocks;
}

vi.mock("@/adapters", () => ({
  apiBody: (body: unknown, options = {}) => ({ ...options, body }),
  apiParams: (params: unknown, options = {}) => ({ ...options, params }),
  apiParamsQuery: (params: unknown, query: unknown, options = {}) => ({
    ...options,
    params,
    query,
  }),
  apiQuery: (query: unknown, options = {}) => ({ ...options, query }),
  apiv1: mocks.apiv1,
}));

const healthyDiagnostics = {
  confidence: 1,
  contention_count: 0,
  current_stage: "healthy",
  explanation_hints: [],
  failure_count: 0,
  recent_transition_count: 0,
  severity: "informational",
  trend: "steady",
  waiter_count: 0,
};

beforeEach(() => {
  vi.clearAllMocks();

  mocks.apiv1.listKvRealms.mockResolvedValue({
    ok: true,
    status: 200,
    data: { realms: [{ realm: "default" }] },
  });
  mocks.apiv1.listKvAreas.mockResolvedValue({
    ok: true,
    status: 200,
    data: { areas: [{ area: "ops" }] },
  });
  mocks.apiv1.listKvResources.mockResolvedValue({
    ok: true,
    status: 200,
    data: { resources: [{ resource: "primary" }] },
  });
  mocks.apiv1.getKvResource.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      diagnostics: healthyDiagnostics,
      keys_total: 12,
      operations_per_second: 1.5,
      transactions_active: 2,
    },
  });
  mocks.apiv1.listKvResourceEvents.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      derived: false,
      events: [],
    },
  });
  mocks.apiv1.listKvTransactions.mockResolvedValue({
    ok: true,
    status: 200,
    data: { transactions: [] },
  });
  mocks.apiv1.getKvCommittedValue.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      found: true,
      key: { base64: "dXNlcjox", len_bytes: 6, utf8: "user:1" },
      realm: "default",
      resource: "primary",
      route_family: 7,
      value: { base64: "YWxpY2U=", len_bytes: 5, utf8: "alice" },
    },
  });
  mocks.apiv1.scanKvCommittedPrefix.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      has_more: false,
      items: [
        {
          key: { base64: "dXNlcjox", len_bytes: 6, utf8: "user:1" },
          value: { base64: "YWxpY2U=", len_bytes: 5, utf8: "alice" },
        },
      ],
      limit: 50,
      prefix: { base64: "dXNlcjo=", len_bytes: 5, utf8: "user:" },
      realm: "default",
      resource: "primary",
      route_family: 7,
    },
  });
  mocks.apiv1.listLeaseRealms.mockResolvedValue({
    ok: true,
    status: 200,
    data: { realms: [{ realm: "default" }] },
  });
  mocks.apiv1.getLeaseStats.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      acquire_timeouts_total: 0,
      forced_releases_total: 0,
      invalid_token_rejects_total: 0,
      leases_active: 3,
      oldest_lease_age_seconds: 42,
      operations_per_second: 1.5,
      waiter_depth: 0,
    },
  });
  mocks.apiv1.listLeaseAreas.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      areas: [{ area: "ops", realm: "default", realm_count: 1 }],
    },
  });
  mocks.apiv1.listLeaseResources.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      realm: "default",
      resources: [{ resource: "primary" }],
    },
  });
  mocks.apiv1.searchStreamRecords.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      from_offset: 3,
      has_more: false,
      limit: 10,
      realm: "default",
      records: [],
      resource: "events",
      route_family: 7,
    },
  });
  mocks.apiv1.readStreamResourceRecords.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      from_offset: 0,
      has_more: false,
      limit: 10,
      realm: "default",
      records: [],
      resource: "events",
      route_family: 7,
    },
  });
  mocks.apiv1.listScheduleAreas.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      areas: [{ area: "ops" }],
      realm: "default",
    },
  });
  mocks.apiv1.listScheduleResources.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      realm: "default",
      resources: [{ resource: "reconcile" }],
    },
  });
  mocks.apiv1.getScheduleResource.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      cron: "*/5 * * * *",
      diagnostics: healthyDiagnostics,
      enabled: true,
      executions_total: 3,
      next_run: "2026-05-21T13:05:00.000Z",
      realm: "default",
      resource: "reconcile",
    },
  });
  mocks.apiv1.listScheduleExecutionObservations.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      limit: 10,
      observations: [],
      realm: "default",
      resource: "reconcile",
      route_family: 7,
    },
  });
  mocks.apiv1.searchScheduleMissedHandoffs.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      limit: 10,
      observations: [],
      route_family: 7,
    },
  });
  mocks.apiv1.searchLeaseOwnership.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      items: [],
      limit: 10,
      route_family: 7,
    },
  });
  mocks.apiv1.searchNoticeDeliveries.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      limit: 10,
      observations: [],
      route_family: 7,
    },
  });
  mocks.apiv1.listNoticeAreas.mockResolvedValue({
    ok: true,
    status: 200,
    data: { areas: [{ area: "ops", realm: "default", realm_count: 1 }] },
  });
  mocks.apiv1.listNoticeResources.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      realm: "default",
      resources: [{ resource: "primary" }],
    },
  });
  mocks.apiv1.searchRpcCalls.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      limit: 10,
      observations: [],
      route_family: 7,
    },
  });
  mocks.apiv1.listRpcAreas.mockResolvedValue({
    ok: true,
    status: 200,
    data: { areas: [{ area: "ops" }], realm: "default" },
  });
  mocks.apiv1.listRpcResources.mockResolvedValue({
    ok: true,
    status: 200,
    data: { area: "ops", realm: "default", resources: [{ resource: "primary" }] },
  });
  mocks.apiv1.getRpcResource.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      operations: [{ operation: "GetStatus" }],
      realm: "default",
      resource: "primary",
    },
  });
  mocks.apiv1.getRpcOperation.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      diagnostics: healthyDiagnostics,
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
  });
  mocks.apiv1.listStreamAreas.mockResolvedValue({
    ok: true,
    status: 200,
    data: { areas: [{ area: "ops" }], realm: "default" },
  });
  mocks.apiv1.listStreamResources.mockResolvedValue({
    ok: true,
    status: 200,
    data: { area: "ops", realm: "default", resources: [{ resource: "events" }] },
  });
  mocks.apiv1.getStreamRealmWatermarks.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area_count: 1,
      family_watermarks: [{ family: 7, watermark: 10 }],
      realm: "default",
      resource_count: 1,
    },
  });
  mocks.apiv1.getStreamAreaWatermarks.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      family_watermarks: [{ family: 7, watermark: 10 }],
      realm: "default",
      resource_count: 1,
    },
  });
  mocks.apiv1.getStreamResource.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      diagnostics: healthyDiagnostics,
      offset: 0,
      realm: "default",
      resource: "events",
      sessions_active: 1,
      size_bytes: 128,
      watermark: 10,
    },
  });
  mocks.apiv1.searchAdminState.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      domain: "queue",
      limit: 10,
      query: "settlement",
      results: [
        {
          area: "ops",
          domain: "queue",
          health: "backlogged",
          href: "/queue/realms/default/areas/ops/resources/settlement",
          id: "queue:resource:7:default:ops:settlement",
          kind: "resource",
          matched_fields: ["resource"],
          metadata: { messages_ready: "3" },
          operation: null,
          realm: "default",
          resource: "settlement",
          route_family: "7",
          summary: "3 ready",
          title: "settlement",
        },
      ],
      route_family: "7",
      total: 1,
      truncated: false,
    },
  });

  const queueOperationalDto = {
    complete_success_total: 3,
    enqueue_success_total: 8,
    in_rate_per_second: 1.5,
    messages_dead_lettered: 0,
    messages_delayed: 1,
    messages_inflight: 2,
    messages_ready: 3,
    messages_total: 6,
    oldest_backlog_age_seconds: 30,
    out_rate_per_second: 0.5,
    status: "falling_behind",
    subscriptions_active: 2,
  };

  mocks.apiv1.listQueueRealms.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      realms: [
        {
          ...queueOperationalDto,
          area_count: 1,
          queue_count: 1,
          realm: "default",
        },
      ],
    },
  });
  mocks.apiv1.getQueueStats.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      backlog_age_buckets: { over_15m: 0, under_15m: 0, under_1m: 1, under_5m: 0 },
      complete_rejected_total: 0,
      completes_total: 3,
      dead_letter_transitions_total: 0,
      delay_age_buckets: { over_15m: 0, under_15m: 0, under_1m: 1, under_5m: 0 },
      diagnostics: healthyDiagnostics,
      enqueues_total: 8,
      extends_total: 0,
      failure_total: 0,
      inflight_active: 2,
      messages_dead_lettered: 0,
      messages_delayed: 1,
      messages_pending: 4,
      messages_ready: 3,
      notify_drops_total: 0,
      oldest_backlog_age_seconds: 30,
      oldest_message_age_seconds: 30,
      operations_per_second: 1.5,
      redeliveries_total: 0,
      releases_total: 0,
      requests_total: 8,
      reserves_total: 2,
      success_total: 8,
    },
  });
  mocks.apiv1.listQueueAreas.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      areas: [
        {
          ...queueOperationalDto,
          area: "ops",
          queue_count: 1,
          realm: "default",
        },
      ],
      realm: "default",
    },
  });
  mocks.apiv1.listQueueResources.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      realm: "default",
      resources: [
        {
          ...queueOperationalDto,
          area: "ops",
          family_count: 1,
          realm: "default",
          resource: "primary",
        },
      ],
    },
  });
  mocks.apiv1.getQueueRealm.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      ...queueOperationalDto,
      area_count: 1,
      areas: [{ ...queueOperationalDto, area: "ops", queue_count: 1, realm: "default" }],
      queue_count: 1,
      queues: [
        {
          ...queueOperationalDto,
          area: "ops",
          family_count: 1,
          realm: "default",
          resource: "primary",
        },
      ],
      realm: "default",
    },
  });
  mocks.apiv1.getQueueArea.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      ...queueOperationalDto,
      area: "ops",
      queue_count: 1,
      queues: [
        {
          ...queueOperationalDto,
          area: "ops",
          family_count: 1,
          realm: "default",
          resource: "primary",
        },
      ],
      realm: "default",
    },
  });

  mocks.apiv1.getQueueResource.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      backlog_age_buckets: { over_15m: 0, under_15m: 0, under_1m: 1, under_5m: 0 },
      complete_success_total: 3,
      delay_age_buckets: { over_15m: 0, under_15m: 0, under_1m: 1, under_5m: 0 },
      diagnostics: healthyDiagnostics,
      enqueue_success_total: 8,
      in_rate_per_second: 1.5,
      messages_dead_lettered: 0,
      messages_delayed: 1,
      messages_inflight: 2,
      messages_ready: 3,
      messages_total: 6,
      oldest_backlog_age_seconds: 30,
      oldest_message_age_seconds: 30,
      out_rate_per_second: 0.5,
      realm: "default",
      resource: "primary",
      status: "falling_behind",
      subscriptions_active: 2,
    },
  });
  mocks.apiv1.listQueueInflightEntries.mockResolvedValue({
    ok: true,
    status: 200,
    data: { inflight: [] },
  });
  mocks.apiv1.listQueueDeadLetters.mockResolvedValue({
    ok: true,
    status: 200,
    data: { messages: [] },
  });
  mocks.apiv1.listQueueResourceEvents.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      derived: false,
      diagnostics: healthyDiagnostics,
      domain: "queue",
      events: [],
      limit: 8,
      realm: "default",
      resource: "primary",
    },
  });
  mocks.apiv1.compareQueueResourceSnapshots.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      comparison_mode: "resource",
      delta: {},
      derived: false,
      left: {
        diagnostics: healthyDiagnostics,
        metrics: {},
        scope: { area: "ops", realm: "default", resource: "primary" },
      },
      right: {
        diagnostics: healthyDiagnostics,
        metrics: {},
        scope: { area: "ops", realm: "default", resource: "secondary" },
      },
      summary: "Snapshots match",
    },
  });
});
