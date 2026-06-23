import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

const mocks = vi.hoisted(() => ({
  apiv1: {
    compareQueueResourceSnapshots: vi.fn(),
    getQueueResource: vi.fn(),
    listScheduleExecutionObservations: vi.fn(),
    listQueueDeadLetters: vi.fn(),
    listQueueInflightEntries: vi.fn(),
    listQueueResourceEvents: vi.fn(),
    listKvAreas: vi.fn(),
    listKvRealms: vi.fn(),
    listKvTransactions: vi.fn(),
    listKvResourceEvents: vi.fn(),
    listKvResources: vi.fn(),
    getKvCommittedValue: vi.fn(),
    getKvResource: vi.fn(),
    readStreamResourceRecords: vi.fn(),
    searchAdminState: vi.fn(),
    searchLeaseOwnership: vi.fn(),
    searchNoticeDeliveries: vi.fn(),
    searchRpcCalls: vi.fn(),
    searchScheduleMissedHandoffs: vi.fn(),
    searchStreamRecords: vi.fn(),
    scanKvCommittedPrefix: vi.fn(),
  },
}));

vi.mock("@/adapters", () => ({
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
  mocks.apiv1.searchRpcCalls.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      limit: 10,
      observations: [],
      route_family: 7,
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

  mocks.apiv1.getQueueResource.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      diagnostics: healthyDiagnostics,
      messages_dead_lettered: 0,
      messages_delayed: 1,
      messages_inflight: 2,
      messages_ready: 3,
      messages_total: 6,
      oldest_message_age_seconds: 30,
      realm: "default",
      resource: "primary",
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

describe("service endpoint contracts", () => {
  it("fans out generic resource inventory requests through the domain endpoints", async () => {
    const { resourceService } = await import("@/features/resource/resource-service");

    await expect(resourceService.getResourceInventory("kv")).resolves.toEqual({
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
    });

    expect(mocks.apiv1.listKvRealms).toHaveBeenCalledTimes(1);
    expect(mocks.apiv1.listKvAreas).toHaveBeenCalledWith("default", {});
    expect(mocks.apiv1.listKvResources).toHaveBeenCalledWith("default", "ops", {});
  });

  it("fans out generic resource detail requests through detail, timeline, and related endpoints", async () => {
    const { resourceService } = await import("@/features/resource/resource-service");

    await resourceService.getResource(
      "kv",
      { area: "ops", realm: "default", resource: "primary" },
      null,
    );

    expect(mocks.apiv1.getKvResource).toHaveBeenCalledWith("default", "ops", "primary", {});
    expect(mocks.apiv1.listKvResourceEvents).toHaveBeenCalledWith(
      "default",
      "ops",
      "primary",
      { limit: 20 },
      {},
    );
    expect(mocks.apiv1.listKvTransactions).toHaveBeenCalledWith("default", "ops", "primary", {});
  });

  it("loads committed KV exact key reads through the committed value endpoint", async () => {
    const { kvService } = await import("@/features/kv/kv-service");

    await expect(
      kvService.getCommittedValue(
        { area: "ops", realm: "default", resource: "primary", routeFamily: 7 },
        "user:1",
        "utf8",
      ),
    ).resolves.toMatchObject({
      found: true,
      routeFamily: 7,
      value: { utf8: "alice" },
    });

    expect(mocks.apiv1.getKvCommittedValue).toHaveBeenCalledWith(
      "default",
      "ops",
      "primary",
      {
        key: "user:1",
        key_encoding: "utf8",
        route_family: 7,
      },
      {},
    );
  });

  it("loads committed KV prefix scans through the prefix endpoint", async () => {
    const { kvService } = await import("@/features/kv/kv-service");

    await expect(
      kvService.scanCommittedPrefix(
        { area: "ops", realm: "default", resource: "primary", routeFamily: 7 },
        "user:",
        "utf8",
        25,
      ),
    ).resolves.toMatchObject({
      hasMore: false,
      items: [{ key: { utf8: "user:1" }, value: { utf8: "alice" } }],
      routeFamily: 7,
    });

    expect(mocks.apiv1.scanKvCommittedPrefix).toHaveBeenCalledWith(
      "default",
      "ops",
      "primary",
      {
        key_encoding: "utf8",
        limit: 25,
        prefix: "user:",
        route_family: 7,
      },
      {},
    );
  });

  it("loads queue resource detail from the expected queue resource endpoints", async () => {
    const { queueResourceService } = await import("@/features/queue/queue-resource-service");

    await queueResourceService.getResource({
      area: "ops",
      realm: "default",
      resource: "primary",
    });

    expect(mocks.apiv1.getQueueResource).toHaveBeenCalledWith("default", "ops", "primary", {});
    expect(mocks.apiv1.listQueueInflightEntries).toHaveBeenCalledWith(
      "default",
      "ops",
      "primary",
      {},
    );
    expect(mocks.apiv1.listQueueDeadLetters).toHaveBeenCalledWith(
      "default",
      "ops",
      "primary",
      undefined,
      {},
    );
    expect(mocks.apiv1.listQueueResourceEvents).toHaveBeenCalledWith(
      "default",
      "ops",
      "primary",
      { limit: 8 },
      {},
    );
  });

  it("loads queue resource comparisons from the comparison endpoint", async () => {
    const { queueResourceService } = await import("@/features/queue/queue-resource-service");

    await queueResourceService.compareResource(
      { area: "ops", realm: "default", resource: "primary" },
      { area: "ops", family: 7, realm: "default", resource: "secondary" },
    );

    expect(mocks.apiv1.compareQueueResourceSnapshots).toHaveBeenCalledWith(
      "default",
      "ops",
      "primary",
      {
        against_area: "ops",
        against_family: 7,
        against_realm: "default",
        against_resource: "secondary",
      },
      {},
    );
  });

  it("loads stream records through route-family scoped stream endpoints", async () => {
    const { streamService } = await import("@/features/stream/stream-service");

    await streamService.searchRecords({
      area: "ops",
      discriminator: "invoice",
      fromOffset: 3,
      limit: 10,
      realm: "default",
      resource: "events",
      routeFamily: 7,
    });
    await streamService.readResourceRecords({
      area: "ops",
      discriminator: "invoice",
      fromOffset: 0,
      limit: 10,
      realm: "default",
      resource: "events",
      routeFamily: 7,
    });

    expect(mocks.apiv1.searchStreamRecords).toHaveBeenCalledWith(
      {
        area: "ops",
        discriminator: "invoice",
        from_offset: 3,
        limit: 10,
        realm: "default",
        resource: "events",
        route_family: 7,
      },
      {},
    );
    expect(mocks.apiv1.readStreamResourceRecords).toHaveBeenCalledWith(
      "default",
      "ops",
      "events",
      {
        discriminator: "invoice",
        from_offset: 0,
        limit: 10,
        route_family: 7,
      },
      {},
    );
  });

  it("loads schedule execution and missed handoff observations through schedule endpoints", async () => {
    const { scheduleService } = await import("@/features/schedule/schedule-service");

    await scheduleService.listExecutionObservations({
      area: "ops",
      limit: 10,
      realm: "default",
      resource: "reconcile",
      routeFamily: 7,
    });
    await scheduleService.searchMissedHandoffs({
      area: "ops",
      limit: 10,
      realm: "default",
      resource: "reconcile",
      routeFamily: 7,
    });

    expect(mocks.apiv1.listScheduleExecutionObservations).toHaveBeenCalledWith(
      "default",
      "ops",
      "reconcile",
      {
        limit: 10,
        route_family: 7,
      },
      {},
    );
    expect(mocks.apiv1.searchScheduleMissedHandoffs).toHaveBeenCalledWith(
      {
        area: "ops",
        limit: 10,
        realm: "default",
        resource: "reconcile",
        route_family: 7,
      },
      {},
    );
  });

  it("loads lease ownership searches through the lease search endpoint", async () => {
    const { leaseService } = await import("@/features/lease/lease-service");

    await leaseService.searchOwnership({
      area: "ops",
      limit: 10,
      owner: "worker-1",
      realm: "default",
      resource: "settlement",
      routeFamily: 7,
      state: "contention",
    });

    expect(mocks.apiv1.searchLeaseOwnership).toHaveBeenCalledWith(
      {
        area: "ops",
        limit: 10,
        owner: "worker-1",
        realm: "default",
        resource: "settlement",
        route_family: 7,
        state: "contention",
      },
      {},
    );
  });

  it("loads communication evidence through notice delivery and RPC call endpoints", async () => {
    const { noticeService } = await import("@/features/notice/notice-service");
    const { rpcService } = await import("@/features/rpc/rpc-service");

    await noticeService.searchDeliveries({
      area: "ops",
      limit: 10,
      query: "events",
      realm: "default",
      resource: "events",
      routeFamily: 7,
    });
    await rpcService.searchCalls({
      area: "ops",
      correlationId: "corr-1",
      limit: 10,
      operation: "sync",
      query: "profile",
      realm: "default",
      resource: "profile",
      routeFamily: 7,
    });

    expect(mocks.apiv1.searchNoticeDeliveries).toHaveBeenCalledWith(
      {
        area: "ops",
        limit: 10,
        q: "events",
        realm: "default",
        resource: "events",
        route_family: 7,
      },
      {},
    );
    expect(mocks.apiv1.searchRpcCalls).toHaveBeenCalledWith(
      {
        area: "ops",
        correlation_id: "corr-1",
        limit: 10,
        operation: "sync",
        q: "profile",
        realm: "default",
        resource: "profile",
        route_family: 7,
      },
      {},
    );
  });

  it("loads global admin search through the search endpoint with route-family scope", async () => {
    const { searchService } = await import("@/features/search/search-service");

    await expect(
      searchService.searchAdminState({
        area: "ops",
        domain: "queue",
        limit: 10,
        query: "settlement",
        realm: "default",
        resource: "settlement",
        routeFamily: "7",
      }),
    ).resolves.toMatchObject({
      results: [{ routeFamily: "7", title: "settlement" }],
      routeFamily: "7",
      total: 1,
    });

    expect(mocks.apiv1.searchAdminState).toHaveBeenCalledWith(
      {
        area: "ops",
        domain: "queue",
        limit: 10,
        operation: undefined,
        q: "settlement",
        realm: "default",
        resource: "settlement",
        route_family: "7",
      },
      {},
    );
  });
});
