import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

const mocks = vi.hoisted(() => ({
  apiv1: {
    compareQueueResourceSnapshots: vi.fn(),
    getQueueResource: vi.fn(),
    listQueueDeadLetters: vi.fn(),
    listQueueInflightEntries: vi.fn(),
    listQueueResourceEvents: vi.fn(),
    listKvAreas: vi.fn(),
    listKvRealms: vi.fn(),
    listKvTransactions: vi.fn(),
    listKvResourceEvents: vi.fn(),
    listKvResources: vi.fn(),
    getKvResource: vi.fn(),
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
    expect(mocks.apiv1.listKvTransactions).toHaveBeenCalledWith(
      "default",
      "ops",
      "primary",
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
});
