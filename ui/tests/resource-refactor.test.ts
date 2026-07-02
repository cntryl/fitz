import { beforeEach, describe, expect, it, vi } from "vite-plus/test";
import {
  filterDomainResourceInventoryRows,
  sortDomainResourceInventoryRows,
  type DomainResourceMetricColumn,
} from "@/components/shared/domain-resource-inventory-table";
import { deadLetterDialogCopy } from "@/features/queue/queue-dead-letter-dialog";
import { communicationModeAdapters } from "@/features/communication/communication-mode-adapters";
import {
  describeQueueState,
  formatQueueScope,
  parseFamilyInput,
} from "@/features/queue/queue-resource-presenters";
import {
  getResourceDomainAdapter,
  resourceDomainAdapterDomains,
} from "@/features/resource/resource-domain-adapters";
import { resourceService } from "@/features/resource/resource-service";
import { buildOverviewStatus } from "@/features/overview/overview-status";
import {
  overviewDomainIssueDescriptors,
  overviewDomainSignal,
} from "@/features/overview/overview-domain-rules";
import {
  domainScopeHref,
  domainSegments,
  genericResourceDomainSegments,
} from "@/shared/navigation/domains";

const mocks = vi.hoisted(() => ({
  apiv1: {
    getNoticeResource: vi.fn(),
    listNoticeAreas: vi.fn(),
    listNoticeRealms: vi.fn(),
    listNoticeResourceEvents: vi.fn(),
    listNoticeResources: vi.fn(),
    listNoticeSubscriptions: vi.fn(),
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

const healthyGlobalDiagnostics = {
  hotspots: [],
  incident_summary: {
    confidence: 1,
    explanation: "No active pressure detected",
    recommended_next_query: "No follow-up needed",
    severity: "informational" as const,
    suggested_next_queries: [],
    status: "healthy" as const,
    title: "Healthy",
  },
  last_significant_transition_at: null,
};

function overviewDomainsFixture() {
  return {
    kv: {
      commitsFailedTotal: 0,
      invalidTransactionRejectsTotal: 0,
      keysTotal: 6,
      operationsPerSecond: 7.5,
      transactionsActive: 8,
    },
    lease: {
      acquireTimeoutsTotal: 0,
      failureTotal: 0,
      forcedReleasesTotal: 0,
      invalidTokenRejectsTotal: 0,
      leasesActive: 9,
      oldestLeaseAgeSeconds: 5,
      operationsPerSecond: 10.5,
      requestsTotal: 5,
      successTotal: 6,
      waiterDepth: 7,
    },
    notice: {
      deliveryDropsTotal: 0,
      failureTotal: 0,
      publishesPerSecond: 11.5,
      requestsTotal: 2,
      successTotal: 4,
      subscriptionsActive: 12,
      unsubscribesTotal: 13,
      wildcardLimitRejectsTotal: 0,
    },
    queue: {
      inflightActive: 13,
      messagesDeadLettered: 0,
      messagesDelayed: 15,
      messagesPending: 16,
      messagesReady: 17,
      operationsPerSecond: 18.5,
    },
    rpc: {
      acksRejectedWrongWorkerTotal: 0,
      backpressureRejectsTotal: 0,
      duplicateCorrelationRejectsTotal: 0,
      failureTotal: 0,
      invalidSequenceErrorsDroppedTotal: 0,
      invalidSequenceErrorsForwardedTotal: 0,
      invalidSequenceResponsesTotal: 0,
      operationsPerSecond: 19.5,
      pendingRoutesActive: 20,
      requestTimeoutsTotal: 0,
      requestsPending: 21,
      requestsTotal: 22,
      responsesDroppedClosedCallerTotal: 0,
      responsesMissingPendingTotal: 0,
      successTotal: 23,
      wrongWorkerRejectsTotal: 0,
      workersRegistered: 24,
    },
    schedule: {
      ackFailuresTotal: 0,
      cancelPersistenceFailuresTotal: 0,
      createPersistenceFailuresTotal: 0,
      executionsPerMinute: 2.5,
      notifyFailuresTotal: 0,
      overdueNormalizationsTotal: 4,
      pendingFireClaims: 5,
      schedulesActive: 6,
      subscriptionsActive: 7,
      upsertPersistenceFailuresTotal: 0,
    },
    stream: {
      appendConflictsTotal: 0,
      failureTotal: 0,
      eventsTotal: 11,
      notifyDropsTotal: 0,
      operationsPerSecond: 12.5,
      requestsTotal: 18,
      successTotal: 19,
      streamsActive: 13,
      subscriptionsActive: 14,
    },
  };
}

beforeEach(() => {
  vi.clearAllMocks();

  mocks.apiv1.listNoticeRealms.mockResolvedValue({
    ok: true,
    status: 200,
    data: { realms: [{ realm: "default" }] },
  });
  mocks.apiv1.listNoticeAreas.mockResolvedValue({
    ok: true,
    status: 200,
    data: { areas: [{ area: "ops" }] },
  });
  mocks.apiv1.listNoticeResources.mockResolvedValue({
    ok: true,
    status: 200,
    data: { resources: [{ resource: "primary" }] },
  });
  mocks.apiv1.getNoticeResource.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      diagnostics: healthyDiagnostics,
      realm: "default",
      resource: "primary",
      subscriptions_active: 3,
    },
  });
  mocks.apiv1.listNoticeResourceEvents.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      area: "ops",
      derived: false,
      diagnostics: healthyDiagnostics,
      domain: "notice",
      events: [],
      limit: 20,
      realm: "default",
      resource: "primary",
    },
  });
  mocks.apiv1.listNoticeSubscriptions.mockResolvedValue({
    ok: true,
    status: 200,
    data: {
      subscriptions: [
        {
          created_at: "2026-05-21T13:05:00.000Z",
          notifications_received: 7,
          pattern: "notice://default/ops/primary",
          realm: "default",
          route_family: 1,
          session_id: "session-1",
          subscription_id: 11,
        },
      ],
    },
  });
});

describe("resource refactor registries", () => {
  it("registers every generic resource domain adapter and rejects missing adapters", () => {
    expect([...resourceDomainAdapterDomains].sort()).toEqual(
      [...genericResourceDomainSegments].sort(),
    );

    for (const domain of genericResourceDomainSegments) {
      expect(getResourceDomainAdapter(domain).domain).toBe(domain);
    }

    expect(() => getResourceDomainAdapter("queue" as never)).toThrow(
      "Missing resource domain adapter for queue",
    );
  });

  it("returns generic notice inventory and detail shapes through the resource adapter", async () => {
    await expect(resourceService.getResourceInventory("notice")).resolves.toEqual({
      domain: "notice",
      realms: [
        {
          areas: [
            {
              area: "ops",
              resourceEntries: [{ resource: "primary" }],
              resources: ["primary"],
            },
          ],
          realm: "default",
        },
      ],
    });

    await expect(
      resourceService.getResource(
        "notice",
        { area: "ops", realm: "default", resource: "primary" },
        null,
      ),
    ).resolves.toMatchObject({
      detailMetrics: expect.arrayContaining([
        expect.objectContaining({
          label: "Active subscriptions (live session fanout)",
          value: 3,
        }),
      ]),
      domain: "notice",
      ref: { area: "ops", realm: "default", resource: "primary" },
      related: expect.arrayContaining([expect.objectContaining({ title: "Notice subscriptions" })]),
      timeline: {
        area: "ops",
        derived: false,
        realm: "default",
        resource: "primary",
      },
    });

    expect(mocks.apiv1.listNoticeRealms).toHaveBeenCalledWith("1", {});
    expect(mocks.apiv1.listNoticeAreas).toHaveBeenCalledWith("1", "default", {});
    expect(mocks.apiv1.listNoticeResources).toHaveBeenCalledWith("1", "default", "ops", {});
    expect(mocks.apiv1.getNoticeResource).toHaveBeenCalledWith(
      "1",
      "default",
      "ops",
      "primary",
      {},
    );
    expect(mocks.apiv1.listNoticeResourceEvents).toHaveBeenCalledWith(
      "1",
      "default",
      "ops",
      "primary",
      { limit: 20 },
      {},
    );
    expect(mocks.apiv1.listNoticeSubscriptions).toHaveBeenCalledWith(
      "1",
      "default",
      "ops",
      "primary",
      {},
    );
  });

  it("keeps domain scope hrefs stable for every supported depth", () => {
    for (const domain of domainSegments) {
      expect(domainScopeHref(domain, {}, "7")).toBe(`/admin/7/${domain}`);
      expect(domainScopeHref(domain, { realm: "default" }, "7")).toBe(`/admin/7/${domain}/default`);
      expect(domainScopeHref(domain, { area: "ops", realm: "default" }, "7")).toBe(
        `/admin/7/${domain}/default/ops`,
      );
      expect(
        domainScopeHref(domain, { area: "ops", realm: "default", resource: "primary" }, "7"),
      ).toBe(`/admin/7/${domain}/default/ops/primary`);
    }

    expect(
      domainScopeHref(
        "notice",
        { area: "ops", operation: "changed", realm: "default", resource: "primary" },
        "7",
      ),
    ).toBe("/admin/7/notice/default/ops/primary/changed");
    expect(
      domainScopeHref(
        "rpc",
        { area: "ops", operation: "GetStatus", realm: "default", resource: "primary" },
        "7",
      ),
    ).toBe("/admin/7/rpc/default/ops/primary/GetStatus");
    expect(
      domainScopeHref(
        "queue",
        { area: "ops", operation: "ignored", realm: "default", resource: "primary" },
        "7",
      ),
    ).toBe("/admin/7/queue/default/ops/primary");
  });

  it("filters pure resource inventory rows without route state", () => {
    const rows = [
      { area: "ops", realm: "default", resource: "primary" },
      { area: "ops", realm: "default", resource: "secondary" },
      { area: "billing", realm: "acme", resource: "ledger" },
    ];

    expect(filterDomainResourceInventoryRows("kv", rows, "kv://default/ops/secondary")).toEqual([
      rows[1],
    ]);
    expect(filterDomainResourceInventoryRows("kv", rows, "acme")).toEqual([rows[2]]);
    expect(filterDomainResourceInventoryRows("kv", rows, "missing")).toEqual([]);
  });

  it("sorts optional inventory metrics while preserving default route order", () => {
    const rows = [
      { area: "ops", estimatedRecordCount: 10, realm: "default", resource: "primary" },
      { area: "ops", estimatedRecordCount: 30, realm: "default", resource: "secondary" },
      { area: "billing", realm: "acme", resource: "ledger" },
    ];
    const metricColumns: readonly DomainResourceMetricColumn[] = [
      {
        cell: (row) => row.estimatedRecordCount ?? "--",
        header: "Records",
        id: "records",
        sortValue: (row) => row.estimatedRecordCount,
      },
    ];

    expect(sortDomainResourceInventoryRows(rows, metricColumns, null)).toEqual(rows);
    expect(
      sortDomainResourceInventoryRows(rows, metricColumns, {
        columnId: "records",
        direction: "desc",
      }).map((row) => row.resource),
    ).toEqual(["secondary", "primary", "ledger"]);
    expect(
      sortDomainResourceInventoryRows(rows, metricColumns, {
        columnId: "records",
        direction: "asc",
      }).map((row) => row.resource),
    ).toEqual(["primary", "secondary", "ledger"]);
  });
});

describe("communication workspace copy", () => {
  it("uses operator-facing adapter-owned labels", () => {
    expect(communicationModeAdapters.notice.liveDataLabel).toBe("Live admin data");
    expect(communicationModeAdapters.notice.searchReadyLabel).toBe("Notice delivery evidence");
    expect(communicationModeAdapters.rpc.searchReadyLabel).toBe("RPC call evidence");
    expect(
      communicationModeAdapters.notice.modeOptions.map((option) => option.description),
    ).not.toEqual(expect.arrayContaining([expect.stringContaining("existing")]));
  });
});

describe("overview domain rules", () => {
  it("preserves issue severities, signals, and public issue sorting", () => {
    const baseDomains = overviewDomainsFixture();
    const domains = {
      ...baseDomains,
      kv: { ...baseDomains.kv, commitsFailedTotal: 1, invalidTransactionRejectsTotal: 1 },
      lease: { ...baseDomains.lease, failureTotal: 1, waiterDepth: 2 },
      notice: { ...baseDomains.notice, deliveryDropsTotal: 1, wildcardLimitRejectsTotal: 1 },
      queue: { ...baseDomains.queue, messagesDeadLettered: 2 },
      rpc: { ...baseDomains.rpc, failureTotal: 1, requestTimeoutsTotal: 1 },
      schedule: { ...baseDomains.schedule, ackFailuresTotal: 1, notifyFailuresTotal: 1 },
      stream: { ...baseDomains.stream, appendConflictsTotal: 1, notifyDropsTotal: 1 },
    };

    const descriptors = overviewDomainIssueDescriptors(domains);
    const status = buildOverviewStatus({
      system: {
        broker: {
          connections: 2,
          messagesPerSecond: 3.5,
          realms: ["default"],
          sessions: 4,
          uptimeSeconds: 5,
        },
        diagnostics: healthyGlobalDiagnostics,
        domains,
        fetchedAt: "2026-05-04T12:00:00Z",
        metrics: { lineCount: 0, lines: [], raw: "" },
      },
    });

    expect(descriptors).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          domain: "queue",
          id: "queue-dead-letters",
          severity: "high",
          title: "Queue dead letters",
        }),
        expect.objectContaining({
          domain: "kv",
          id: "kv-pressure",
          severity: "medium",
          title: "KV write pressure",
        }),
      ]),
    );
    expect(overviewDomainSignal("queue", domains)).toBe("17 ready / 13 inflight");
    expect(overviewDomainSignal("kv", domains)).toBe("6 keys / 8 transactions");
    expect(status.issues.map((issue) => `${issue.severity}:${issue.title}`)).toEqual([
      "high:Lease contention",
      "high:Queue dead letters",
      "high:RPC failures",
      "high:Schedule failures",
      "medium:KV write pressure",
      "medium:Notice delivery pressure",
      "medium:Stream pressure",
    ]);
  });
});

describe("queue resource presenters", () => {
  it("covers comparison parsing and dead-letter copy helpers", () => {
    const activeDetail = {
      area: "ops",
      completeSuccessTotal: 0,
      enqueueSuccessTotal: 0,
      inRatePerSecond: 0,
      messagesDeadLettered: 1,
      messagesDelayed: 0,
      messagesInflight: 0,
      messagesReady: 3,
      messagesTotal: 4,
      oldestBacklogAgeSeconds: 10,
      oldestMessageAgeSeconds: 65,
      outRatePerSecond: 0,
      realm: "default",
      resource: "primary",
      status: "backlogged" as const,
      subscriptionsActive: 0,
    };

    expect(parseFamilyInput("")).toEqual({ valid: true, value: null });
    expect(parseFamilyInput(" 42 ")).toEqual({ valid: true, value: 42 });
    expect(parseFamilyInput("abc")).toEqual({ valid: false, value: null });
    expect(parseFamilyInput("1.5")).toEqual({ valid: false, value: null });
    expect(
      formatQueueScope({ area: "ops", family: null, realm: "default", resource: "primary" }),
    ).toBe("default / ops / primary");
    expect(
      formatQueueScope({ area: "ops", family: 9, realm: "default", resource: "primary" }),
    ).toBe("default / ops / primary / family 9");
    expect(describeQueueState(activeDetail, null)).toMatchObject({
      label: "Attention",
      tone: "danger",
    });
    expect(deadLetterDialogCopy("replay", 42, "default / ops / primary")).toMatchObject({
      title: "Replay dead-letter message?",
      description: "Replay message 42 in default / ops / primary.",
    });
    expect(deadLetterDialogCopy("purge", 42, "default / ops / primary")).toMatchObject({
      title: "Purge dead-letter message?",
      description: "Purge message 42 from default / ops / primary. This is permanent.",
    });
  });
});
