import { afterEach, beforeEach, vi } from "vite-plus/test";
import { createRouteRegistry, route } from "@askrjs/askr/router";
import type { DataRuntime, Query } from "@askrjs/askr/data";
import type { RouteHandler } from "@askrjs/askr/router";
import { mockQuery, renderRoute, type RenderResult } from "@askrjs/askr/testing";

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

let activeRender: RenderResult | null = null;

export function pageSmokeMocks() {
  return mocks;
}

export function queryOptions() {
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

vi.mock("@/features/search/search-query", () => ({
  createAdminSearchQuery: () => mocks.queryStates.search,
}));

vi.mock("@/features/queue/queue-query", () => ({
  createQueueDeadLettersQuery: vi.fn(() => mocks.queryStates.queueDeadLetters),
  createQueueAreaQuery: () => mocks.queryStates.queueArea,
  createQueueOverviewQuery: () => mocks.queryStates.queue,
  createQueueRealmQuery: () => mocks.queryStates.queueRealm,
  createQueueInventoryQuery: () => mocks.queryStates.queueInventory,
}));

vi.mock("@/features/queue/queue-resource-query", () => ({
  createQueueResourceQuery: () => mocks.queryStates.queueResource,
  createQueueResourceInflightQuery: vi.fn(() => mocks.queryStates.queueInflight),
  createQueueResourceTimelineQuery: () => mocks.queryStates.queueTimeline,
}));

vi.mock("@/features/queue/queue-actions", () => ({
  createPurgeQueueDeadLetterMutation: () => mocks.mutation,
  createReplayQueueDeadLetterMutation: () => mocks.mutation,
}));

vi.mock("@/features/kv/kv-query", () => ({
  createKvOverviewQuery: () => mocks.queryStates.kv,
  createKvResourceDetailQuery: () => mocks.queryStates.kvResourceDetail,
  createKvTransactionsQuery: vi.fn(() => mocks.queryStates.kvTransactions),
}));

vi.mock("@/features/kv/kv-rows-query", () => ({
  createKvRowsQuery: vi.fn(() => mocks.queryStates.kvRows),
}));

vi.mock("@/features/kv/kv-value-query", () => ({
  createKvValueQuery: vi.fn(() => mocks.queryStates.kvValue),
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
  createScheduleOperationQuery: () => mocks.queryStates.scheduleOperation,
  createScheduleRealmQuery: () => mocks.queryStates.scheduleRealm,
  createScheduleResourceQuery: () => mocks.queryStates.scheduleResource,
}));

vi.mock("@/features/stream/stream-query", () => ({
  createStreamAreaQuery: () => mocks.queryStates.streamArea,
  createStreamOverviewQuery: () => mocks.queryStates.stream,
  createStreamRealmQuery: () => mocks.queryStates.streamRealm,
  createStreamRecordsQuery: vi.fn(() => mocks.queryStates.streamRecords),
  createStreamResourceQuery: () => mocks.queryStates.streamResource,
}));

vi.mock("@/features/resource/resource-query", () => ({
  createResourceInventoryQuery: () => mocks.queryStates.inventory,
}));
import {
  activeSessions,
  inventory,
  kvOverview,
  kvResourceDetail,
  kvRows,
  leaseArea,
  leaseOverview,
  leaseRealm,
  leaseResourceRowsFixture,
  metricsOverview,
  noticeAreaInventory,
  noticeOperationRows,
  noticeOverview,
  noticeRealmInventory,
  noticeResourceRows,
  queueAreaDetail,
  queueInventory,
  queueOverview,
  queueRealmDetail,
  queueResource,
  rpcArea,
  rpcOperation,
  rpcOverview,
  rpcRealm,
  rpcResource,
  scheduleArea,
  scheduleOperation,
  scheduleOverview,
  scheduleRealm,
  scheduleResource,
  streamArea,
  streamOverview,
  streamRealm,
  streamResource,
  systemOverview,
  topologyOverview,
} from "./fixtures";

export function resetQueries() {
  mocks.queryStates.currentSession = mockQuery(
    {
      authenticated: true,
      routeFamilies: ["1"],
      routeFamiliesWildcard: false,
      username: "root",
    },
    queryOptions(),
  );
  mocks.queryStates.activeSessions = mockQuery(activeSessions, queryOptions());
  mocks.queryStates.system = mockQuery(systemOverview, queryOptions());
  mocks.queryStates.topology = mockQuery(topologyOverview, queryOptions());
  mocks.queryStates.metrics = mockQuery(metricsOverview, queryOptions());
  mocks.queryStates.search = mockQuery(
    {
      limit: 100,
      query: "orders",
      results: [
        {
          domain: "sessions",
          health: "live",
          href: "/sessions",
          id: "session:1:session-1",
          kind: "session",
          matchedFields: ["session_id"],
          metadata: {},
          routeFamily: "1",
          summary: "Active broker session",
          title: "session-1",
        },
        {
          domain: "kv",
          href: "/kv?realm=acme",
          id: "kv:1:acme",
          kind: "realm",
          matchedFields: ["realm"],
          metadata: {},
          realm: "acme",
          routeFamily: "1",
          summary: "KV realm",
          title: "acme",
        },
      ],
      routeFamily: "1",
      total: 2,
      truncated: false,
    },
    queryOptions(),
  );
  mocks.queryStates.queue = mockQuery(queueOverview, queryOptions());
  mocks.queryStates.queueArea = mockQuery(queueAreaDetail, queryOptions());
  mocks.queryStates.queueDeadLetters = mockQuery([], queryOptions());
  mocks.queryStates.queueInventory = mockQuery(queueInventory, queryOptions());
  mocks.queryStates.queueRealm = mockQuery(queueRealmDetail, queryOptions());
  mocks.queryStates.queueResource = mockQuery(queueResource.detail, queryOptions());
  mocks.queryStates.queueInflight = mockQuery(queueResource.inflight, queryOptions());
  mocks.queryStates.queueTimeline = mockQuery(queueResource.timeline, queryOptions());
  mocks.queryStates.kv = mockQuery(kvOverview, queryOptions());
  mocks.queryStates.kvResourceDetail = mockQuery(kvResourceDetail, queryOptions());
  mocks.queryStates.kvTransactions = mockQuery([], queryOptions());
  mocks.queryStates.lease = mockQuery(leaseOverview, queryOptions());
  mocks.queryStates.leaseRealm = mockQuery(leaseRealm, queryOptions());
  mocks.queryStates.leaseArea = mockQuery(leaseArea, queryOptions());
  mocks.queryStates.leaseResourceRows = mockQuery(leaseResourceRowsFixture(), queryOptions());
  mocks.queryStates.notice = mockQuery(noticeOverview, queryOptions());
  mocks.queryStates.noticeRealm = mockQuery(noticeRealmInventory, queryOptions());
  mocks.queryStates.noticeArea = mockQuery(noticeAreaInventory, queryOptions());
  mocks.queryStates.noticeResourceRows = mockQuery(noticeResourceRows, queryOptions());
  mocks.queryStates.noticeOperationRows = mockQuery(noticeOperationRows, queryOptions());
  mocks.queryStates.rpc = mockQuery(rpcOverview, queryOptions());
  mocks.queryStates.rpcRealm = mockQuery(rpcRealm, queryOptions());
  mocks.queryStates.rpcArea = mockQuery(rpcArea, queryOptions());
  mocks.queryStates.rpcResource = mockQuery(rpcResource, queryOptions());
  mocks.queryStates.rpcOperation = mockQuery(rpcOperation, queryOptions());
  mocks.queryStates.schedule = mockQuery(scheduleOverview, queryOptions());
  mocks.queryStates.scheduleRealm = mockQuery(scheduleRealm, queryOptions());
  mocks.queryStates.scheduleArea = mockQuery(scheduleArea, queryOptions());
  mocks.queryStates.scheduleOperation = mockQuery(scheduleOperation, queryOptions());
  mocks.queryStates.scheduleResource = mockQuery(scheduleResource, queryOptions());
  mocks.queryStates.scheduleExecutionObservations = mockQuery(
    scheduleResource.executionObservations,
    queryOptions(),
  );
  mocks.queryStates.scheduleMissedHandoffs = mockQuery(
    scheduleOperation.missedHandoffs,
    queryOptions(),
  );
  mocks.queryStates.stream = mockQuery(streamOverview, queryOptions());
  mocks.queryStates.streamRealm = mockQuery(streamRealm, queryOptions());
  mocks.queryStates.streamArea = mockQuery(streamArea, queryOptions());
  mocks.queryStates.streamResource = mockQuery(streamResource.detail, queryOptions());
  mocks.queryStates.streamRecords = mockQuery(streamResource.records, queryOptions());
  mocks.queryStates.inventory = mockQuery(inventory, queryOptions());
  mocks.queryStates.kvRows = mockQuery(kvRows, queryOptions());
  mocks.queryStates.kvValue = mockQuery(
    {
      area: "ops",
      found: true,
      key: kvRows.items[0]?.key,
      realm: "default",
      resource: "primary",
      routeFamily: 1,
      value: kvRows.items[0]?.value,
    },
    queryOptions(),
  );
  mocks.mutation.error = null;
  mocks.mutation.pending = false;
  mocks.mutation.result = null;
}

export async function mountRoute(
  path: string,
  routePath: string,
  handler: RouteHandler,
  dataRuntime?: DataRuntime,
) {
  activeRender?.cleanup();
  activeRender = null;
  document.body.innerHTML = '<div id="app"></div>';

  const root = document.getElementById("app");
  if (!root) {
    throw new Error("Missing test app root");
  }

  activeRender = await renderRoute({
    container: root,
    dataRuntime,
    registry: createRouteRegistry(() => {
      route(routePath, handler);
    }),
    url: path,
  });

  await new Promise<void>((resolve) => queueMicrotask(() => resolve()));
  return activeRender.root;
}

afterEach(() => {
  activeRender?.cleanup();
  activeRender = null;
  document.body.innerHTML = "";
  vi.clearAllMocks();
});

beforeEach(() => {
  resetQueries();
});
