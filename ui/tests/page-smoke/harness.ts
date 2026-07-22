import { afterEach, beforeEach, vi } from "vite-plus/test";
import { cleanupApp, createSPA } from "@askrjs/askr/boot";
import type { Query } from "@askrjs/askr/data";
import type { RouteHandler } from "@askrjs/askr/router";
import { queryState } from "@askrjs/askr/testing";

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
  createQueueDeadLettersQuery: () => mocks.queryStates.queueDeadLetters,
  createQueueAreaQuery: () => mocks.queryStates.queueArea,
  createQueueOverviewQuery: () => mocks.queryStates.queue,
  createQueueRealmQuery: () => mocks.queryStates.queueRealm,
  createQueueInventoryQuery: () => mocks.queryStates.queueInventory,
}));

vi.mock("@/features/queue/queue-resource-query", () => ({
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

vi.mock("@/features/kv/kv-value-query", () => ({
  createKvValueQuery: () => mocks.queryStates.kvValue,
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
import {
  activeSessions,
  inventory,
  kvOverview,
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
  resourceDetail,
  rpcArea,
  rpcOperation,
  rpcOverview,
  rpcRealm,
  rpcResource,
  scheduleArea,
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
  mocks.queryStates.currentSession = queryState.fresh(
    {
      authenticated: true,
      routeFamilies: ["1"],
      routeFamiliesWildcard: false,
      username: "admin",
    },
    queryOptions(),
  );
  mocks.queryStates.activeSessions = queryState.fresh(activeSessions, queryOptions());
  mocks.queryStates.system = queryState.fresh(systemOverview, queryOptions());
  mocks.queryStates.topology = queryState.fresh(topologyOverview, queryOptions());
  mocks.queryStates.metrics = queryState.fresh(metricsOverview, queryOptions());
  mocks.queryStates.search = queryState.fresh(
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
  mocks.queryStates.queue = queryState.fresh(queueOverview, queryOptions());
  mocks.queryStates.queueArea = queryState.fresh(queueAreaDetail, queryOptions());
  mocks.queryStates.queueDeadLetters = queryState.fresh([], queryOptions());
  mocks.queryStates.queueInventory = queryState.fresh(queueInventory, queryOptions());
  mocks.queryStates.queueRealm = queryState.fresh(queueRealmDetail, queryOptions());
  mocks.queryStates.queueResource = queryState.fresh(queueResource, queryOptions());
  mocks.queryStates.queueTimeline = queryState.fresh(queueResource.timeline, queryOptions());
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
  mocks.queryStates.kvValue = queryState.fresh(
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

export async function mountRoute(path: string, routePath: string, handler: RouteHandler) {
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
