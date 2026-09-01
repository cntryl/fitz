import { describe, expect, it } from "vite-plus/test";
import {
  createQueueDeadLettersQuery,
  createQueueOverviewQuery,
  createQueueInventoryQuery,
  createQueueAreaQuery,
  createQueueRealmQuery,
  QUEUE_OVERVIEW_KEY,
  QUEUE_INVENTORY_KEY,
  queueOverviewQueryKey,
  queueAreaQueryKey,
  queueDeadLettersQueryKey,
  queueDeadLettersQueryPrefix,
  queueRealmQueryKey,
} from "@/features/queue/queue-query";
import {
  createQueueResourceQuery,
  createQueueResourceTimelineQuery,
  createQueueResourceComparisonQuery,
  queueResourceQueryKey,
  queueResourceTimelineQueryKey,
} from "@/features/queue/queue-resource-query";
import {
  createActiveSessionsQuery,
  createCurrentSessionQuery,
  SESSION_QUERY_PREFIX,
} from "@/features/session/session-query";
import { createSignInMutation, createSignOutMutation } from "@/features/session/session-mutation";
import { createKvOverviewQuery } from "@/features/kv/kv-query";
import {
  createLeaseAreaQuery,
  createLeaseOverviewQuery,
  createLeaseRealmQuery,
  createLeaseResourceRowsQuery,
  leaseAreaQueryKey,
  leaseOverviewQueryKey,
  leaseRealmQueryKey,
  leaseResourceRowsQueryKey,
} from "@/features/lease/lease-query";
import {
  createNoticeAreaQuery,
  createNoticeOverviewQuery,
  createNoticeOperationRowsQuery,
  createNoticeRealmQuery,
  createNoticeResourceRowsQuery,
  noticeAreaQueryKey,
  noticeOperationRowsQueryKey,
  noticeRealmQueryKey,
  noticeResourceRowsQueryKey,
} from "@/features/notice/notice-query";
import {
  createRpcAreaQuery,
  createRpcOperationQuery,
  createRpcOverviewQuery,
  createRpcRealmQuery,
  createRpcResourceQuery,
  rpcAreaQueryKey,
  rpcOperationQueryKey,
  rpcRealmQueryKey,
  rpcResourceQueryKey,
} from "@/features/rpc/rpc-query";
import {
  createScheduleAreaQuery,
  createScheduleExecutionObservationsQuery,
  createScheduleMissedHandoffsQuery,
  createScheduleOperationQuery,
  createScheduleOverviewQuery,
  createScheduleRealmQuery,
  createScheduleResourceQuery,
  scheduleAreaQueryKey,
  scheduleExecutionObservationsQueryKey,
  scheduleMissedHandoffsQueryKey,
  scheduleOperationQueryKey,
  scheduleRealmQueryKey,
  scheduleResourceQueryKey,
} from "@/features/schedule/schedule-query";
import {
  createStreamAreaQuery,
  createStreamOverviewQuery,
  createStreamRealmQuery,
  createStreamResourceQuery,
  streamAreaQueryKey,
  streamRealmQueryKey,
  streamResourceQueryKey,
} from "@/features/stream/stream-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { resourceService } from "@/features/resource/resource-service";
import { createMetricsOverviewQuery } from "@/features/metrics/metrics-query";
import { metricsService } from "@/features/metrics/metrics-service";
import { createSystemOverviewQuery } from "@/features/system/system-query";
import { createMessagingTopologyQuery } from "@/features/topology/topology-query";
import { kvService } from "@/features/kv/kv-service";
import { leaseService } from "@/features/lease/lease-service";
import { noticeService } from "@/features/notice/notice-service";
import { rpcService } from "@/features/rpc/rpc-service";
import { scheduleService } from "@/features/schedule/schedule-service";
import { streamService } from "@/features/stream/stream-service";
import { queueService } from "@/features/queue/queue-service";
import { queueResourceService } from "@/features/queue/queue-resource-service";
import { systemService } from "@/features/system/system-service";
import { topologyService } from "@/features/topology/topology-service";
import {
  affectedQueueKeys,
  createPurgeQueueDeadLetterMutation,
  createReplayQueueDeadLetterMutation,
} from "@/features/queue/queue-actions";

describe("Data query layer", () => {
  it("exports session query helpers", () => {
    expect(createCurrentSessionQuery).toBeDefined();
    expect(typeof createCurrentSessionQuery).toBe("function");
    expect(createActiveSessionsQuery).toBeDefined();
    expect(typeof createActiveSessionsQuery).toBe("function");
    expect(createSignInMutation).toBeDefined();
    expect(typeof createSignInMutation).toBe("function");
    expect(createSignOutMutation).toBeDefined();
    expect(typeof createSignOutMutation).toBe("function");
  });
  it("exports queue dead-letter query helpers", () => {
    expect(createQueueDeadLettersQuery).toBeDefined();
    expect(typeof createQueueDeadLettersQuery).toBe("function");
    expect(createQueueOverviewQuery).toBeDefined();
    expect(typeof createQueueOverviewQuery).toBe("function");
    expect(createQueueInventoryQuery).toBeDefined();
    expect(typeof createQueueInventoryQuery).toBe("function");
    expect(createQueueRealmQuery).toBeDefined();
    expect(typeof createQueueRealmQuery).toBe("function");
    expect(createQueueAreaQuery).toBeDefined();
    expect(typeof createQueueAreaQuery).toBe("function");
    expect(QUEUE_INVENTORY_KEY).toEqual(expect.any(String));
    expect(QUEUE_OVERVIEW_KEY).toEqual(expect.any(String));
    expect(createQueueResourceQuery).toBeDefined();
    expect(typeof createQueueResourceQuery).toBe("function");
    expect(createQueueResourceTimelineQuery).toBeDefined();
    expect(typeof createQueueResourceTimelineQuery).toBe("function");
    expect(createQueueResourceComparisonQuery).toBeDefined();
    expect(typeof createQueueResourceComparisonQuery).toBe("function");
    expect(createReplayQueueDeadLetterMutation).toBeDefined();
    expect(typeof createReplayQueueDeadLetterMutation).toBe("function");
    expect(createPurgeQueueDeadLetterMutation).toBeDefined();
    expect(typeof createPurgeQueueDeadLetterMutation).toBe("function");
  });
  it("builds prefix-friendly queue mutation affected keys", () => {
    const ref = {
      area: "ops",
      realm: "default",
      resource: "primary",
    };

    expect(SESSION_QUERY_PREFIX.length).toBeGreaterThan(0);
    expect(queueResourceTimelineQueryKey(ref).startsWith(queueResourceQueryKey(ref))).toBe(true);
    expect(queueDeadLettersQueryKey(ref).startsWith(queueDeadLettersQueryPrefix(ref))).toBe(true);
    expect(
      queueDeadLettersQueryKey(ref, { family: 4 }).startsWith(queueDeadLettersQueryPrefix(ref)),
    ).toBe(true);
    expect(affectedQueueKeys(ref)).toEqual([
      queueOverviewQueryKey(),
      queueRealmQueryKey(ref.realm),
      queueAreaQueryKey(ref.realm, ref.area),
      queueResourceQueryKey(ref),
      queueResourceTimelineQueryKey(ref),
      queueDeadLettersQueryPrefix(ref),
    ]);
  });
  it("builds hierarchical lease query keys", () => {
    const ref = {
      area: "ops",
      realm: "default",
      resource: "primary",
    };

    expect(leaseOverviewQueryKey()).toEqual(expect.any(String));
    expect(leaseRealmQueryKey(ref.realm)).toEqual(expect.any(String));
    expect(leaseAreaQueryKey(ref.realm, ref.area)).toEqual(expect.any(String));
    expect(leaseResourceRowsQueryKey(ref.realm, ref.area, ref.resource)).toEqual(
      expect.any(String),
    );

    expect(leaseRealmQueryKey("default", "1")).not.toBe(leaseRealmQueryKey("default", "2"));
    expect(leaseRealmQueryKey(ref.realm)).not.toBe(leaseAreaQueryKey(ref.realm, ref.area));
    expect(leaseAreaQueryKey(ref.realm, ref.area)).not.toBe(
      leaseResourceRowsQueryKey(ref.realm, ref.area, ref.resource),
    );
    expect(leaseResourceRowsQueryKey(ref.realm, ref.area, ref.resource, 50)).toBe(
      leaseResourceRowsQueryKey(ref.realm, ref.area, ref.resource, 50),
    );
    expect(leaseResourceRowsQueryKey(ref.realm, ref.area, ref.resource, 50)).not.toBe(
      leaseResourceRowsQueryKey(ref.realm, ref.area, ref.resource, 51),
    );
  });
  it("builds hierarchical notice query keys", () => {
    const ref = {
      area: "ops",
      realm: "default",
      operation: "GetStatus",
      resource: "primary",
    };

    expect(noticeRealmQueryKey(ref.realm)).toEqual(expect.any(String));
    expect(noticeAreaQueryKey(ref.realm, ref.area)).toEqual(expect.any(String));
    expect(noticeResourceRowsQueryKey(ref.realm, ref.area, ref.resource)).toEqual(
      expect.any(String),
    );
    expect(noticeOperationRowsQueryKey(ref.realm, ref.area, ref.resource, ref.operation)).toEqual(
      expect.any(String),
    );
    expect(noticeRealmQueryKey(ref.realm, "1")).toBe(noticeRealmQueryKey(ref.realm, "1"));
    expect(noticeRealmQueryKey(ref.realm, "1")).not.toBe(noticeRealmQueryKey(ref.realm, "2"));
    expect(noticeRealmQueryKey(ref.realm)).not.toBe(noticeAreaQueryKey(ref.realm, ref.area));
    expect(noticeAreaQueryKey(ref.realm, ref.area)).not.toBe(
      noticeResourceRowsQueryKey(ref.realm, ref.area, ref.resource),
    );
    expect(
      noticeOperationRowsQueryKey(ref.realm, ref.area, ref.resource, ref.operation, 50),
    ).not.toBe(noticeOperationRowsQueryKey(ref.realm, ref.area, "secondary", ref.operation, 50));
    expect(
      noticeOperationRowsQueryKey(ref.realm, ref.area, ref.resource, ref.operation, 50),
    ).not.toBe(noticeOperationRowsQueryKey(ref.realm, ref.area, ref.resource, "List", 50));
    expect(
      noticeOperationRowsQueryKey(ref.realm, ref.area, ref.resource, ref.operation, 50, "1"),
    ).not.toBe(
      noticeOperationRowsQueryKey(ref.realm, ref.area, ref.resource, ref.operation, 50, "2"),
    );
    expect(noticeResourceRowsQueryKey(ref.realm, ref.area, ref.resource, 50, "1")).not.toBe(
      noticeResourceRowsQueryKey(ref.realm, ref.area, ref.resource, 51, "1"),
    );
  });
  it("exports domain overview queries and service boundaries", () => {
    expect(createKvOverviewQuery).toBeDefined();
    expect(typeof createKvOverviewQuery).toBe("function");
    expect(createLeaseOverviewQuery).toBeDefined();
    expect(typeof createLeaseOverviewQuery).toBe("function");
    expect(createLeaseRealmQuery).toBeDefined();
    expect(typeof createLeaseRealmQuery).toBe("function");
    expect(createLeaseAreaQuery).toBeDefined();
    expect(typeof createLeaseAreaQuery).toBe("function");
    expect(createLeaseResourceRowsQuery).toBeDefined();
    expect(typeof createLeaseResourceRowsQuery).toBe("function");
    expect(createNoticeOverviewQuery).toBeDefined();
    expect(typeof createNoticeOverviewQuery).toBe("function");
    expect(createNoticeRealmQuery).toBeDefined();
    expect(typeof createNoticeRealmQuery).toBe("function");
    expect(createNoticeAreaQuery).toBeDefined();
    expect(typeof createNoticeAreaQuery).toBe("function");
    expect(createNoticeResourceRowsQuery).toBeDefined();
    expect(typeof createNoticeResourceRowsQuery).toBe("function");
    expect(createNoticeOperationRowsQuery).toBeDefined();
    expect(typeof createNoticeOperationRowsQuery).toBe("function");
    expect(createRpcOverviewQuery).toBeDefined();
    expect(typeof createRpcOverviewQuery).toBe("function");
    expect(createRpcRealmQuery).toBeDefined();
    expect(typeof createRpcRealmQuery).toBe("function");
    expect(createRpcAreaQuery).toBeDefined();
    expect(typeof createRpcAreaQuery).toBe("function");
    expect(createRpcResourceQuery).toBeDefined();
    expect(typeof createRpcResourceQuery).toBe("function");
    expect(createRpcOperationQuery).toBeDefined();
    expect(typeof createRpcOperationQuery).toBe("function");
    expect(createScheduleOverviewQuery).toBeDefined();
    expect(typeof createScheduleOverviewQuery).toBe("function");
    expect(createScheduleRealmQuery).toBeDefined();
    expect(typeof createScheduleRealmQuery).toBe("function");
    expect(createScheduleAreaQuery).toBeDefined();
    expect(typeof createScheduleAreaQuery).toBe("function");
    expect(createScheduleResourceQuery).toBeDefined();
    expect(typeof createScheduleResourceQuery).toBe("function");
    expect(createScheduleOperationQuery).toBeDefined();
    expect(typeof createScheduleOperationQuery).toBe("function");
    expect(createScheduleExecutionObservationsQuery).toBeDefined();
    expect(typeof createScheduleExecutionObservationsQuery).toBe("function");
    expect(createScheduleMissedHandoffsQuery).toBeDefined();
    expect(typeof createScheduleMissedHandoffsQuery).toBe("function");
    expect(createStreamOverviewQuery).toBeDefined();
    expect(typeof createStreamOverviewQuery).toBe("function");
    expect(createStreamRealmQuery).toBeDefined();
    expect(typeof createStreamRealmQuery).toBe("function");
    expect(createStreamAreaQuery).toBeDefined();
    expect(typeof createStreamAreaQuery).toBe("function");
    expect(createStreamResourceQuery).toBeDefined();
    expect(typeof createStreamResourceQuery).toBe("function");
    expect(createSystemOverviewQuery).toBeDefined();
    expect(typeof createSystemOverviewQuery).toBe("function");
    expect(createMessagingTopologyQuery).toBeDefined();
    expect(typeof createMessagingTopologyQuery).toBe("function");
    expect(createResourceInventoryQuery).toBeDefined();
    expect(typeof createResourceInventoryQuery).toBe("function");
    expect(createMetricsOverviewQuery).toBeDefined();
    expect(typeof createMetricsOverviewQuery).toBe("function");

    expect(kvService.getOverview).toBeDefined();
    expect(typeof kvService.getOverview).toBe("function");
    expect(leaseService.getOverview).toBeDefined();
    expect(typeof leaseService.getOverview).toBe("function");
    expect(noticeService.getOverview).toBeDefined();
    expect(typeof noticeService.getOverview).toBe("function");
    expect(noticeService.listNoticeAreas).toBeDefined();
    expect(typeof noticeService.listNoticeAreas).toBe("function");
    expect(noticeService.listNoticeResources).toBeDefined();
    expect(typeof noticeService.listNoticeResources).toBe("function");
    expect(noticeService.searchResourceRows).toBeDefined();
    expect(typeof noticeService.searchResourceRows).toBe("function");
    expect(noticeService.searchOperationRows).toBeDefined();
    expect(typeof noticeService.searchOperationRows).toBe("function");
    expect(rpcService.getOverview).toBeDefined();
    expect(typeof rpcService.getOverview).toBe("function");
    expect(scheduleService.getOverview).toBeDefined();
    expect(typeof scheduleService.getOverview).toBe("function");
    expect(scheduleService.listScheduleAreas).toBeDefined();
    expect(typeof scheduleService.listScheduleAreas).toBe("function");
    expect(scheduleService.listScheduleResources).toBeDefined();
    expect(typeof scheduleService.listScheduleResources).toBe("function");
    expect(scheduleService.getScheduleResource).toBeDefined();
    expect(typeof scheduleService.getScheduleResource).toBe("function");
    expect(scheduleService.getScheduleOperation).toBeDefined();
    expect(typeof scheduleService.getScheduleOperation).toBe("function");
    expect(scheduleService.listExecutionObservations).toBeDefined();
    expect(typeof scheduleService.listExecutionObservations).toBe("function");
    expect(scheduleService.searchMissedHandoffs).toBeDefined();
    expect(typeof scheduleService.searchMissedHandoffs).toBe("function");
    expect(streamService.getOverview).toBeDefined();
    expect(typeof streamService.getOverview).toBe("function");
    expect(queueService.getOverview).toBeDefined();
    expect(typeof queueService.getOverview).toBe("function");
    expect(queueService.listInventory).toBeDefined();
    expect(typeof queueService.listInventory).toBe("function");
    expect(queueResourceService.getResource).toBeDefined();
    expect(typeof queueResourceService.getResource).toBe("function");
    expect(resourceService.getResourceInventory).toBeDefined();
    expect(typeof resourceService.getResourceInventory).toBe("function");
    expect(metricsService.getOverview).toBeDefined();
    expect(typeof metricsService.getOverview).toBe("function");
    expect(systemService.getOverview).toBeDefined();
    expect(typeof systemService.getOverview).toBe("function");
    expect(topologyService.getOverview).toBeDefined();
    expect(typeof topologyService.getOverview).toBe("function");
  });
  it("builds hierarchical RPC query keys", () => {
    const ref = {
      area: "ops",
      operation: "GetStatus",
      realm: "default",
      resource: "primary",
    };

    expect(rpcRealmQueryKey(ref.realm)).toEqual(expect.any(String));
    expect(rpcAreaQueryKey(ref.realm, ref.area)).toEqual(expect.any(String));
    expect(rpcResourceQueryKey(ref.realm, ref.area, ref.resource)).toEqual(expect.any(String));
    expect(rpcOperationQueryKey(ref.realm, ref.area, ref.resource, ref.operation)).toEqual(
      expect.any(String),
    );
    expect(rpcRealmQueryKey(ref.realm, "1")).not.toBe(rpcRealmQueryKey(ref.realm, "2"));
    expect(rpcRealmQueryKey(ref.realm)).not.toBe(rpcAreaQueryKey(ref.realm, ref.area));
    expect(rpcAreaQueryKey(ref.realm, ref.area)).not.toBe(
      rpcResourceQueryKey(ref.realm, ref.area, ref.resource),
    );
    expect(rpcResourceQueryKey(ref.realm, ref.area, ref.resource)).not.toBe(
      rpcOperationQueryKey(ref.realm, ref.area, ref.resource, ref.operation),
    );
    expect(rpcOperationQueryKey(ref.realm, ref.area, ref.resource, ref.operation, 50)).not.toBe(
      rpcOperationQueryKey(ref.realm, ref.area, ref.resource, ref.operation, 51),
    );
  });
  it("builds hierarchical Stream query keys", () => {
    const ref = {
      area: "ops",
      discriminator: "invoice",
      realm: "default",
      resource: "events",
    };

    expect(streamRealmQueryKey(ref.realm)).toEqual(expect.any(String));
    expect(streamAreaQueryKey(ref.realm, ref.area)).toEqual(expect.any(String));
    expect(streamResourceQueryKey({ ...ref, fromOffset: 0, limit: 50 })).toEqual(
      expect.any(String),
    );
    expect(streamRealmQueryKey(ref.realm, "1")).not.toBe(streamRealmQueryKey(ref.realm, "2"));
    expect(streamRealmQueryKey(ref.realm)).not.toBe(streamAreaQueryKey(ref.realm, ref.area));
    expect(streamAreaQueryKey(ref.realm, ref.area)).not.toBe(
      streamResourceQueryKey({ ...ref, fromOffset: 0, limit: 50 }),
    );
    expect(streamResourceQueryKey({ ...ref, fromOffset: 0, limit: 50 })).not.toBe(
      streamResourceQueryKey({ ...ref, fromOffset: 1, limit: 50 }),
    );
    expect(
      streamResourceQueryKey({ ...ref, discriminator: "a", fromOffset: 0, limit: 50 }),
    ).not.toBe(streamResourceQueryKey({ ...ref, discriminator: "b", fromOffset: 0, limit: 50 }));
    expect(streamResourceQueryKey({ ...ref, fromOffset: 0, limit: 50 })).not.toBe(
      streamResourceQueryKey({ ...ref, fromOffset: 0, limit: 51 }),
    );
  });
  it("builds hierarchical Schedule query keys", () => {
    const ref = {
      area: "ops",
      realm: "default",
      resource: "reconcile",
    };

    expect(scheduleRealmQueryKey(ref.realm)).toEqual(expect.any(String));
    expect(scheduleAreaQueryKey(ref.realm, ref.area)).toEqual(expect.any(String));
    expect(scheduleResourceQueryKey({ ...ref, limit: 20 })).toEqual(expect.any(String));
    expect(scheduleOperationQueryKey({ ...ref, limit: 20, operation: "handoff" })).toEqual(
      expect.any(String),
    );
    expect(scheduleExecutionObservationsQueryKey({ ...ref, limit: 20 })).toEqual(
      expect.any(String),
    );
    expect(scheduleMissedHandoffsQueryKey({ ...ref, limit: 20 })).toEqual(expect.any(String));
    expect(scheduleRealmQueryKey(ref.realm, "1")).not.toBe(scheduleRealmQueryKey(ref.realm, "2"));
    expect(scheduleRealmQueryKey(ref.realm)).not.toBe(scheduleAreaQueryKey(ref.realm, ref.area));
    expect(scheduleAreaQueryKey(ref.realm, ref.area)).not.toBe(
      scheduleResourceQueryKey({ ...ref, limit: 20 }),
    );
    expect(scheduleResourceQueryKey({ ...ref, limit: 20 })).not.toBe(
      scheduleResourceQueryKey({ ...ref, limit: 21 }),
    );
    expect(scheduleResourceQueryKey({ ...ref, limit: 20 })).not.toBe(
      scheduleResourceQueryKey({ ...ref, limit: 20, offset: 20 }),
    );
    expect(scheduleResourceQueryKey({ ...ref, limit: 20 })).not.toBe(
      scheduleOperationQueryKey({ ...ref, limit: 20, operation: "handoff" }),
    );
    expect(scheduleExecutionObservationsQueryKey({ ...ref, limit: 20 })).not.toBe(
      scheduleMissedHandoffsQueryKey({ ...ref, limit: 20 }),
    );
    expect(scheduleMissedHandoffsQueryKey({ ...ref, limit: 20 })).not.toBe(
      scheduleMissedHandoffsQueryKey({ ...ref, limit: 21 }),
    );
  });
});
