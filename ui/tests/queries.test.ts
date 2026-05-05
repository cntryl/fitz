import { describe, expect, it } from "vite-plus/test";
import {
  createQueueDeadLettersQuery,
  createQueueOverviewQuery,
} from "@/features/queue/queue-query";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { createSignInMutation, createSignOutMutation } from "@/features/session/session-mutation";
import { mapQueueDeadLetter, mapQueueStats } from "@/features/queue/queue-mappers";
import { createKvOverviewQuery } from "@/features/kv/kv-query";
import { createLeaseOverviewQuery } from "@/features/lease/lease-query";
import { createNoticeOverviewQuery } from "@/features/notice/notice-query";
import { createRpcOverviewQuery } from "@/features/rpc/rpc-query";
import { createScheduleOverviewQuery } from "@/features/schedule/schedule-query";
import { createStreamOverviewQuery } from "@/features/stream/stream-query";
import { kvService } from "@/features/kv/kv-service";
import { leaseService } from "@/features/lease/lease-service";
import { noticeService } from "@/features/notice/notice-service";
import { rpcService } from "@/features/rpc/rpc-service";
import { scheduleService } from "@/features/schedule/schedule-service";
import { streamService } from "@/features/stream/stream-service";
import { queueService } from "@/features/queue/queue-service";
import { mapKvStats } from "@/features/kv/kv-mappers";
import { mapLeaseStats } from "@/features/lease/lease-mappers";
import { mapNoticeStats } from "@/features/notice/notice-mappers";
import { mapRpcStats } from "@/features/rpc/rpc-mappers";
import { mapScheduleStats } from "@/features/schedule/schedule-mappers";
import { mapStreamStats } from "@/features/stream/stream-mappers";

describe("Data query layer", () => {
  it("exports session query helpers", () => {
    expect(createCurrentSessionQuery).toBeDefined();
    expect(typeof createCurrentSessionQuery).toBe("function");
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
  });

  it("exports domain overview queries and service boundaries", () => {
    expect(createKvOverviewQuery).toBeDefined();
    expect(typeof createKvOverviewQuery).toBe("function");
    expect(createLeaseOverviewQuery).toBeDefined();
    expect(typeof createLeaseOverviewQuery).toBe("function");
    expect(createNoticeOverviewQuery).toBeDefined();
    expect(typeof createNoticeOverviewQuery).toBe("function");
    expect(createRpcOverviewQuery).toBeDefined();
    expect(typeof createRpcOverviewQuery).toBe("function");
    expect(createScheduleOverviewQuery).toBeDefined();
    expect(typeof createScheduleOverviewQuery).toBe("function");
    expect(createStreamOverviewQuery).toBeDefined();
    expect(typeof createStreamOverviewQuery).toBe("function");

    expect(kvService.getOverview).toBeDefined();
    expect(typeof kvService.getOverview).toBe("function");
    expect(leaseService.getOverview).toBeDefined();
    expect(typeof leaseService.getOverview).toBe("function");
    expect(noticeService.getOverview).toBeDefined();
    expect(typeof noticeService.getOverview).toBe("function");
    expect(rpcService.getOverview).toBeDefined();
    expect(typeof rpcService.getOverview).toBe("function");
    expect(scheduleService.getOverview).toBeDefined();
    expect(typeof scheduleService.getOverview).toBe("function");
    expect(streamService.getOverview).toBeDefined();
    expect(typeof streamService.getOverview).toBe("function");
    expect(queueService.getOverview).toBeDefined();
    expect(typeof queueService.getOverview).toBe("function");
  });

  it("maps queue DTOs to camelCase app models", () => {
    expect(
      mapQueueDeadLetter({
        realm: "r",
        area: "a",
        resource: "q",
        family: 4,
        message_id: 42,
        attempts: 3,
        reason: "exhausted retries",
        dead_lettered_at: "2026-05-04T12:00:00Z",
      }),
    ).toEqual({
      realm: "r",
      area: "a",
      resource: "q",
      family: 4,
      messageId: 42,
      attempts: 3,
      reason: "exhausted retries",
      deadLetteredAt: "2026-05-04T12:00:00Z",
    });
  });

  it("maps domain stats DTOs to app models", () => {
    expect(
      mapQueueStats({
        inflight_active: 8,
        messages_dead_lettered: 9,
        messages_delayed: 10,
        messages_pending: 11,
        messages_ready: 12,
        operations_per_second: 13.5,
      }),
    ).toEqual({
      inflightActive: 8,
      messagesDeadLettered: 9,
      messagesDelayed: 10,
      messagesPending: 11,
      messagesReady: 12,
      operationsPerSecond: 13.5,
    });

    expect(
      mapKvStats({
        keys_total: 4,
        operations_per_second: 1.5,
        transactions_active: 2,
      }),
    ).toEqual({
      keysTotal: 4,
      operationsPerSecond: 1.5,
      transactionsActive: 2,
    });

    expect(
      mapLeaseStats({
        leases_active: 3,
        operations_per_second: 2.5,
      }),
    ).toEqual({
      leasesActive: 3,
      operationsPerSecond: 2.5,
    });

    expect(
      mapNoticeStats({
        publishes_per_second: 1.25,
        subscriptions_active: 9,
      }),
    ).toEqual({
      publishesPerSecond: 1.25,
      subscriptionsActive: 9,
    });

    expect(
      mapRpcStats({
        operations_per_second: 5.5,
        requests_pending: 6,
        workers_registered: 7,
      }),
    ).toEqual({
      operationsPerSecond: 5.5,
      requestsPending: 6,
      workersRegistered: 7,
    });

    expect(
      mapScheduleStats({
        ack_failures_total: 1,
        executions_per_minute: 2.5,
        notify_failures_total: 3,
        overdue_normalizations_total: 4,
        pending_fire_claims: 5,
        schedules_active: 6,
        subscriptions_active: 7,
      }),
    ).toEqual({
      ackFailuresTotal: 1,
      executionsPerMinute: 2.5,
      notifyFailuresTotal: 3,
      overdueNormalizationsTotal: 4,
      pendingFireClaims: 5,
      schedulesActive: 6,
      subscriptionsActive: 7,
    });

    expect(
      mapStreamStats({
        events_total: 11,
        operations_per_second: 12.5,
        streams_active: 13,
        subscriptions_active: 14,
      }),
    ).toEqual({
      eventsTotal: 11,
      operationsPerSecond: 12.5,
      streamsActive: 13,
      subscriptionsActive: 14,
    });
  });
});
