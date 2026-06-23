import { describe, expect, it } from "vite-plus/test";
import {
  createQueueDeadLettersQuery,
  createQueueOverviewQuery,
  createQueueInventoryQuery,
  QUEUE_OVERVIEW_KEY,
  QUEUE_INVENTORY_KEY,
  queueDeadLettersQueryKey,
  queueDeadLettersQueryPrefix,
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
import { mapQueueDeadLetter, mapQueueStats } from "@/features/queue/queue-mappers";
import {
  mapQueueInflight,
  mapQueueResourceComparison,
  mapQueueResourceDetail,
  mapQueueResourceTimeline,
} from "@/features/queue/queue-resource-mappers";
import { createKvOverviewQuery } from "@/features/kv/kv-query";
import { createLeaseOverviewQuery } from "@/features/lease/lease-query";
import { createNoticeOverviewQuery } from "@/features/notice/notice-query";
import { createRpcOverviewQuery } from "@/features/rpc/rpc-query";
import { createScheduleOverviewQuery } from "@/features/schedule/schedule-query";
import { createStreamOverviewQuery } from "@/features/stream/stream-query";
import {
  createResourceInventoryQuery,
  createResourceQuery,
} from "@/features/resource/resource-query";
import { resourceService } from "@/features/resource/resource-service";
import { createMetricsOverviewQuery } from "@/features/metrics/metrics-query";
import { metricsService } from "@/features/metrics/metrics-service";
import { parsePrometheusMetrics } from "@/features/metrics/metrics-mappers";
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
import { mapKvStats } from "@/features/kv/kv-mappers";
import { mapLeaseStats } from "@/features/lease/lease-mappers";
import { mapNoticeStats } from "@/features/notice/notice-mappers";
import { mapRpcStats } from "@/features/rpc/rpc-mappers";
import { mapScheduleStats } from "@/features/schedule/schedule-mappers";
import { mapStreamStats } from "@/features/stream/stream-mappers";
import {
  affectedQueueKeys,
  createPurgeQueueDeadLetterMutation,
  createReplayQueueDeadLetterMutation,
} from "@/features/queue/queue-actions";
import { mapActiveSession, mapActiveSessionsOverview } from "@/features/session/session-mappers";
import { mapSystemOverview } from "@/features/system/system-mappers";
import {
  appendTopologyTrendPoint,
  defaultTopologySelectionId,
  laneTrendDirection,
  mapMessagingTopology,
  resolveTopologySelection,
  topologyScopeHref,
  topologyTrendDirection,
} from "@/features/topology/topology-mappers";
import { healthyDiagnostics, healthyGlobalDiagnostics, topologyDtoLane } from "./fixtures/topology";

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
      QUEUE_OVERVIEW_KEY,
      queueResourceQueryKey(ref),
      queueResourceTimelineQueryKey(ref),
      queueDeadLettersQueryPrefix(ref),
    ]);
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
    expect(createSystemOverviewQuery).toBeDefined();
    expect(typeof createSystemOverviewQuery).toBe("function");
    expect(createMessagingTopologyQuery).toBeDefined();
    expect(typeof createMessagingTopologyQuery).toBe("function");
    expect(createResourceInventoryQuery).toBeDefined();
    expect(typeof createResourceInventoryQuery).toBe("function");
    expect(createResourceQuery).toBeDefined();
    expect(typeof createResourceQuery).toBe("function");
    expect(createMetricsOverviewQuery).toBeDefined();
    expect(typeof createMetricsOverviewQuery).toBe("function");

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
    expect(queueService.listInventory).toBeDefined();
    expect(typeof queueService.listInventory).toBe("function");
    expect(queueResourceService.getResource).toBeDefined();
    expect(typeof queueResourceService.getResource).toBe("function");
    expect(resourceService.getResourceInventory).toBeDefined();
    expect(typeof resourceService.getResourceInventory).toBe("function");
    expect(resourceService.getResource).toBeDefined();
    expect(typeof resourceService.getResource).toBe("function");
    expect(metricsService.getOverview).toBeDefined();
    expect(typeof metricsService.getOverview).toBe("function");
    expect(systemService.getOverview).toBeDefined();
    expect(typeof systemService.getOverview).toBe("function");
    expect(topologyService.getOverview).toBeDefined();
    expect(typeof topologyService.getOverview).toBe("function");
  });

  it("parses Prometheus metrics into searchable families", () => {
    expect(
      parsePrometheusMetrics(`# HELP fitz_rpc_requests_total RPC requests
# TYPE fitz_rpc_requests_total counter
fitz_rpc_requests_total{realm="prod",area="api"} 42
fitz_queue_ready{realm="prod",area="jobs",resource="emails"} 7
`),
    ).toEqual({
      raw: `# HELP fitz_rpc_requests_total RPC requests
# TYPE fitz_rpc_requests_total counter
fitz_rpc_requests_total{realm="prod",area="api"} 42
fitz_queue_ready{realm="prod",area="jobs",resource="emails"} 7
`,
      families: [
        {
          name: "fitz_queue_ready",
          samples: [
            {
              labels: {
                area: "jobs",
                realm: "prod",
                resource: "emails",
              },
              name: "fitz_queue_ready",
              value: 7,
            },
          ],
        },
        {
          help: "RPC requests",
          name: "fitz_rpc_requests_total",
          samples: [
            {
              labels: {
                area: "api",
                realm: "prod",
              },
              name: "fitz_rpc_requests_total",
              value: 42,
            },
          ],
          type: "counter",
        },
      ],
    });
  });

  it("maps topology DTOs and derives selection links and trends", () => {
    const topology = mapMessagingTopology({
      broker: {
        connections: 3,
        messages_per_second: 2,
        realms: ["prod"],
        router_backpressure_total: 0,
        router_high_lane_backpressure_total: 0,
        sessions: 2,
        uptime_seconds: 60,
      },
      connections: {
        items: [
          {
            id: "queue-inflight:4:99",
            kind: "queue_inflight_consumer",
            label: "jobs / worker inflight",
            metrics: [{ key: "attempts", label: "Attempts", value: 2 }],
            scope: {
              area: "jobs",
              realm: "prod",
              resource: "worker",
              route_family: 4,
              session_id: "11",
            },
            source: "domain:queue",
            state: "flowing",
            target: "session:11",
          },
        ],
        limit: 250,
        total: 1,
        truncated: false,
      },
      diagnostics: {
        ...healthyGlobalDiagnostics,
        top_bottleneck: {
          ...healthyDiagnostics,
          area: "jobs",
          current_stage: "dead_letter_pressure",
          domain: "queue",
          realm: "prod",
          resource: "worker",
          severity: "high",
        },
      },
      generated_at: "2026-05-21T13:10:00Z",
      lanes: [
        topologyDtoLane("queue", "blocked", 4),
        topologyDtoLane("rpc", "flowing", 1),
        topologyDtoLane("notice", "quiet", 0),
        topologyDtoLane("schedule", "pressure", 2),
        topologyDtoLane("stream", "flowing", 3),
        topologyDtoLane("lease", "quiet", 0),
        topologyDtoLane("kv", "flowing", 1),
      ],
      session_groups: [
        {
          max_idle_seconds: 12,
          messages_received: 5,
          messages_sent: 7,
          representative_sessions: [
            {
              messages_received: 5,
              messages_sent: 7,
              route_family: 4,
              session_id: "11",
              transport: "websocket",
            },
          ],
          route_family: 4,
          sessions: 1,
          transports: ["websocket"],
        },
      ],
    });

    expect(topology.lanes.map((entry) => entry.id)).toEqual([
      "queue",
      "rpc",
      "notice",
      "schedule",
      "stream",
      "lease",
      "kv",
    ]);
    expect(topology.sessionGroups[0].representativeSessions[0].routeFamily).toBe(4);
    expect(topology.connections.items[0].scope.routeFamily).toBe(4);
    expect(topologyScopeHref("queue", topology.connections.items[0].scope)).toBe(
      "/queue/prod/jobs/worker",
    );
    expect(defaultTopologySelectionId(topology)).toBe("lane:queue");
    expect(resolveTopologySelection(topology, "lane:queue").title).toBe("QUEUE");

    const history = appendTopologyTrendPoint([], topology);
    const nextTopology = {
      ...topology,
      broker: {
        ...topology.broker,
        messagesPerSecond: 5,
      },
      generatedAt: "2026-05-21T13:10:10Z",
      lanes: topology.lanes.map((entry) =>
        entry.id === "queue"
          ? {
              ...entry,
              counters: [{ key: "pressure", label: "Pressure", value: 8 }],
            }
          : entry,
      ),
    };
    const nextHistory = appendTopologyTrendPoint(history, nextTopology);

    expect(topologyTrendDirection(nextHistory, "messagesPerSecond")).toBe("rising");
    expect(laneTrendDirection(nextHistory, "queue")).toBe("rising");
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
        completes_total: 5,
        diagnostics: healthyDiagnostics,
        enqueues_total: 6,
        extends_total: 7,
        failure_total: 8,
        inflight_active: 8,
        messages_dead_lettered: 9,
        messages_delayed: 10,
        messages_pending: 11,
        messages_ready: 12,
        oldest_backlog_age_seconds: 14,
        oldest_message_age_seconds: 13,
        backlog_age_buckets: {
          under_1m: 1,
          under_5m: 2,
          under_15m: 3,
          over_15m: 4,
        },
        delay_age_buckets: {
          under_1m: 0,
          under_5m: 1,
          under_15m: 2,
          over_15m: 3,
        },
        operations_per_second: 13.5,
        notify_drops_total: 14,
        dead_letter_transitions_total: 16,
        complete_rejected_total: 17,
        redeliveries_total: 15,
        releases_total: 10,
        requests_total: 11,
        reserves_total: 12,
        success_total: 13,
      }),
    ).toEqual({
      inflightActive: 8,
      messagesDeadLettered: 9,
      messagesDelayed: 10,
      messagesPending: 11,
      messagesReady: 12,
      oldestBacklogAgeSeconds: 14,
      operationsPerSecond: 13.5,
    });

    expect(
      mapQueueResourceDetail({
        area: "a",
        diagnostics: healthyDiagnostics,
        messages_dead_lettered: 2,
        messages_delayed: 3,
        messages_inflight: 4,
        messages_ready: 5,
        messages_total: 6,
        oldest_message_age_seconds: 7,
        oldest_backlog_age_seconds: 8,
        backlog_age_buckets: {
          under_1m: 1,
          under_5m: 1,
          under_15m: 1,
          over_15m: 0,
        },
        delay_age_buckets: {
          under_1m: 0,
          under_5m: 1,
          under_15m: 0,
          over_15m: 0,
        },
        realm: "r",
        resource: "q",
      }),
    ).toEqual({
      area: "a",
      realm: "r",
      resource: "q",
      messagesReady: 5,
      messagesInflight: 4,
      messagesDelayed: 3,
      messagesDeadLettered: 2,
      messagesTotal: 6,
      oldestMessageAgeSeconds: 7,
    });

    expect(
      mapQueueResourceTimeline({
        area: "a",
        derived: true,
        diagnostics: healthyDiagnostics,
        domain: "queue",
        events: [
          {
            age_seconds: 12,
            area: "a",
            attempts: 2,
            correlation_id: "corr-1",
            domain: "queue",
            family: 4,
            kind: "retry",
            message_id: 77,
            observed_at: "2026-05-04T12:00:00Z",
            operation: "enqueue",
            owner_session: "owner-1",
            realm: "r",
            resource: "q",
            summary: "Message retried after delay",
            worker_session: "worker-1",
          },
        ],
        family: 4,
        limit: 8,
        realm: "r",
        resource: "q",
      }),
    ).toEqual({
      area: "a",
      derived: true,
      events: [
        {
          ageSeconds: 12,
          area: "a",
          attempts: 2,
          correlationId: "corr-1",
          kind: "retry",
          messageId: 77,
          observedAt: "2026-05-04T12:00:00Z",
          operation: "enqueue",
          ownerSession: "owner-1",
          realm: "r",
          resource: "q",
          summary: "Message retried after delay",
          workerSession: "worker-1",
        },
      ],
      limit: 8,
      realm: "r",
      resource: "q",
    });

    expect(
      mapQueueResourceComparison({
        comparison_mode: "resource",
        derived: true,
        delta: {
          backlog: 4,
          dead_letters: 1,
          delayed: -2,
          inflight: 3,
          ready: -5,
          recent_transition_count: 6,
          waiters: 7,
        },
        domain: "queue",
        left: {
          diagnostics: healthyDiagnostics,
          metrics: {
            backlog: 10,
            dead_letters: 2,
            delayed: 3,
            inflight: 4,
            ready: 5,
            recent_transition_count: 6,
            waiters: 7,
          },
          scope: {
            area: "a",
            family: 2,
            realm: "r",
            resource: "q",
          },
        },
        right: {
          diagnostics: healthyDiagnostics,
          metrics: {
            backlog: 6,
            dead_letters: 1,
            delayed: 5,
            inflight: 1,
            ready: 10,
            recent_transition_count: 0,
            waiters: 0,
          },
          scope: {
            area: "b",
            family: null,
            realm: "r2",
            resource: "q2",
          },
        },
        summary: "Queue pressure is higher on the left snapshot",
      }),
    ).toEqual({
      comparisonMode: "resource",
      derived: true,
      delta: {
        ageSeconds: null,
        backlog: 4,
        deadLetters: 1,
        delayed: -2,
        inflight: 3,
        ready: -5,
        recentTransitionCount: 6,
        waiters: 7,
      },
      left: {
        metrics: {
          ageSeconds: null,
          backlog: 10,
          deadLetters: 2,
          delayed: 3,
          inflight: 4,
          ready: 5,
          recentTransitionCount: 6,
          waiters: 7,
        },
        scope: {
          area: "a",
          family: 2,
          realm: "r",
          resource: "q",
        },
      },
      right: {
        metrics: {
          ageSeconds: null,
          backlog: 6,
          deadLetters: 1,
          delayed: 5,
          inflight: 1,
          ready: 10,
          recentTransitionCount: 0,
          waiters: 0,
        },
        scope: {
          area: "b",
          family: null,
          realm: "r2",
          resource: "q2",
        },
      },
      summary: "Queue pressure is higher on the left snapshot",
    });

    expect(
      mapQueueInflight({
        area: "a",
        attempts: 8,
        expires_at: "2026-05-04T12:00:00Z",
        family: 9,
        inflight_token: "tok",
        message_id: 10,
        realm: "r",
        resource: "q",
        session_id: "s",
      }),
    ).toEqual({
      area: "a",
      attempts: 8,
      expiresAt: "2026-05-04T12:00:00Z",
      family: 9,
      inflightToken: "tok",
      messageId: 10,
      realm: "r",
      resource: "q",
      sessionId: "s",
    });

    expect(
      mapKvStats({
        commits_failed_total: 9,
        diagnostics: healthyDiagnostics,
        invalid_transaction_rejects_total: 11,
        keys_total: 4,
        operations_per_second: 1.5,
        rollbacks_total: 10,
        transactions_active: 2,
      }),
    ).toEqual({
      commitsFailedTotal: 9,
      invalidTransactionRejectsTotal: 11,
      keysTotal: 4,
      operationsPerSecond: 1.5,
      rollbacksTotal: 10,
      transactionsActive: 2,
    });

    expect(
      mapLeaseStats({
        acquire_timeouts_total: 4,
        diagnostics: healthyDiagnostics,
        failure_total: 6,
        forced_releases_total: 5,
        leases_active: 3,
        invalid_token_rejects_total: 7,
        oldest_lease_age_seconds: 8,
        ownership_churn_total: 11,
        operations_per_second: 2.5,
        requests_total: 8,
        success_total: 9,
        waiter_depth: 10,
      }),
    ).toEqual({
      acquireTimeoutsTotal: 4,
      forcedReleasesTotal: 5,
      invalidTokenRejectsTotal: 7,
      leasesActive: 3,
      oldestLeaseAgeSeconds: 8,
      operationsPerSecond: 2.5,
      waiterDepth: 10,
    });

    expect(
      mapNoticeStats({
        delivery_drops_total: 4,
        diagnostics: healthyDiagnostics,
        failure_total: 6,
        publishes_per_second: 1.25,
        routes_active: 2,
        max_route_subscribers: 5,
        requests_total: 7,
        success_total: 8,
        subscriptions_active: 9,
        unsubscribes_total: 10,
        wildcard_limit_rejects_total: 10,
      }),
    ).toEqual({
      publishesPerSecond: 1.25,
      deliveryDropsTotal: 4,
      routesActive: 2,
      wildcardLimitRejectsTotal: 10,
      subscriptionsActive: 9,
      maxRouteSubscribers: 5,
    });

    expect(
      mapRpcStats({
        acks_rejected_wrong_worker_total: 14,
        backpressure_rejects_total: 8,
        duplicate_correlation_rejects_total: 9,
        diagnostics: healthyDiagnostics,
        failure_total: 6,
        invalid_sequence_errors_dropped_total: 18,
        invalid_sequence_errors_forwarded_total: 17,
        invalid_sequence_responses_total: 16,
        operations_per_second: 5.5,
        oldest_pending_request_age_seconds: 11,
        pending_routes_active: 12,
        slowest_worker_average_latency_ms: 4.5,
        request_timeouts_total: 7,
        requests_pending: 6,
        requests_total: 10,
        responses_dropped_closed_caller_total: 11,
        responses_missing_pending_total: 12,
        worker_latency_buckets: {
          under_5ms: 1,
          under_25ms: 0,
          under_100ms: 0,
          over_100ms: 0,
        },
        success_total: 13,
        wrong_worker_rejects_total: 15,
        workers_registered: 7,
      }),
    ).toEqual({
      invalidSequenceErrorsDroppedTotal: 18,
      invalidSequenceErrorsForwardedTotal: 17,
      invalidSequenceResponsesTotal: 16,
      failureTotal: 6,
      operationsPerSecond: 5.5,
      pendingRoutesActive: 12,
      requestsPending: 6,
      responsesDroppedClosedCallerTotal: 11,
      responsesMissingPendingTotal: 12,
      requestTimeoutsTotal: 7,
      workersRegistered: 7,
    });

    expect(
      mapScheduleStats({
        ack_failures_total: 1,
        cancel_persistence_failures_total: 9,
        create_persistence_failures_total: 7,
        diagnostics: healthyDiagnostics,
        executions_per_minute: 2.5,
        oldest_pending_claim_age_seconds: 0,
        request_latency_buckets: {
          under_1ms: 8,
          under_5ms: 9,
          under_10ms: 10,
          under_50ms: 11,
          under_100ms: 12,
          under_500ms: 13,
          under_1s: 14,
          under_5s: 15,
          over_5s: 16,
        },
        notify_failures_total: 3,
        pending_claim_cleanup_failures_total: 6,
        pending_claims_expired_total: 5,
        pending_ack_retries: 0,
        overdue_normalizations_total: 4,
        pending_fire_claims: 5,
        schedules_active: 6,
        subscriptions_active: 7,
        upsert_persistence_failures_total: 8,
      }),
    ).toEqual({
      ackFailuresTotal: 1,
      cancelPersistenceFailuresTotal: 9,
      createPersistenceFailuresTotal: 7,
      executionsPerMinute: 2.5,
      notifyFailuresTotal: 3,
      overdueNormalizationsTotal: 4,
      pendingFireClaims: 5,
      schedulesActive: 6,
      subscriptionsActive: 7,
      upsertPersistenceFailuresTotal: 8,
    });

    expect(
      mapStreamStats({
        diagnostics: healthyDiagnostics,
        append_sessions_active: 0,
        append_sessions_ended_total: 0,
        append_sessions_started_total: 0,
        append_conflicts_total: 15,
        failure_total: 16,
        events_total: 11,
        operations_per_second: 12.5,
        notify_drops_total: 17,
        request_latency_buckets: {
          under_1ms: 22,
          under_5ms: 23,
          under_10ms: 24,
          under_50ms: 25,
          under_100ms: 26,
          under_500ms: 27,
          under_1s: 28,
          under_5s: 29,
          over_5s: 30,
        },
        watermark_lag_buckets: {
          caught_up: 18,
          over_100: 21,
          under_10: 19,
          under_100: 20,
        },
        requests_total: 18,
        success_total: 19,
        streams_active: 13,
        subscriptions_active: 14,
      }),
    ).toEqual({
      eventsTotal: 11,
      operationsPerSecond: 12.5,
      watermarkLagBuckets: {
        caughtUp: 18,
        over100: 21,
        under10: 19,
        under100: 20,
      },
      streamsActive: 13,
      subscriptionsActive: 14,
    });
  });

  it("maps sessions and system overview DTOs", () => {
    expect(
      mapActiveSession({
        connected_at: "2026-05-04T12:00:00Z",
        idle_seconds: 6,
        identity_claim: "tid",
        identity_value: "r",
        messages_received: 7,
        messages_sent: 8,
        remote_addr: "127.0.0.1",
        route_family: 2,
        session_id: "sess-1",
        subject: "user:1",
        transport: "ws",
      }),
    ).toEqual({
      key: "sess-1:2:r:127.0.0.1:2026-05-04T12:00:00Z",
      connectedAt: "2026-05-04T12:00:00Z",
      idleSeconds: 6,
      identityClaim: "tid",
      identityValue: "r",
      messagesReceived: 7,
      messagesSent: 8,
      remoteAddress: "127.0.0.1",
      routeFamily: 2,
      sessionId: "sess-1",
      subject: "user:1",
      transport: "ws",
    });

    expect(
      mapActiveSessionsOverview([
        {
          session_id: "sess-1",
          route_family: 2,
          identity_value: "r",
        },
      ]),
    ).toEqual({
      sessions: [
        {
          key: "sess-1:2:r",
          identityValue: "r",
          routeFamily: 2,
          sessionId: "sess-1",
        },
      ],
    });

    const systemOverview = mapSystemOverview(
      {
        broker: {
          connections: 2,
          messages_per_second: 3.5,
          realms: ["r"],
          router_backpressure_total: 0,
          router_high_lane_backpressure_total: 0,
          sessions: 4,
          uptime_seconds: 5,
        },
        diagnostics: healthyGlobalDiagnostics,
        domains: {
          kv: {
            commits_failed_total: 41,
            diagnostics: healthyDiagnostics,
            invalid_transaction_rejects_total: 43,
            keys_total: 6,
            operations_per_second: 7.5,
            rollbacks_total: 42,
            transactions_active: 8,
          },
          lease: {
            acquire_timeouts_total: 1,
            diagnostics: healthyDiagnostics,
            failure_total: 3,
            forced_releases_total: 2,
            leases_active: 9,
            invalid_token_rejects_total: 4,
            oldest_lease_age_seconds: 5,
            ownership_churn_total: 14,
            operations_per_second: 10.5,
            requests_total: 5,
            success_total: 6,
            waiter_depth: 7,
          },
          notice: {
            delivery_drops_total: 1,
            diagnostics: healthyDiagnostics,
            failure_total: 3,
            max_route_subscribers: 2,
            publishes_per_second: 11.5,
            routes_active: 2,
            requests_total: 2,
            success_total: 4,
            subscriptions_active: 12,
            unsubscribes_total: 13,
            wildcard_limit_rejects_total: 5,
          },
          queue: {
            completes_total: 33,
            diagnostics: healthyDiagnostics,
            enqueues_total: 34,
            extends_total: 35,
            failure_total: 36,
            inflight_active: 13,
            messages_dead_lettered: 14,
            messages_delayed: 15,
            messages_pending: 16,
            messages_ready: 17,
            oldest_message_age_seconds: 18,
            oldest_backlog_age_seconds: 19,
            backlog_age_buckets: {
              under_1m: 4,
              under_5m: 5,
              under_15m: 6,
              over_15m: 7,
            },
            delay_age_buckets: {
              under_1m: 1,
              under_5m: 2,
              under_15m: 3,
              over_15m: 4,
            },
            complete_rejected_total: 21,
            dead_letter_transitions_total: 20,
            notify_drops_total: 19,
            redeliveries_total: 20,
            operations_per_second: 18.5,
            releases_total: 37,
            requests_total: 38,
            reserves_total: 39,
            success_total: 40,
          },
          rpc: {
            acks_rejected_wrong_worker_total: 7,
            backpressure_rejects_total: 8,
            duplicate_correlation_rejects_total: 9,
            diagnostics: healthyDiagnostics,
            failure_total: 10,
            invalid_sequence_errors_dropped_total: 19,
            invalid_sequence_errors_forwarded_total: 18,
            invalid_sequence_responses_total: 17,
            operations_per_second: 19.5,
            oldest_pending_request_age_seconds: 22,
            pending_routes_active: 23,
            slowest_worker_average_latency_ms: 4.5,
            request_timeouts_total: 11,
            requests_pending: 20,
            requests_total: 12,
            responses_dropped_closed_caller_total: 13,
            responses_missing_pending_total: 14,
            worker_latency_buckets: {
              under_5ms: 1,
              under_25ms: 0,
              under_100ms: 0,
              over_100ms: 0,
            },
            success_total: 15,
            wrong_worker_rejects_total: 16,
            workers_registered: 21,
          },
          schedule: {
            ack_failures_total: 22,
            cancel_persistence_failures_total: 43,
            create_persistence_failures_total: 41,
            diagnostics: healthyDiagnostics,
            executions_per_minute: 23.5,
            oldest_pending_claim_age_seconds: 30,
            request_latency_buckets: {
              under_1ms: 33,
              under_5ms: 34,
              under_10ms: 35,
              under_50ms: 36,
              under_100ms: 37,
              under_500ms: 38,
              under_1s: 39,
              under_5s: 40,
              over_5s: 41,
            },
            notify_failures_total: 24,
            pending_claim_cleanup_failures_total: 27,
            pending_claims_expired_total: 26,
            pending_ack_retries: 29,
            overdue_normalizations_total: 25,
            pending_fire_claims: 26,
            schedules_active: 27,
            subscriptions_active: 28,
            upsert_persistence_failures_total: 42,
          },
          stream: {
            append_sessions_active: 0,
            append_sessions_ended_total: 0,
            append_sessions_started_total: 0,
            append_conflicts_total: 1,
            diagnostics: healthyDiagnostics,
            failure_total: 3,
            events_total: 29,
            operations_per_second: 30.5,
            notify_drops_total: 2,
            request_latency_buckets: {
              under_1ms: 33,
              under_5ms: 34,
              under_10ms: 35,
              under_50ms: 36,
              under_100ms: 37,
              under_500ms: 38,
              under_1s: 39,
              under_5s: 40,
              over_5s: 41,
            },
            watermark_lag_buckets: {
              caught_up: 24,
              over_100: 27,
              under_10: 25,
              under_100: 26,
            },
            requests_total: 4,
            success_total: 5,
            streams_active: 31,
            subscriptions_active: 32,
          },
        },
      },
      "line-a\nline-b",
    );

    expect(Date.parse(systemOverview.fetchedAt)).not.toBeNaN();
    expect(systemOverview).toEqual({
      broker: {
        connections: 2,
        messagesPerSecond: 3.5,
        realms: ["r"],
        sessions: 4,
        uptimeSeconds: 5,
      },
      diagnostics: healthyGlobalDiagnostics,
      domains: {
        kv: {
          commitsFailedTotal: 41,
          invalidTransactionRejectsTotal: 43,
          keysTotal: 6,
          operationsPerSecond: 7.5,
          rollbacksTotal: 42,
          transactionsActive: 8,
        },
        lease: {
          acquireTimeoutsTotal: 1,
          failureTotal: 3,
          forcedReleasesTotal: 2,
          leasesActive: 9,
          invalidTokenRejectsTotal: 4,
          oldestLeaseAgeSeconds: 5,
          operationsPerSecond: 10.5,
          requestsTotal: 5,
          successTotal: 6,
          waiterDepth: 7,
        },
        notice: {
          deliveryDropsTotal: 1,
          failureTotal: 3,
          publishesPerSecond: 11.5,
          requestsTotal: 2,
          successTotal: 4,
          subscriptionsActive: 12,
          unsubscribesTotal: 13,
          wildcardLimitRejectsTotal: 5,
        },
        queue: {
          inflightActive: 13,
          messagesDeadLettered: 14,
          messagesDelayed: 15,
          messagesPending: 16,
          messagesReady: 17,
          operationsPerSecond: 18.5,
        },
        rpc: {
          acksRejectedWrongWorkerTotal: 7,
          backpressureRejectsTotal: 8,
          duplicateCorrelationRejectsTotal: 9,
          invalidSequenceErrorsDroppedTotal: 19,
          invalidSequenceErrorsForwardedTotal: 18,
          invalidSequenceResponsesTotal: 17,
          operationsPerSecond: 19.5,
          pendingRoutesActive: 23,
          failureTotal: 10,
          requestsPending: 20,
          requestTimeoutsTotal: 11,
          requestsTotal: 12,
          responsesDroppedClosedCallerTotal: 13,
          responsesMissingPendingTotal: 14,
          successTotal: 15,
          wrongWorkerRejectsTotal: 16,
          workersRegistered: 21,
        },
        schedule: {
          ackFailuresTotal: 22,
          cancelPersistenceFailuresTotal: 43,
          createPersistenceFailuresTotal: 41,
          executionsPerMinute: 23.5,
          notifyFailuresTotal: 24,
          overdueNormalizationsTotal: 25,
          pendingFireClaims: 26,
          schedulesActive: 27,
          subscriptionsActive: 28,
          upsertPersistenceFailuresTotal: 42,
        },
        stream: {
          appendConflictsTotal: 1,
          failureTotal: 3,
          eventsTotal: 29,
          operationsPerSecond: 30.5,
          notifyDropsTotal: 2,
          requestsTotal: 4,
          successTotal: 5,
          streamsActive: 31,
          subscriptionsActive: 32,
        },
      },
      fetchedAt: systemOverview.fetchedAt,
      metrics: {
        raw: "line-a\nline-b",
        lines: ["line-a", "line-b"],
        lineCount: 2,
      },
    });
  });
});
