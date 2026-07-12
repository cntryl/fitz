import type { Page } from "@playwright/test";
import type {
  DiagnosticSnapshot,
  GlobalStats,
  MessagingTopology,
  StructuredMetricsResponse,
} from "@/adapters";
import { dashboardDiagnostics, topologyDtoLane, topologyOverview } from "../../fixtures/topology";

export type DomainOverviewFixture = {
  realms: (Record<string, unknown> & {
    realm: string;
  })[];
  stats: Record<string, unknown>;
};

export const adminFeatures = {
  admin_auth_required: false,
  admin_auth_mode: "open" as const,
  route_families: ["1", "7", "42"],
  route_families_wildcard: false,
};

export async function mockAdminFeatures(page: Page) {
  await page.route("**/api/v1/features", async (route) => {
    await route.fulfill({
      json: adminFeatures,
    });
  });

  await page.route("**/api/v1/*/stats", async (route) => {
    await route.fulfill({
      json: makeGlobalStatsPayload(),
    });
  });

  await page.route("**/api/v1/*/topology", async (route) => {
    await route.fulfill({
      json: topologyApiPayload,
    });
  });

  await page.route("**/api/v1/*/metrics", async (route) => {
    await route.fulfill({ json: structuredMetricsPayload });
  });
}

export const topologyApiPayload: MessagingTopology = {
  broker: {
    connections: topologyOverview.broker.connections,
    messages_per_second: topologyOverview.broker.messagesPerSecond,
    realms: topologyOverview.broker.realms,
    router_backpressure_total: topologyOverview.broker.routerBackpressureTotal,
    router_high_lane_backpressure_total: topologyOverview.broker.routerHighLaneBackpressureTotal,
    sessions: topologyOverview.broker.sessions,
    uptime_seconds: topologyOverview.broker.uptimeSeconds,
  },
  connections: {
    items: [
      {
        id: "queue-inflight:1:42",
        kind: "queue_inflight_consumer",
        label: "ops / primary inflight",
        metrics: [{ key: "attempts", label: "Attempts", value: 2 }],
        scope: {
          area: "ops",
          realm: "default",
          resource: "primary",
          route_family: 1,
          session_id: "session-1",
        },
        source: "domain:queue",
        state: "flowing",
        target: "session:session-1",
      },
    ],
    limit: 250,
    total: 1,
    truncated: false,
  },
  diagnostics: dashboardDiagnostics,
  generated_at: "2026-06-23T18:30:00.000Z",
  lanes: [
    topologyDtoLane("queue", "blocked", 4),
    topologyDtoLane("rpc", "flowing", 4),
    topologyDtoLane("notice", "flowing", 7),
    topologyDtoLane("schedule", "pressure", 5),
    topologyDtoLane("stream", "flowing", 9),
    topologyDtoLane("lease", "flowing", 3),
    topologyDtoLane("kv", "flowing", 12),
  ],
  session_groups: [
    {
      max_idle_seconds: 12,
      messages_received: 2,
      messages_sent: 3,
      representative_sessions: [],
      route_family: 1,
      sessions: 1,
      transports: ["ws"],
    },
  ],
};

export const domainOverviewPages = [
  {
    path: "/admin/1/kv",
    domain: "kv",
    heading: "KV tables",
  },
  {
    path: "/admin/1/lease",
    domain: "lease",
    heading: "Lease inventory",
  },
  {
    path: "/admin/1/notice",
    domain: "notice",
    heading: "Notice inventory",
  },
  {
    path: "/admin/1/rpc",
    domain: "rpc",
    heading: "RPC inventory",
  },
  {
    path: "/admin/1/schedule",
    domain: "schedule",
    heading: "Schedule inventory",
  },
  {
    path: "/admin/1/stream",
    domain: "stream",
    heading: "Stream inventory",
  },
  {
    path: "/admin/1/queue",
    domain: "queue",
    heading: "Queue inventory",
  },
];

export const queueOperationalFixture = {
  complete_success_total: 12,
  enqueue_success_total: 24,
  in_rate_per_second: 1.5,
  messages_dead_lettered: 0,
  messages_delayed: 6,
  messages_inflight: 3,
  messages_ready: 7,
  messages_total: 16,
  oldest_backlog_age_seconds: 28,
  out_rate_per_second: 0.75,
  status: "falling_behind",
  subscriptions_active: 2,
};

export function queueAreaFixture(realm = "default", area = "ops") {
  return {
    ...queueOperationalFixture,
    area,
    queue_count: 1,
    realm,
  };
}

export function queueResourceFixture(realm = "default", area = "ops", resource = "primary") {
  return {
    ...queueOperationalFixture,
    area,
    family_count: 1,
    realm,
    resource,
  };
}

export function queueRealmFixture(realm = "default") {
  return {
    ...queueOperationalFixture,
    area_count: 1,
    queue_count: 1,
    realm,
  };
}

export function queueRealmDetailFixture(realm = "default") {
  return {
    ...queueRealmFixture(realm),
    areas: [queueAreaFixture(realm)],
    queues: [queueResourceFixture(realm)],
  };
}

export function queueAreaDetailFixture(realm = "default", area = "ops") {
  return {
    ...queueAreaFixture(realm, area),
    queues: [queueResourceFixture(realm, area)],
  };
}

export const domainOverviewData: Record<string, DomainOverviewFixture> = {
  kv: {
    realms: [
      {
        realm: "default",
      },
      {
        realm: "analytics",
      },
    ],
    stats: {
      commits_failed_total: 0,
      invalid_transaction_rejects_total: 0,
      keys_total: 1280,
      operations_per_second: 2.75,
      transactions_active: 22,
    },
  },
  lease: {
    realms: [{ realm: "default" }],
    stats: {
      acquire_timeouts_total: 0,
      forced_releases_total: 0,
      invalid_token_rejects_total: 1,
      leases_active: 18,
      oldest_lease_age_seconds: 47,
      operations_per_second: 1.9,
      waiter_depth: 3,
    },
  },
  notice: {
    realms: [{ realm: "default" }],
    stats: {
      delivery_drops_total: 0,
      publishes_per_second: 4.1,
      routes_active: 2,
      wildcard_limit_rejects_total: 0,
      subscriptions_active: 9,
      max_route_subscribers: 8,
    },
  },
  rpc: {
    realms: [{ realm: "default" }],
    stats: {
      failure_total: 0,
      invalid_sequence_errors_dropped_total: 0,
      invalid_sequence_errors_forwarded_total: 0,
      invalid_sequence_responses_total: 0,
      operations_per_second: 6.2,
      requests_pending: 4,
      pending_routes_active: 1,
      responses_dropped_closed_caller_total: 0,
      responses_missing_pending_total: 0,
      request_timeouts_total: 0,
      workers_registered: 18,
    },
  },
  queue: {
    realms: [queueRealmFixture()],
    stats: {
      inflight_active: 3,
      messages_dead_lettered: 0,
      messages_delayed: 6,
      messages_pending: 12,
      messages_ready: 7,
      oldest_backlog_age_seconds: 28,
      operations_per_second: 14.8,
    },
  },
  schedule: {
    realms: [{ realm: "default" }],
    stats: {
      ack_failures_total: 0,
      cancel_persistence_failures_total: 0,
      create_persistence_failures_total: 0,
      executions_per_minute: 9.5,
      notify_failures_total: 0,
      overdue_normalizations_total: 0,
      pending_fire_claims: 1,
      schedules_active: 27,
      subscriptions_active: 3,
      upsert_persistence_failures_total: 0,
    },
  },
  stream: {
    realms: [{ realm: "default" }],
    stats: {
      events_total: 840,
      operations_per_second: 3.5,
      streams_active: 12,
      subscriptions_active: 4,
      watermark_lag_buckets: {
        caught_up: 11,
        over_100: 0,
        under_10: 6,
        under_100: 2,
      },
    },
  },
};

export type DomainOverviewOverride = Partial<DomainOverviewFixture>;

export const domainApiSegments = new Set([
  "kv",
  "queue",
  "stream",
  "lease",
  "schedule",
  "notice",
  "rpc",
]);

export function normalizedAdminApiSegments(pathname: string) {
  const segments = pathname.split("/").filter(Boolean);

  if (segments[0] === "api" && segments[1] === "v1" && domainApiSegments.has(segments[3] ?? "")) {
    return [segments[0], segments[1], segments[3], ...segments.slice(4)];
  }

  return segments;
}

export function applyLeaseOverride(overrides?: DomainOverviewOverride) {
  const base = domainOverviewData.lease;
  return {
    ...base,
    ...overrides,
    realms: overrides?.realms ?? base.realms,
    stats: {
      ...base.stats,
      ...overrides?.stats,
    },
  };
}

export function domainAreasByRealm(_domain: string, _realm: string) {
  return ["default", "analytics"];
}

export function domainResourcesByArea(domain: string, realm: string, area: string) {
  return (
    {
      [domain]: {
        [realm]: {
          [area]: [{ resource: "primary" }, { resource: "tenant-dashboard-stateful-resource" }],
        },
      },
    }[domain]?.[realm]?.[area] ?? [{ resource: "primary" }]
  );
}

export function noticeDeliveriesFixture(options: {
  area?: string;
  limit?: number;
  operation?: string | null;
  realm?: string;
  resource?: string;
}) {
  const rows = [
    {
      area: options.area ?? "ops",
      notifications_received: 12,
      publishes_per_minute: 30,
      publishes_total: 120,
      realm: options.realm ?? "default",
      resource: options.resource ?? "primary",
      route: "GetStatus",
      session_id: "session-1",
      status: "open",
      subscription_id: 11,
    },
    {
      area: options.area ?? "ops",
      notifications_received: 8,
      publishes_per_minute: 11,
      publishes_total: 45,
      realm: options.realm ?? "default",
      resource: options.resource ?? "primary",
      route: "Stream",
      session_id: "session-2",
      status: "open",
      subscription_id: 12,
    },
  ];

  const observations = options.operation
    ? rows.filter((row) => row.route === options.operation)
    : rows;

  return {
    area: options.area ?? "ops",
    limit: options.limit ?? 50,
    observations,
    realm: options.realm ?? "default",
    route_family: 7,
  };
}

export async function mockDomainOverviewApis(
  page: Page,
  overrides: Partial<Record<string, DomainOverviewOverride>> = {},
) {
  await mockAdminFeatures(page);

  await page.route("**/api/v1/**", async (route) => {
    const parsed = new URL(route.request().url());
    const segments = normalizedAdminApiSegments(parsed.pathname);
    if (segments.length === 3 && segments[2] === "topology") {
      await route.fulfill({
        json: topologyApiPayload,
      });
      return;
    }

    if (segments.length < 3) {
      await route.continue();
      return;
    }

    const domain = segments[2] ?? "";
    if (domain === "features") {
      await route.fulfill({
        json: adminFeatures,
      });
      return;
    }

    const baseFixture = domainOverviewData[domain as keyof typeof domainOverviewData];
    const override = overrides[domain];
    const domainFixture = baseFixture
      ? {
          ...baseFixture,
          ...override,
          realms: override?.realms ?? baseFixture.realms,
          stats: {
            ...baseFixture.stats,
            ...override?.stats,
          },
        }
      : null;
    if (!domainFixture) {
      await route.continue();
      return;
    }

    const detail = segments[segments.length - 1];
    if (segments.length === 4 && detail === "stats") {
      await route.fulfill({
        json: domainFixture.stats,
      });
      return;
    }

    if (segments.length === 4 && detail === "realms") {
      await route.fulfill({
        json: { realms: domainFixture.realms },
      });
      return;
    }

    if (segments.length === 4 && detail === "deliveries") {
      const realm = parsed.searchParams.get("realm") ?? "";
      const area = parsed.searchParams.get("area") ?? "";
      const resource = parsed.searchParams.get("resource") ?? "";
      const operation = parsed.searchParams.get("q");
      const limit = Number(parsed.searchParams.get("limit") || 50);

      await route.fulfill({
        json: noticeDeliveriesFixture({
          area,
          limit,
          operation,
          realm,
          resource,
        }),
      });
      return;
    }

    if (domain === "queue" && segments[3] === "realms") {
      const realm = decodeURIComponent(segments[4] ?? "default");

      if (segments.length === 5) {
        await route.fulfill({
          json: queueRealmDetailFixture(realm),
        });
        return;
      }

      if (segments.length === 6 && detail === "areas") {
        await route.fulfill({
          json: { areas: [queueAreaFixture(realm)], realm },
        });
        return;
      }

      if (segments.length === 7 && segments[5] === "areas") {
        const area = decodeURIComponent(segments[6] ?? "ops");
        await route.fulfill({
          json: queueAreaDetailFixture(realm, area),
        });
        return;
      }

      if (segments.length === 8 && detail === "resources") {
        const area = decodeURIComponent(segments[6] ?? "ops");
        await route.fulfill({
          json: {
            area,
            realm,
            resources: [queueResourceFixture(realm, area)],
          },
        });
        return;
      }
    }

    if (segments.length === 6 && detail === "areas") {
      const realm = decodeURIComponent(segments[4] ?? "");
      const areas = domainAreasByRealm(domain, realm).map((entry) => ({ area: entry }));
      await route.fulfill({
        json: { areas },
      });
      return;
    }

    if (segments.length === 8 && detail === "resources") {
      const realm = decodeURIComponent(segments[4] ?? "");
      const area = decodeURIComponent(segments[6] ?? "");
      await route.fulfill({
        json: {
          resources: domainResourcesByArea(domain, realm, area),
        },
      });
      return;
    }

    await route.continue();
  });
}

export type SessionsPayload = {
  sessions: Array<{
    connected_at?: string;
    idle_seconds?: number;
    identity_claim?: string;
    identity_value?: string;
    messages_received?: number;
    messages_sent?: number;
    remote_addr?: string;
    route_family?: number;
    session_id?: string;
    subject?: string;
    transport?: string;
  }>;
};

export const sessionsWithData: SessionsPayload = {
  sessions: [
    {
      connected_at: "2026-05-21T13:00:00Z",
      idle_seconds: 12,
      identity_claim: "tid",
      identity_value: "default",
      messages_received: 2,
      messages_sent: 3,
      remote_addr: "127.0.0.1",
      route_family: 1,
      session_id: "session-1",
      subject: "user:1",
      transport: "ws",
    },
    {
      connected_at: "2026-05-21T13:01:00Z",
      idle_seconds: 45,
      identity_claim: "tenant",
      identity_value: "ops",
      messages_received: 4,
      messages_sent: 8,
      remote_addr: "2001:db8::1ff:fe23:4567:890a",
      route_family: 2,
      session_id: "session-long-id-2",
      subject: "user:2",
      transport: "http",
    },
  ],
};

export const sessionsEmpty: SessionsPayload = {
  sessions: [],
};

export const structuredMetricsPayload: StructuredMetricsResponse = {
  scope: "family",
  family: 1,
  generated_at: 1719167400000,
  samples: [
    {
      name: "fitz_broker_uptime_seconds",
      kind: "gauge",
      help: "Broker uptime",
      labels: { family: "1" },
      value: 120,
    },
    {
      name: "fitz_queue_ready",
      kind: "gauge",
      help: "Ready queue messages",
      labels: { area: "jobs", family: "1", realm: "default" },
      value: 7,
    },
    {
      name: "fitz_rpc_requests_total",
      kind: "counter",
      help: "RPC requests",
      labels: { family: "1", realm: "default" },
      value: 19,
    },
  ],
};

export async function mockSessionsApi(page: Page, payload: SessionsPayload) {
  await mockAdminFeatures(page);

  await page.route("**/api/v1/*/sessions", async (route) => {
    await route.fulfill({
      json: payload,
    });
  });
}

export async function mockMetricsApi(
  page: Page,
  payload: StructuredMetricsResponse = structuredMetricsPayload,
) {
  await mockAdminFeatures(page);

  await page.route("**/api/v1/*/metrics", async (route) => {
    await route.fulfill({ json: payload });
  });
}

export function makeDiagnosticSnapshot(
  overrides: Partial<DiagnosticSnapshot> = {},
): DiagnosticSnapshot {
  return {
    age_seconds: overrides.age_seconds ?? 12,
    confidence: 1,
    confidence_justification: {
      rationale: "Sprint 16 route fixture",
      signals_matched: [],
      signals_missing: [],
      ...overrides.confidence_justification,
    },
    contention_count: 0,
    current_stage: "healthy",
    explanation_hints: ["Sprint 16 fixture route state."],
    failure_count: 0,
    last_changed_at: "2026-05-21T13:00:00.000Z",
    last_failure_at: null,
    last_success_at: "2026-05-21T13:00:00.000Z",
    likely_bottleneck: null,
    recent_transition_count: 0,
    severity: "informational",
    trend: "steady",
    waiter_count: 0,
    ...overrides,
  };
}

export const emptyQueueAgeBuckets = {
  over_15m: 0,
  under_15m: 0,
  under_1m: 0,
  under_5m: 0,
};

export const emptyLatencyBuckets = {
  over_5s: 0,
  under_100ms: 0,
  under_10ms: 0,
  under_1ms: 0,
  under_1s: 0,
  under_500ms: 0,
  under_50ms: 0,
  under_5ms: 0,
  under_5s: 0,
};

export function makeGlobalStatsPayload(): GlobalStats {
  const diagnostics = makeDiagnosticSnapshot();

  return {
    broker: {
      connections: topologyOverview.broker.connections,
      messages_per_second: topologyOverview.broker.messagesPerSecond,
      realms: topologyOverview.broker.realms,
      router_backpressure_total: topologyOverview.broker.routerBackpressureTotal,
      router_high_lane_backpressure_total: topologyOverview.broker.routerHighLaneBackpressureTotal,
      sessions: topologyOverview.broker.sessions,
      uptime_seconds: topologyOverview.broker.uptimeSeconds,
    },
    diagnostics: dashboardDiagnostics,
    domains: {
      kv: {
        ...domainOverviewData.kv.stats,
        diagnostics,
      },
      lease: {
        ...domainOverviewData.lease.stats,
        diagnostics,
        failure_total: 0,
        ownership_churn_total: 0,
        requests_total: 18,
        success_total: 17,
      },
      notice: {
        ...domainOverviewData.notice.stats,
        diagnostics,
        failure_total: 0,
        requests_total: 9,
        success_total: 9,
        unsubscribes_total: 0,
      },
      queue: {
        ...domainOverviewData.queue.stats,
        backlog_age_buckets: emptyQueueAgeBuckets,
        complete_rejected_total: 0,
        completes_total: 4,
        dead_letter_transitions_total: 0,
        delay_age_buckets: emptyQueueAgeBuckets,
        diagnostics,
        enqueues_total: 18,
        extends_total: 0,
        failure_total: 0,
        notify_drops_total: 0,
        oldest_backlog_age_seconds: 28,
        oldest_message_age_seconds: 42,
        redeliveries_total: 0,
        releases_total: 1,
        requests_total: 22,
        reserves_total: 7,
        success_total: 21,
      },
      rpc: {
        ...domainOverviewData.rpc.stats,
        acks_rejected_wrong_worker_total: 0,
        backpressure_rejects_total: 0,
        diagnostics,
        duplicate_correlation_rejects_total: 0,
        oldest_pending_request_age_seconds: 7,
        requests_total: 19,
        slowest_worker_average_latency_ms: 12,
        success_total: 19,
        worker_latency_buckets: {
          over_100ms: 0,
          under_100ms: 0,
          under_25ms: 2,
          under_5ms: 4,
        },
        wrong_worker_rejects_total: 0,
      },
      schedule: {
        ...domainOverviewData.schedule.stats,
        diagnostics,
        oldest_pending_claim_age_seconds: 12,
        pending_ack_retries: 0,
        request_latency_buckets: emptyLatencyBuckets,
      },
      stream: {
        ...domainOverviewData.stream.stats,
        append_conflicts_total: 0,
        append_sessions_active: 1,
        append_sessions_ended_total: 0,
        append_sessions_started_total: 1,
        diagnostics,
        failure_total: 0,
        notify_drops_total: 0,
        request_latency_buckets: emptyLatencyBuckets,
        requests_total: 12,
        success_total: 12,
      },
    },
  } as GlobalStats;
}

export async function mockDiagnosticsApis(page: Page) {
  await mockAdminFeatures(page);

  await page.route("**/api/v1/*/stats", async (route) => {
    await route.fulfill({
      json: makeGlobalStatsPayload(),
    });
  });

  await page.route("**/api/v1/*/topology", async (route) => {
    await route.fulfill({
      json: topologyApiPayload,
    });
  });

  await page.route("**/api/v1/*/metrics", async (route) => {
    await route.fulfill({ json: structuredMetricsPayload });
  });
}

export async function mockHomeRouteApis(page: Page) {
  await mockAdminFeatures(page);

  await page.route("**/api/v1/*/topology", async (route) => {
    await route.fulfill({
      json: topologyApiPayload,
    });
  });
}
