import { expect, test, type Page } from "@playwright/test";
import type { DiagnosticSnapshot, GlobalStats, MessagingTopology } from "@/adapters";
import { dashboardDiagnostics, topologyDtoLane, topologyOverview } from "../fixtures/topology";

async function openDashboard(
  page: Page,
  theme: "light" | "dark" = "light",
  setupApis = true,
) {
  if (theme === "dark") {
    await page.addInitScript(() => {
      localStorage.setItem("fitz-admin-theme", "dark");
    });
  }

  if (setupApis) {
    await mockHomeRouteApis(page);
  }

  await page.goto("/admin");

  await expect(page.locator("main#main-content")).toHaveCount(1);
  const primaryNav = page.getByRole("navigation", { name: "Primary navigation" });
  const viewport = page.viewportSize();
  if ((viewport?.width ?? 0) < 768) {
    await expect(primaryNav.getByRole("button", { name: "Menu" })).toBeVisible();
    return;
  }
  await expect(primaryNav.getByRole("link", { name: "Fitz admin home" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Overview" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Queue" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Diagnostics" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Settings" })).toBeVisible();
  await expect(primaryNav.getByRole("button", { name: "Toggle color theme" })).toBeVisible();
  await expect(primaryNav.getByRole("button", { name: "User menu" })).toBeVisible();
}

type DomainOverviewFixture = {
  realms: {
    realm: string;
  }[];
  stats: Record<string, number | Record<string, number>>;
};

const adminFeatures = {
  admin_auth_required: false,
  admin_auth_mode: "open" as const,
};

async function mockAdminFeatures(page: Page) {
  await page.route("**/api/v1/features", async (route) => {
    await route.fulfill({
      json: adminFeatures,
    });
  });
}

const topologyApiPayload: MessagingTopology = {
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
  generated_at: "2026-05-21T13:10:00.000Z",
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

const domainOverviewPages = [
  {
    path: "/kv",
    domain: "kv",
    heading: "KV overview",
  },
  {
    path: "/lease",
    domain: "lease",
    heading: "Lease overview",
  },
  {
    path: "/notice",
    domain: "notice",
    heading: "Notice overview",
  },
  {
    path: "/rpc",
    domain: "rpc",
    heading: "RPC overview",
  },
  {
    path: "/schedule",
    domain: "schedule",
    heading: "Schedule overview",
  },
  {
    path: "/stream",
    domain: "stream",
    heading: "Stream overview",
  },
  {
    path: "/queue",
    domain: "queue",
    heading: "Queue overview",
  },
];

const domainOverviewData: Record<string, DomainOverviewFixture> = {
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
      rollbacks_total: 0,
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
    realms: [{ realm: "default" }],
    stats: {
      inflight_active: 3,
      messages_dead_lettered: 0,
      messages_delayed: 6,
      messages_pending: 12,
      messages_ready: 7,
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

type DomainOverviewOverride = Partial<DomainOverviewFixture>;

function applyLeaseOverride(overrides?: DomainOverviewOverride) {
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

function domainAreasByRealm(_domain: string, _realm: string) {
  return ["default", "analytics"];
}

function domainResourcesByArea(domain: string, realm: string, area: string) {
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

async function mockDomainOverviewApis(
  page: Page,
  overrides: Partial<Record<string, DomainOverviewOverride>> = {},
) {
  await mockAdminFeatures(page);

  await page.route("**/api/v1/**", async (route) => {
    const parsed = new URL(route.request().url());
    const segments = parsed.pathname.split("/").filter(Boolean);
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

type SessionsPayload = {
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

const sessionsWithData: SessionsPayload = {
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

const sessionsEmpty: SessionsPayload = {
  sessions: [],
};

const metricsPayload = `# HELP fitz_broker_uptime_seconds Broker up
# TYPE fitz_broker_uptime_seconds gauge
fitz_broker_uptime_seconds 120
# HELP fitz_queue_ready Gauge
# TYPE fitz_queue_ready gauge
fitz_queue_ready{realm="default",area="jobs"} 7
# HELP fitz_rpc_requests_total rpc requests
# TYPE fitz_rpc_requests_total counter
fitz_rpc_requests_total{realm="default"} 19
`;

async function mockSessionsApi(page: Page, payload: SessionsPayload) {
  await mockAdminFeatures(page);

  await page.route("**/api/v1/sessions", async (route) => {
    await route.fulfill({
      json: payload,
    });
  });
}

async function mockMetricsApi(page: Page, payload = metricsPayload) {
  await mockAdminFeatures(page);

  await page.route(
    (url) => {
      const parsedUrl = new URL(url);
      return parsedUrl.pathname === "/metrics";
    },
    async (route) => {
      await route.fulfill({
        body: payload,
        contentType: "text/plain; charset=utf-8",
      });
    },
  );
}

type ResourceScope = {
  area: string;
  realm: string;
  resource: string;
};

type ResourceDomain = "kv" | "lease" | "notice" | "rpc" | "schedule" | "stream";

type RouteChrome = "app" | "auth";

type ThemeMode = "light" | "dark";

type ViewportPreset = {
  height: number;
  isMobile: boolean;
  key: "desktop" | "mobile" | "tablet";
  width: number;
};

type RouteScenario = {
  path: string;
  setup: (page: Page) => Promise<void>;
  shell: RouteChrome;
  title: string | RegExp;
};

function exactHeadingMatcher(title: string | RegExp): string | RegExp {
  if (title instanceof RegExp) {
    return title;
  }

  const escaped = title.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(`^${escaped}$`);
}

const viewportPresets: ViewportPreset[] = [
  {
    height: 1200,
    isMobile: false,
    key: "desktop",
    width: 1440,
  },
  {
    height: 1200,
    isMobile: false,
    key: "tablet",
    width: 1024,
  },
  {
    height: 844,
    isMobile: true,
    key: "mobile",
    width: 390,
  },
];

const themeModes: ThemeMode[] = ["light", "dark"];

function normalizeRoute(path: string) {
  if (path === "/") {
    return "home";
  }

  return path
    .replace(/^\//, "")
    .replace(/[^a-zA-Z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function makeDiagnosticSnapshot(overrides: Partial<DiagnosticSnapshot> = {}): DiagnosticSnapshot {
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

const emptyQueueAgeBuckets = {
  over_15m: 0,
  under_15m: 0,
  under_1m: 0,
  under_5m: 0,
};

const emptyLatencyBuckets = {
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

function makeGlobalStatsPayload(): GlobalStats {
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
        pending_claim_cleanup_failures_total: 0,
        pending_claims_expired_total: 0,
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

async function mockDiagnosticsApis(page: Page) {
  await mockAdminFeatures(page);

  await page.route("**/api/v1/stats", async (route) => {
    await route.fulfill({
      json: makeGlobalStatsPayload(),
    });
  });

  await page.route("**/api/v1/topology", async (route) => {
    await route.fulfill({
      json: topologyApiPayload,
    });
  });

  await page.route(
    (url) => {
      const parsedUrl = new URL(url);
      return parsedUrl.pathname === "/metrics";
    },
    async (route) => {
      await route.fulfill({
        body: metricsPayload,
        contentType: "text/plain; charset=utf-8",
      });
    },
  );
}

function parseResourceScope(segments: string[]): ResourceScope | null {
  if (segments.length < 9) {
    return null;
  }

  if (segments[3] !== "realms" || segments[5] !== "areas" || segments[7] !== "resources") {
    return null;
  }

  return {
    area: decodeURIComponent(segments[6] ?? ""),
    realm: decodeURIComponent(segments[4] ?? ""),
    resource: decodeURIComponent(segments[8] ?? ""),
  };
}

function parseRouteResourceScope(path: string): ResourceScope {
  const parts = path.split("?")[0].split("/").filter(Boolean);
  return {
    area: decodeURIComponent(parts[2] ?? ""),
    realm: decodeURIComponent(parts[1] ?? ""),
    resource: decodeURIComponent(parts[3] ?? ""),
  };
}

async function mockHomeRouteApis(page: Page) {
  await mockAdminFeatures(page);

  await page.route("**/api/v1/topology", async (route) => {
    await route.fulfill({
      json: topologyApiPayload,
    });
  });
}

function resourceDetailFixture(
  domain: ResourceDomain,
  scope: ResourceScope,
  diagnostics: DiagnosticSnapshot,
) {
  if (domain === "kv") {
    return {
      area: scope.area,
      diagnostics,
      realm: scope.realm,
      resource: scope.resource,
      transactions_active: 18,
    };
  }

  if (domain === "lease") {
    return {
      active_leases: 4,
      area: scope.area,
      diagnostics,
      oldest_lease_age_seconds: 47,
      realm: scope.realm,
      resource: scope.resource,
    };
  }

  if (domain === "notice") {
    return {
      area: scope.area,
      diagnostics,
      realm: scope.realm,
      resource: scope.resource,
      subscriptions_active: 9,
    };
  }

  if (domain === "rpc") {
    return {
      area: scope.area,
      diagnostics,
      operations: [{ operation: "GetStatus" }, { operation: "SetState" }],
      realm: scope.realm,
      resource: scope.resource,
    };
  }

  if (domain === "schedule") {
    return {
      area: scope.area,
      cron: "*/5 * * * *",
      diagnostics,
      enabled: true,
      executions_total: 42,
      next_run: "2026-05-21T13:01:00.000Z",
      realm: scope.realm,
      resource: scope.resource,
    };
  }

  return {
    area: scope.area,
    diagnostics,
    offset: 1200,
    realm: scope.realm,
    resource: scope.resource,
    sessions_active: 3,
    size_bytes: 4096,
    watermark: 1210,
  };
}

function resourceTimelineFixture(domain: string, scope: ResourceScope) {
  return {
    area: scope.area,
    derived: false,
    domain,
    events: [
      {
        age_seconds: 5,
        area: scope.area,
        attempts: 2,
        correlation_id: "corr-1",
        domain,
        kind: "observation",
        message_id: 100,
        observed_at: "2026-05-21T13:00:00.000Z",
        operation: "GetStatus",
        owner_session: "session-1",
        realm: scope.realm,
        resource: scope.resource,
        summary: "Sample route transition observed.",
        worker_session: "worker-1",
      },
    ],
    family: null,
    limit: 12,
    realm: scope.realm,
    resource: scope.resource,
  };
}

async function mockResourceDetailApis(
  page: Page,
  domain: ResourceDomain,
  routeScope: ResourceScope,
) {
  const diagnostics = makeDiagnosticSnapshot();

  await mockAdminFeatures(page);

  await page.route("**/api/v1/**", async (route) => {
    const parsed = new URL(route.request().url());
    const segments = parsed.pathname.split("/").filter(Boolean);

    if (segments[2] === "features") {
      await route.fulfill({
        json: adminFeatures,
      });
      return;
    }

    if (segments.length < 3 || segments[2] !== domain) {
      await route.continue();
      return;
    }

    if (domain === "stream" && segments.length === 8) {
      if (segments[3] === "realms" && segments[5] === "areas" && segments[7] === "watermarks") {
        const realm = decodeURIComponent(segments[4] ?? "");
        const area = decodeURIComponent(segments[6] ?? "");

        if (realm === routeScope.realm && area === routeScope.area) {
          await route.fulfill({
            json: {
              area,
              family_watermarks: [{ family: 1, watermark: 20 }],
              realm,
              resource_count: 2,
            },
          });
          return;
        }
      }
    }

    if (domain === "rpc" && segments.length === 4 && segments[3] === "pending") {
      await route.fulfill({
        json: {
          requests: [
            {
              age_seconds: 7,
              correlation_id: "corr-1",
              route: "GetStatus",
              submitted_at: "2026-05-21T13:00:00.000Z",
              worker_session_id: "worker-1",
            },
          ],
        },
      });
      return;
    }

    const scope = parseResourceScope(segments);

    if (!scope) {
      await route.continue();
      return;
    }

    if (
      scope.area !== routeScope.area ||
      scope.realm !== routeScope.realm ||
      scope.resource !== routeScope.resource
    ) {
      await route.continue();
      return;
    }

    if (segments.length === 9) {
      await route.fulfill({
        json: resourceDetailFixture(domain, scope, diagnostics),
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "events") {
      await route.fulfill({
        json: resourceTimelineFixture(domain, scope),
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "transactions" && domain === "kv") {
      await route.fulfill({
        json: {
          transactions: [
            {
              area: scope.area,
              idle_seconds: 11,
              mode: "write",
              operations_count: 4,
              realm: scope.realm,
              resource: scope.resource,
              started_at: "2026-05-21T13:00:00.000Z",
              tx_id: 101,
            },
          ],
        },
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "subscriptions" && domain === "notice") {
      await route.fulfill({
        json: {
          subscriptions: [
            {
              created_at: "2026-05-21T13:00:00.000Z",
              notifications_received: 8,
              pattern: "notifications/**",
              realm: scope.realm,
              session_id: "session-1",
              subscription_id: 11,
            },
          ],
        },
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "operations" && domain === "rpc") {
      await route.fulfill({
        json: {
          operations: [{ operation: "GetStatus" }, { operation: "SetState" }],
        },
      });
      return;
    }

    if (segments.length === 12 && domain === "rpc" && segments[9] === "operations") {
      await route.fulfill({
        json: {
          workers: [
            {
              average_latency_ms: 12,
              realm: scope.realm,
              registered_at: "2026-05-21T13:00:00.000Z",
              requests_handled: 7,
              route: decodeURIComponent(segments[10] ?? "GetStatus"),
              session_id: "worker-1",
            },
          ],
        },
      });
      return;
    }

    await route.continue();
  });
}

function queueTimelineFixture(scope: ResourceScope) {
  return {
    area: scope.area,
    derived: false,
    domain: "queue",
    events: [
      {
        age_seconds: 2,
        area: scope.area,
        attempts: 1,
        correlation_id: "corr-queue",
        domain: "queue",
        kind: "transition",
        message_id: 200,
        observed_at: "2026-05-21T13:00:00.000Z",
        operation: "Peek",
        owner_session: "session-queue-1",
        realm: scope.realm,
        resource: scope.resource,
        summary: "Queue worker activity sample.",
        worker_session: "worker-queue-1",
      },
    ],
    family: 1,
    limit: 8,
    realm: scope.realm,
    resource: scope.resource,
  };
}

async function mockQueueResourceApis(page: Page, routeScope: ResourceScope) {
  const diagnostics = makeDiagnosticSnapshot();

  await mockAdminFeatures(page);

  await page.route("**/api/v1/**", async (route) => {
    const parsed = new URL(route.request().url());
    const segments = parsed.pathname.split("/").filter(Boolean);

    if (segments[2] === "features") {
      await route.fulfill({
        json: adminFeatures,
      });
      return;
    }

    if (segments.length < 9 || segments[2] !== "queue") {
      await route.continue();
      return;
    }

    const scope = parseResourceScope(segments);

    if (!scope) {
      await route.continue();
      return;
    }

    if (
      scope.area !== routeScope.area ||
      scope.realm !== routeScope.realm ||
      scope.resource !== routeScope.resource
    ) {
      await route.continue();
      return;
    }

    if (segments.length === 9) {
      await route.fulfill({
        json: {
          area: scope.area,
          backlog_age_buckets: {
            over_15m: 0,
            under_1m: 2,
            under_5m: 1,
            under_15m: 0,
          },
          delay_age_buckets: {
            over_15m: 0,
            under_1m: 0,
            under_5m: 0,
            under_15m: 0,
          },
          diagnostics,
          messages_dead_lettered: 0,
          messages_delayed: 1,
          messages_inflight: 2,
          messages_ready: 6,
          messages_total: 9,
          oldest_backlog_age_seconds: 28,
          oldest_message_age_seconds: 42,
          realm: scope.realm,
          resource: scope.resource,
        },
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "inflight") {
      await route.fulfill({
        json: {
          inflight: [
            {
              area: scope.area,
              attempts: 1,
              expires_at: "2026-05-21T13:05:00.000Z",
              family: 1,
              inflight_token: "token-1",
              message_id: 101,
              realm: scope.realm,
              resource: scope.resource,
              session_id: "session-queue-1",
            },
          ],
        },
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "dead-letters") {
      await route.fulfill({
        json: {
          messages: [
            {
              area: scope.area,
              attempts: 2,
              dead_lettered_at: "2026-05-21T12:59:00.000Z",
              family: 1,
              message_id: 88,
              realm: scope.realm,
              reason: "Transient failure",
              resource: scope.resource,
              session_id: "session-queue-2",
            },
          ],
        },
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "events") {
      await route.fulfill({
        json: queueTimelineFixture(scope),
      });
      return;
    }

    await route.continue();
  });
}

async function applyTheme(page: Page, theme: ThemeMode) {
  if (theme === "dark") {
    await page.addInitScript(() => {
      localStorage.setItem("fitz-admin-theme", "dark");
    });
    return;
  }

  await page.addInitScript(() => {
    localStorage.removeItem("fitz-admin-theme");
  });
}

async function expectNoHorizontalOverflow(page: Page) {
  const visibleOverflow = await page.evaluate(() => {
    const viewportRight = window.innerWidth + 2;
    const viewportLeft = -2;
    const isClippedByScrollableAncestor = (element: HTMLElement) => {
      let parent = element.parentElement;

      while (parent && parent !== document.body) {
        const style = window.getComputedStyle(parent);
        const clipsInlineOverflow = ["auto", "clip", "hidden", "scroll"].includes(style.overflowX);

        if (clipsInlineOverflow) {
          const rect = parent.getBoundingClientRect();
          if (rect.left >= viewportLeft && rect.right <= viewportRight) {
            return true;
          }
        }

        parent = parent.parentElement;
      }

      return false;
    };
    const offenders = Array.from(document.body.querySelectorAll<HTMLElement>("*")).filter(
      (element) => {
        // askrjs/askr-charts#2 tracks sr-only chart tables inflating document width.
        if (element.closest(".ak-chart-sr-only")) {
          return false;
        }

        const style = window.getComputedStyle(element);
        if (style.display === "none" || style.visibility === "hidden") {
          return false;
        }

        const rect = element.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) {
          return false;
        }

        if (isClippedByScrollableAncestor(element)) {
          return false;
        }

        return rect.right > viewportRight || rect.left < viewportLeft;
      },
    );

    return offenders.length === 0;
  });

  expect(visibleOverflow).toBe(true);
}

async function expectRouteChrome(page: Page, route: RouteScenario) {
  const viewport = page.viewportSize();
  const isMobile = (viewport?.width ?? 0) < 768;
  const primaryNav = page.getByRole("navigation", { name: "Primary navigation" });

  await expect(page.locator("main#main-content")).toHaveCount(1);
  await expect(page.getByRole("heading", { name: exactHeadingMatcher(route.title) })).toBeVisible();

  await expectNoHorizontalOverflow(page);

  if (route.shell === "app") {
    if (!isMobile) {
      await expect(primaryNav.getByRole("link", { name: "Fitz admin home" })).toBeVisible();
    }

    if (isMobile) {
      const menu = primaryNav.getByRole("button", { name: "Menu" });
      await expect(menu).toBeVisible();
      await menu.click();

      await expect(primaryNav.getByRole("link", { name: "Overview" })).toBeVisible();
      await expect(primaryNav.getByRole("link", { name: "Queue" })).toBeVisible();
      await expect(primaryNav.getByRole("link", { name: "Diagnostics" })).toBeVisible();
      await expect(primaryNav.getByRole("link", { name: "Settings" })).toBeVisible();
      await expect(primaryNav.getByRole("button", { name: "Toggle color theme" })).toBeVisible();
      await expect(primaryNav.getByRole("button", { name: "User menu" })).toBeVisible();
      await expectNoHorizontalOverflow(page);

      await page.keyboard.press("Escape");
      return;
    }

    await expect(primaryNav.getByRole("button", { name: "Toggle color theme" })).toBeVisible();

    await expect(primaryNav.getByRole("link", { name: "Overview" })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Stream" })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "KV" })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Queue" })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Diagnostics" })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Settings" })).toBeVisible();
    await expect(primaryNav.getByRole("button", { name: "User menu" })).toBeVisible();
    await expectNoHorizontalOverflow(page);
    return;
  }

  if (!isMobile) {
    await expect(primaryNav.getByRole("link", { name: "Fitz admin home" })).toBeVisible();
    await expect(primaryNav.getByRole("button", { name: "Toggle color theme" })).toBeVisible();
  }

  if (isMobile) {
    await expect(primaryNav.getByRole("button", { name: "Menu" })).toBeVisible();
  }
}

const sprint16Routes: RouteScenario[] = [
  {
    path: "/",
    shell: "app",
    setup: mockHomeRouteApis,
    title: "Broker status",
  },
  {
    path: "/admin",
    shell: "app",
    setup: mockHomeRouteApis,
    title: "Broker status",
  },
  {
    path: "/sessions",
    shell: "app",
    setup: (page) => mockSessionsApi(page, sessionsWithData),
    title: "Active sessions",
  },
  {
    path: "/admin/metrics",
    shell: "app",
    setup: mockMetricsApi,
    title: "Metrics explorer",
  },
  {
    path: "/diagnostics",
    shell: "app",
    setup: mockDiagnosticsApis,
    title: "Diagnostics",
  },
  {
    path: "/settings",
    shell: "app",
    setup: mockHomeRouteApis,
    title: "Settings",
  },
  {
    path: "/lease",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Lease overview",
  },
  {
    path: "/notice",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Notice overview",
  },
  {
    path: "/rpc",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "RPC overview",
  },
  {
    path: "/schedule",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Schedule overview",
  },
  {
    path: "/queue",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Queue overview",
  },
  {
    path: "/stream",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Stream overview",
  },
  {
    path: "/kv",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "KV overview",
  },
  {
    path: "/queue/default/ops/primary",
    shell: "app",
    setup: (page) =>
      mockQueueResourceApis(page, parseRouteResourceScope("/queue/default/ops/primary")),
    title: "Queue resource inspection",
  },
  {
    path: "/kv/default/ops/primary",
    shell: "app",
    setup: (page) =>
      mockResourceDetailApis(page, "kv", parseRouteResourceScope("/kv/default/ops/primary")),
    title: "KV resource inspection",
  },
  {
    path: "/lease/default/ops/primary",
    shell: "app",
    setup: (page) =>
      mockResourceDetailApis(page, "lease", parseRouteResourceScope("/lease/default/ops/primary")),
    title: "Lease resource inspection",
  },
  {
    path: "/notice/default/ops/primary",
    shell: "app",
    setup: (page) =>
      mockResourceDetailApis(
        page,
        "notice",
        parseRouteResourceScope("/notice/default/ops/primary"),
      ),
    title: "Notice resource inspection",
  },
  {
    path: "/rpc/default/ops/primary",
    shell: "app",
    setup: (page) =>
      mockResourceDetailApis(page, "rpc", parseRouteResourceScope("/rpc/default/ops/primary")),
    title: "RPC resource inspection",
  },
  {
    path: "/schedule/default/ops/primary",
    shell: "app",
    setup: (page) =>
      mockResourceDetailApis(
        page,
        "schedule",
        parseRouteResourceScope("/schedule/default/ops/primary"),
      ),
    title: "Schedule resource inspection",
  },
  {
    path: "/stream/default/ops/primary",
    shell: "app",
    setup: (page) =>
      mockResourceDetailApis(
        page,
        "stream",
        parseRouteResourceScope("/stream/default/ops/primary"),
      ),
    title: "Stream resource inspection",
  },
  {
    path: "/login",
    shell: "auth",
    setup: (page) => mockHomeRouteApis(page),
    title: "Sign in to Fitz Admin",
  },
  {
    path: "/logout",
    shell: "auth",
    setup: (page) => mockHomeRouteApis(page),
    title: /Signed out|Signing out/,
  },
];

test.describe("sprint 16 route matrix", () => {
  for (const route of sprint16Routes) {
    for (const viewport of viewportPresets) {
      for (const theme of themeModes) {
        test(`${route.path} [${viewport.key}] [${theme}]`, async ({ page }, testInfo) => {
          await page.setViewportSize({ width: viewport.width, height: viewport.height });
          await applyTheme(page, theme);
          await route.setup(page);
          await page.goto(route.path);

          await expectRouteChrome(page, route);

          if (theme === "dark") {
            await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
          }

          await page.screenshot({
            fullPage: true,
            path: testInfo.outputPath(`${normalizeRoute(route.path)}-${viewport.key}-${theme}.png`),
            animations: "disabled",
          });
        });
      }
    }
  }
});

test("captures the desktop dashboard shell", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await openDashboard(page);

  await expect(page.getByRole("heading", { name: "Broker status" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Domain signals" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("dashboard-desktop.png"),
    animations: "disabled",
  });
});

test("captures the tablet dashboard shell", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1024, height: 1200 });
  await openDashboard(page);

  await expect(page.getByRole("heading", { name: "Broker status" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Domain signals" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("dashboard-tablet.png"),
    animations: "disabled",
  });
});

test("captures the desktop dashboard shell in dark mode", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await openDashboard(page, "dark");

  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(page.getByRole("heading", { name: "Broker status" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("dashboard-dark.png"),
    animations: "disabled",
  });
});

test("captures the dashboard refreshing state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });

  let releaseRefresh: (() => void) | undefined;
  let topologyRequests = 0;

  await mockAdminFeatures(page);
  await page.route("**/api/v1/topology", async (route) => {
    topologyRequests += 1;

    if (topologyRequests > 1) {
      await new Promise<void>((resolve) => {
        releaseRefresh = resolve;
      });
    }

    await route.fulfill({
      json: topologyApiPayload,
    });
  });

  await openDashboard(page, "light", false);
  await expect(page.getByRole("heading", { name: "Domain signals" })).toBeVisible();

  await page.getByRole("button", { name: "Refresh topology" }).click();
  await expect(page.locator('[data-slot="badge"]').filter({ hasText: "Refreshing" })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("dashboard-refreshing.png"),
    animations: "disabled",
  });

  releaseRefresh?.();
  await page.waitForTimeout(100);
});

test("captures desktop domain navigation", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await openDashboard(page);
  const primaryNav = page.getByRole("navigation", { name: "Primary navigation" });

  await expect(primaryNav.getByRole("link", { name: "Stream" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "KV" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Schedule" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Queue" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Lease" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Notice" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "RPC" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("domains-navigation-desktop.png"),
    animations: "disabled",
  });

  await mockDomainOverviewApis(page);
  await primaryNav.getByRole("link", { name: "Queue" }).click();
  await expect(page).toHaveURL(/\/queue$/);
  await expect(page.locator("main#main-content")).toHaveCount(1);
});

test("captures a sidebar domain page", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page);
  await page.goto("/queue");

  await expect(page.locator("main#main-content")).toHaveCount(1);
  await expect(page.locator(".page-frame-sidebar")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Queue overview" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("queue-sidebar.png"),
    animations: "disabled",
  });
});

test("captures lease overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    lease: applyLeaseOverride({ realms: [] }),
  });

  await page.goto("/lease");
  await expect(page.getByRole("heading", { name: "Lease overview" })).toBeVisible();
  await expect(page.getByText("No lease realms are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("lease-empty-desktop.png"),
    animations: "disabled",
  });
});

test("captures kv overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    kv: {
      realms: [],
      stats: {
        commits_failed_total: 0,
        invalid_transaction_rejects_total: 0,
        keys_total: 0,
        operations_per_second: 0,
        rollbacks_total: 0,
        transactions_active: 0,
      },
    },
  });

  await page.goto("/kv");
  await expect(page.getByRole("heading", { name: "KV overview" })).toBeVisible();
  await expect(page.getByText("No KV realms are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("kv-empty-desktop.png"),
    animations: "disabled",
  });
});

test("captures notice overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    notice: {
      realms: [],
      stats: {
        delivery_drops_total: 0,
        publishes_per_second: 0,
        routes_active: 0,
        wildcard_limit_rejects_total: 0,
        subscriptions_active: 0,
        max_route_subscribers: 0,
      },
    },
  });

  await page.goto("/notice");
  await expect(page.getByRole("heading", { name: "Notice overview" })).toBeVisible();
  await expect(page.getByText("No notice realms are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("notice-empty-desktop.png"),
    animations: "disabled",
  });
});

test("captures stream overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    stream: {
      realms: [],
      stats: {
        events_total: 0,
        operations_per_second: 0,
        streams_active: 0,
        subscriptions_active: 0,
        watermark_lag_buckets: {
          caught_up: 0,
          over_100: 0,
          under_10: 0,
          under_100: 0,
        },
      },
    },
  });

  await page.goto("/stream");
  await expect(page.getByRole("heading", { name: "Stream overview" })).toBeVisible();
  await expect(page.getByText("No stream realms are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("stream-empty-desktop.png"),
    animations: "disabled",
  });
});

test("captures rpc overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    rpc: {
      realms: [],
      stats: {
        failure_total: 0,
        invalid_sequence_errors_dropped_total: 0,
        invalid_sequence_errors_forwarded_total: 0,
        invalid_sequence_responses_total: 0,
        operations_per_second: 0,
        pending_routes_active: 0,
        request_timeouts_total: 0,
        requests_pending: 0,
        responses_dropped_closed_caller_total: 0,
        responses_missing_pending_total: 0,
        workers_registered: 0,
      },
    },
  });

  await page.goto("/rpc");
  await expect(page.getByRole("heading", { name: "RPC overview" })).toBeVisible();
  await expect(page.getByText("No RPC realms are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("rpc-empty-desktop.png"),
    animations: "disabled",
  });
});

test("captures schedule overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    schedule: {
      realms: [],
      stats: {
        ack_failures_total: 0,
        cancel_persistence_failures_total: 0,
        create_persistence_failures_total: 0,
        executions_per_minute: 0,
        notify_failures_total: 0,
        overdue_normalizations_total: 0,
        pending_fire_claims: 0,
        schedules_active: 0,
        subscriptions_active: 0,
        upsert_persistence_failures_total: 0,
      },
    },
  });

  await page.goto("/schedule");
  await expect(page.getByRole("heading", { name: "Schedule overview" })).toBeVisible();
  await expect(page.getByText("No schedule realms are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("schedule-empty-desktop.png"),
    animations: "disabled",
  });
});

test.describe("captures domain overview templates", () => {
  for (const overviewPage of domainOverviewPages) {
    test(`captures ${overviewPage.domain} overview on desktop`, async ({ page }, testInfo) => {
      await page.setViewportSize({ width: 1440, height: 1200 });
      await mockDomainOverviewApis(page);
      await page.goto(overviewPage.path);

      await expect(page.getByRole("heading", { name: overviewPage.heading })).toBeVisible();
      await expect(page.locator("main#main-content")).toHaveCount(1);

      await page.screenshot({
        fullPage: true,
        path: testInfo.outputPath(`${overviewPage.domain}-desktop.png`),
        animations: "disabled",
      });
    });

    test(`captures ${overviewPage.domain} overview on mobile`, async ({ page }, testInfo) => {
      await page.setViewportSize({ width: 390, height: 844 });
      await mockDomainOverviewApis(page);
      await page.goto(overviewPage.path);

      await expect(page.getByRole("heading", { name: overviewPage.heading })).toBeVisible();
      await expect(page.locator("main#main-content")).toHaveCount(1);

      await page.screenshot({
        fullPage: true,
        path: testInfo.outputPath(`${overviewPage.domain}-mobile.png`),
        animations: "disabled",
      });
    });
  }
});

test("captures the mobile navbar panel", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await openDashboard(page);
  const primaryNav = page.getByRole("navigation", { name: "Primary navigation" });

  await primaryNav.getByRole("button", { name: "Menu" }).click();
  await expect(primaryNav.getByRole("link", { name: "Overview" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Queue" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Diagnostics" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Settings" })).toBeVisible();
  await expect(primaryNav.getByRole("button", { name: "Toggle color theme" })).toBeVisible();
  await expect(primaryNav.getByRole("button", { name: "User menu" })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("mobile-nav-open.png"),
    animations: "disabled",
  });
});

test("captures sessions data state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockSessionsApi(page, sessionsWithData);

  await page.goto("/sessions");
  await expect(page.getByRole("heading", { name: "Active sessions", exact: true })).toBeVisible();
  await expect(page.locator("table tbody tr").first()).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("sessions-desktop.png"),
    animations: "disabled",
  });
});

test("captures sessions empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockSessionsApi(page, sessionsEmpty);

  await page.goto("/sessions");
  await expect(page.getByRole("heading", { name: "Active sessions", exact: true })).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "No active sessions", exact: true }),
  ).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("sessions-empty-desktop.png"),
    animations: "disabled",
  });
});

test("captures sessions on mobile", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockSessionsApi(page, sessionsWithData);

  await page.goto("/sessions");
  await expect(page.getByRole("heading", { name: "Active sessions", exact: true })).toBeVisible();
  await expect(page.locator("ul.session-mobile-list li").first()).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("sessions-mobile.png"),
    animations: "disabled",
  });
});

test("captures metrics desktop", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockMetricsApi(page);
  await page.goto("/admin/metrics");

  await expect(page.getByRole("heading", { name: "Metrics explorer" })).toBeVisible();
  await expect(page.locator('input[aria-label="Filter metrics"]')).toBeVisible();
  await expect(page.getByRole("button", { name: "Refresh metrics" })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("metrics-desktop.png"),
    animations: "disabled",
  });
});

test("captures metrics filtered empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockMetricsApi(page);
  await page.goto("/admin/metrics");

  const filter = page.locator('input[aria-label="Filter metrics"]');
  await filter.fill("does-not-exist");
  await expect(page.getByText("No matching metrics")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("metrics-filtered-empty.png"),
    animations: "disabled",
  });
});

test("captures metrics on mobile", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockMetricsApi(page);
  await page.goto("/admin/metrics");

  await expect(page.getByRole("heading", { name: "Metrics explorer" })).toBeVisible();
  await expect(page.locator('input[aria-label="Filter metrics"]')).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("metrics-mobile.png"),
    animations: "disabled",
  });
});

test("captures metrics in dark mode", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockMetricsApi(page);
  await page.addInitScript(() => {
    localStorage.setItem("fitz-admin-theme", "dark");
  });

  await page.goto("/admin/metrics");

  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(page.getByRole("heading", { name: "Metrics explorer" })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("metrics-dark.png"),
    animations: "disabled",
  });
});
