export type MockResponse = {
  body: string;
  headers: Record<string, string>;
  status: number;
};

export const routeFamilies = ["1", "7", "42"];
export const realms = ["acme", "platform", "ops"];
export const areas = ["payments", "messaging", "control"];
export const resources = ["invoices", "orders", "worker-pool"];
export const operations = ["ReconcileInvoice", "RefreshProjection", "ExpireWindow"];
export const now = "2026-06-29T14:30:00.000Z";

export const domains = ["kv", "queue", "stream", "lease", "schedule", "notice", "rpc"] as const;
export type Domain = (typeof domains)[number];

export function domainUsesOperationSegment(domain: Domain) {
  return domain === "notice" || domain === "rpc" || domain === "schedule";
}

export function operationForIndex(index: number) {
  return operations[index % operations.length];
}

export function fitzRoute(
  domain: Domain,
  realm: string,
  area: string,
  resource: string,
  operation = operationForIndex(0),
) {
  const routeParts = [realm, area, resource];

  if (domainUsesOperationSegment(domain)) {
    routeParts.push(operation);
  }

  return `${domain}://${routeParts.join("/")}`;
}

export function json(body: unknown, status = 200): MockResponse {
  return {
    body: JSON.stringify(body),
    headers: { "content-type": "application/json" },
    status,
  };
}

export function text(body: string, status = 200): MockResponse {
  return {
    body,
    headers: { "content-type": "text/plain; version=0.0.4" },
    status,
  };
}

export function diagnostic(
  severity: "informational" | "low" | "medium" | "high" = "low",
  currentStage = "steady",
) {
  return {
    age_seconds: 96,
    confidence: severity === "high" ? 0.91 : 0.74,
    confidence_justification: {
      rationale: "Mock pressure combines backlog, live waiters, and recent failure counters.",
      signals_matched: ["backlog", "recent transitions", "active sessions"],
      signals_missing: ["storage errors"],
    },
    contention_count: severity === "high" ? 8 : 2,
    current_stage: currentStage,
    delta_1h: 18,
    delta_5m: 4,
    explanation_hints: [
      "Queue backlog is growing faster than consumers are draining.",
      "RPC has live pending requests and a small worker pool.",
    ],
    failure_count: severity === "high" ? 3 : 0,
    last_changed_at: now,
    last_failure_at: severity === "high" ? now : null,
    last_success_at: now,
    likely_bottleneck: severity === "high" ? "queue worker capacity" : "live demand",
    recent_transition_count: severity === "high" ? 12 : 3,
    severity,
    trend: severity === "high" ? "growing" : "steady",
    waiter_count: severity === "high" ? 11 : 2,
  };
}

export const incidentSummary = {
  confidence: 0.86,
  explanation:
    "Mock broker is healthy enough to browse, with deliberate queue and RPC pressure for UI tuning.",
  likely_bottleneck: "queue worker capacity",
  recommended_next_query: "queue acme/payments/invoices",
  severity: "medium",
  status: "degraded",
  suggested_next_queries: [
    {
      endpoint: "/api/v1/7/queue/realms/acme/areas/payments/resources/invoices",
      priority: 1,
      rationale: "Largest backlog and oldest work age are on invoices.",
      remediation: "Inspect queue workers and dead-letter rows before scaling producers.",
      title: "Inspect invoices queue",
    },
    {
      endpoint:
        "/api/v1/7/rpc/realms/platform/areas/control/resources/worker-pool/operations/ReconcileInvoice",
      priority: 2,
      rationale: "RPC pending requests exceed registered workers.",
      remediation: "Review worker registration and latency buckets.",
      title: "Inspect RPC worker pool",
    },
  ],
  title: "Mock delivery pressure",
};

export const diagnostics = {
  hotspots: [
    {
      ...diagnostic("high", "queue_backlog"),
      area: "payments",
      backlog: 37,
      dead_letters: 2,
      delayed: 9,
      domain: "queue",
      family: 7,
      inflight: 11,
      ready: 28,
      realm: "acme",
      resource: "invoices",
    },
    {
      ...diagnostic("medium", "rpc_pending"),
      area: "control",
      domain: "rpc",
      family: 7,
      operation: "ReconcileInvoice",
      realm: "platform",
      resource: "worker-pool",
      workers: 3,
    },
  ],
  incident_summary: incidentSummary,
  last_significant_transition_at: now,
  top_bottleneck: {
    ...diagnostic("high", "queue_backlog"),
    area: "payments",
    backlog: 37,
    domain: "queue",
    family: 7,
    realm: "acme",
    resource: "invoices",
  },
};

export const ageBuckets = { over_15m: 2, under_15m: 5, under_1m: 7, under_5m: 23 };
export const latencyBuckets = {
  over_5s: 0,
  under_100ms: 5,
  under_10ms: 9,
  under_1ms: 3,
  under_1s: 1,
  under_500ms: 2,
  under_50ms: 12,
  under_5ms: 6,
  under_5s: 0,
};

export const domainStats = {
  kv: {
    commits_failed_total: 1,
    diagnostics: diagnostic("low", "commit_latency"),
    invalid_transaction_rejects_total: 2,
    keys_total: 12842,
    operations_per_second: 42.8,
    transactions_active: 6,
  },
  lease: {
    acquire_timeouts_total: 4,
    diagnostics: diagnostic("medium", "lease_waiters"),
    failure_total: 4,
    forced_releases_total: 1,
    invalid_token_rejects_total: 2,
    leases_active: 18,
    oldest_lease_age_seconds: 640,
    operations_per_second: 8.4,
    ownership_churn_total: 91,
    requests_total: 1830,
    success_total: 1789,
    waiter_depth: 11,
  },
  notice: {
    delivery_drops_total: 3,
    diagnostics: diagnostic("medium", "fanout_pressure"),
    failure_total: 3,
    max_route_subscribers: 87,
    publishes_per_second: 18.5,
    requests_total: 9012,
    routes_active: 16,
    subscriptions_active: 244,
    success_total: 8974,
    unsubscribes_total: 61,
    wildcard_limit_rejects_total: 1,
  },
  queue: {
    backlog_age_buckets: ageBuckets,
    complete_rejected_total: 2,
    completes_total: 4155,
    dead_letter_transitions_total: 2,
    delay_age_buckets: ageBuckets,
    diagnostics: diagnostic("high", "queue_backlog"),
    enqueues_total: 4820,
    extends_total: 83,
    failure_total: 5,
    inflight_active: 11,
    messages_dead_lettered: 2,
    messages_delayed: 9,
    messages_pending: 37,
    messages_ready: 28,
    notify_drops_total: 2,
    oldest_backlog_age_seconds: 1260,
    oldest_message_age_seconds: 1410,
    operations_per_second: 34.2,
    redeliveries_total: 6,
    releases_total: 44,
    requests_total: 9270,
    reserves_total: 4102,
    success_total: 9211,
  },
  rpc: {
    acks_rejected_wrong_worker_total: 1,
    backpressure_rejects_total: 4,
    diagnostics: diagnostic("medium", "rpc_pending"),
    duplicate_correlation_rejects_total: 2,
    failure_total: 7,
    invalid_sequence_errors_dropped_total: 0,
    invalid_sequence_errors_forwarded_total: 1,
    invalid_sequence_responses_total: 1,
    oldest_pending_request_age_seconds: 18,
    operations_per_second: 21.4,
    pending_routes_active: 5,
    request_timeouts_total: 3,
    requests_pending: 14,
    requests_total: 7420,
    responses_dropped_closed_caller_total: 2,
    responses_missing_pending_total: 1,
    slowest_worker_average_latency_ms: 184.2,
    success_total: 7360,
    worker_latency_buckets: { over_100ms: 1, under_100ms: 3, under_25ms: 7, under_5ms: 12 },
    workers_registered: 9,
    wrong_worker_rejects_total: 1,
  },
  schedule: {
    ack_failures_total: 2,
    cancel_persistence_failures_total: 0,
    create_persistence_failures_total: 1,
    diagnostics: diagnostic("low", "pending_fire_claims"),
    executions_per_minute: 14.6,
    notify_failures_total: 1,
    oldest_pending_claim_age_seconds: 75,
    overdue_normalizations_total: 3,
    pending_ack_retries: 4,
    pending_claim_cleanup_failures_total: 0,
    pending_claims_expired_total: 2,
    pending_fire_claims: 6,
    request_latency_buckets: latencyBuckets,
    schedules_active: 19,
    subscriptions_active: 7,
    upsert_persistence_failures_total: 1,
  },
  stream: {
    append_conflicts_total: 2,
    append_sessions_active: 5,
    append_sessions_ended_total: 323,
    append_sessions_started_total: 328,
    diagnostics: diagnostic("low", "append_activity"),
    events_total: 92348,
    failure_total: 2,
    notify_drops_total: 1,
    operations_per_second: 29.7,
    request_latency_buckets: latencyBuckets,
    requests_total: 11240,
    streams_active: 24,
    subscriptions_active: 18,
    success_total: 11210,
    watermark_lag_buckets: { caught_up: 12, over_100: 1, under_10: 6, under_100: 3 },
  },
};

export const broker = {
  connections: 31,
  messages_per_second: 86.7,
  realms,
  router_backpressure_total: 3,
  router_high_lane_backpressure_total: 1,
  sessions: 24,
  uptime_seconds: 172840,
};

export const globalStats = {
  broker,
  diagnostics,
  domains: domainStats,
};

export const sessions = {
  sessions: [
    {
      connected_at: now,
      identity_claim: "org_id",
      identity_value: "acme",
      idle_seconds: 4,
      messages_received: 1842,
      messages_sent: 2391,
      remote_addr: "127.0.0.1:58120",
      route_family: 7,
      session_id: "sess-admin-acme",
      subject: "admin@acme.test",
      transport: "ws",
    },
    {
      connected_at: now,
      identity_claim: "org_id",
      identity_value: "platform",
      idle_seconds: 37,
      messages_received: 904,
      messages_sent: 1310,
      remote_addr: "10.0.9.44:49122",
      route_family: 42,
      session_id: "sess-worker-platform",
      subject: "worker:reconcile",
      transport: "tcp",
    },
  ],
};

export function resourceEntry(resource: string, index: number, domain?: Domain) {
  const entry = {
    area: areas[index % areas.length],
    complete_success_total: 420 + index * 11,
    enqueue_success_total: 510 + index * 13,
    estimate_complete: true,
    estimated_record_count: 300 + index * 37,
    estimated_storage_bytes: 1024 * (16 + index * 9),
    family_count: 3,
    in_rate_per_second: 4.2 + index,
    messages_dead_lettered: index % 2,
    messages_delayed: 2 + index,
    messages_inflight: 3 + index,
    messages_ready: 8 + index * 3,
    messages_total: 14 + index * 6,
    oldest_backlog_age_seconds: 90 + index * 120,
    out_rate_per_second: 3.8 + index,
    read_latency_avg_ms: 2.4 + index,
    read_latency_p95_ms: 12.5 + index * 2,
    realm: realms[index % realms.length],
    resource,
    status: index === 0 ? "falling_behind" : "draining",
    subscriptions_active: 5 + index,
    transactions_active: 1 + index,
    write_latency_avg_ms: 4.1 + index,
    write_latency_p95_ms: 18.3 + index * 2,
  };

  return domain && domainUsesOperationSegment(domain)
    ? { ...entry, operation: operationForIndex(index) }
    : entry;
}

export function resourceEntries(domain?: Domain) {
  return resources.map((resource, index) => resourceEntry(resource, index, domain));
}

export function topologyLane(domain: Domain, index: number) {
  const state = domain === "queue" ? "pressure" : domain === "rpc" ? "pressure" : "flowing";
  return {
    activity_per_second: 8 + index * 3.2,
    consumers: 2 + index,
    counters: [
      { key: "requests", label: "Requests", value: 1000 + index * 250 },
      { key: "failures", label: "Failures", value: domain === "queue" || domain === "rpc" ? 3 : 0 },
    ],
    diagnostics: diagnostic(state === "pressure" ? "medium" : "low", `${domain}_activity`),
    id: domain,
    observers: 4 + index,
    state,
    title: domain.toUpperCase(),
    top_scoped_resources: resourceEntries()
      .slice(0, 2)
      .map((entry, resourceIndex) => {
        const operation = operationForIndex(resourceIndex);
        const route = fitzRoute(domain, entry.realm, entry.area, entry.resource, operation);

        return {
          counters: [
            { key: "pressure", label: "Pressure", value: resourceIndex === 0 ? 87 : 42 },
            { key: "activity", label: "Activity/sec", value: 12 + resourceIndex },
          ],
          id: domainUsesOperationSegment(domain)
            ? `${domain}:${entry.realm}:${entry.area}:${entry.resource}:${operation}`
            : `${domain}:${entry.realm}:${entry.area}:${entry.resource}`,
          label: route.replace(`${domain}://`, ""),
          scope: {
            area: entry.area,
            ...(domainUsesOperationSegment(domain) ? { operation } : {}),
            realm: entry.realm,
            resource: entry.resource,
            route,
            route_family: index === 0 ? 7 : 42,
          },
          state: resourceIndex === 0 && state === "pressure" ? "pressure" : "flowing",
        };
      }),
  };
}

export const topology = {
  broker,
  connections: {
    items: [
      {
        id: "conn-queue-invoices",
        kind: "queue_inflight_consumer",
        label: "Invoices queue consumers",
        metrics: [
          { key: "inflight", label: "Inflight", value: 11 },
          { key: "ready", label: "Ready", value: 28 },
        ],
        scope: {
          area: "payments",
          realm: "acme",
          resource: "invoices",
          route: "queue://acme/payments/invoices",
          route_family: 7,
        },
        source: "domain:queue",
        state: "pressure",
        target: "session:sess-worker-platform",
      },
      {
        id: "conn-rpc-reconcile",
        kind: "rpc_worker",
        label: "ReconcileInvoice workers",
        metrics: [
          { key: "workers", label: "Workers", value: 3 },
          { key: "pending", label: "Pending", value: 14 },
        ],
        scope: {
          area: "control",
          operation: "ReconcileInvoice",
          realm: "platform",
          resource: "worker-pool",
          route: "rpc://platform/control/worker-pool/ReconcileInvoice",
          route_family: 7,
        },
        source: "domain:rpc",
        state: "pressure",
        target: "session:sess-worker-platform",
      },
    ],
    limit: 20,
    total: 2,
    truncated: false,
  },
  diagnostics,
  generated_at: now,
  lanes: domains.map(topologyLane),
  session_groups: [
    {
      max_idle_seconds: 37,
      messages_received: 2746,
      messages_sent: 3701,
      representative_sessions: sessions.sessions,
      route_family: 7,
      sessions: 12,
      transports: ["ws", "tcp"],
    },
    {
      max_idle_seconds: 81,
      messages_received: 1400,
      messages_sent: 1900,
      representative_sessions: sessions.sessions.slice(1),
      route_family: 42,
      sessions: 7,
      transports: ["ws"],
    },
  ],
};

export const metricsText = `# HELP fitz_uptime_seconds Broker uptime in seconds
# TYPE fitz_uptime_seconds gauge
fitz_uptime_seconds 172840

# HELP fitz_queue_messages_pending Pending queue messages
# TYPE fitz_queue_messages_pending gauge
fitz_queue_messages_pending 37

# HELP fitz_rpc_requests_pending Pending RPC requests
# TYPE fitz_rpc_requests_pending gauge
fitz_rpc_requests_pending 14

# HELP fitz_notice_delivery_drops_total Notice delivery drops
# TYPE fitz_notice_delivery_drops_total counter
fitz_notice_delivery_drops_total 3

# HELP fitz_schedule_latency_ms Schedule request latency
# TYPE fitz_schedule_latency_ms histogram
fitz_schedule_latency_ms{le="1ms"} 3
fitz_schedule_latency_ms{le="5ms"} 9
fitz_schedule_latency_ms{le="10ms"} 18
fitz_schedule_latency_ms{le="50ms"} 30
fitz_schedule_latency_ms{le="100ms"} 35
fitz_schedule_latency_ms{le="500ms"} 37
fitz_schedule_latency_ms{le="1s"} 38
fitz_schedule_latency_ms{le="5s"} 38
fitz_schedule_latency_ms{le="+Inf"} 38
fitz_schedule_latency_ms_count 38
`;

export function routeFamilyFrom(value: string) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 7;
}

export function areaCollection(realm: string) {
  return { areas: areas.map((area) => ({ area })), realm };
}

export function resourceCollection(realm: string, area: string, domain?: Domain) {
  return {
    area,
    realm,
    resources: resources.map((resource, index) => ({
      ...resourceEntry(resource, index, domain),
      area,
      realm,
      resource,
    })),
  };
}

export function queueRealm(realm: string) {
  const queues = resourceEntries().map((entry) => ({ ...entry, realm }));
  return {
    ...domainStats.queue,
    area_count: areas.length,
    areas: areas.map((area, index) => ({
      ...resourceEntry(resources[index % resources.length], index),
      area,
      queue_count: resources.length,
      realm,
    })),
    queue_count: queues.length,
    queues,
    realm,
    status: "falling_behind",
    subscriptions_active: 14,
  };
}

export function queueArea(realm: string, area: string) {
  return {
    ...resourceEntry("invoices", 0),
    area,
    queue_count: resources.length,
    queues: resourceCollection(realm, area).resources,
    realm,
    status: "falling_behind",
  };
}

export function resourceDetail(
  domain: Domain,
  family: number,
  realm: string,
  area: string,
  resource: string,
) {
  if (domain === "kv") {
    return {
      area,
      diagnostics: diagnostic("low", "kv_current_state"),
      estimate_complete: true,
      estimated_record_count: 728,
      estimated_storage_bytes: 163840,
      read_latency_avg_ms: 2.8,
      read_latency_p95_ms: 11.4,
      realm,
      resource,
      route_family: family,
      transactions_active: 3,
      write_latency_avg_ms: 4.6,
      write_latency_p95_ms: 19.2,
    };
  }

  if (domain === "queue") {
    return {
      ...resourceEntry(resource, 0),
      area,
      realm,
      resource,
      route_family: family,
    };
  }

  if (domain === "stream") {
    return {
      area,
      diagnostics: diagnostic("low", "stream_append"),
      offset: 92348,
      realm,
      resource,
      sessions_active: 5,
      size_bytes: 8421376,
      watermark: 92341,
    };
  }

  if (domain === "lease") {
    return {
      active_leases: 2,
      area,
      diagnostics: diagnostic("medium", "lease_waiters"),
      oldest_lease_age_seconds: 640,
      realm,
      resource,
    };
  }

  if (domain === "schedule") {
    return {
      area,
      cron: "*/5 * * * *",
      diagnostics: diagnostic("low", "next_fire"),
      enabled: true,
      executions_total: 184,
      next_run: "2026-06-29T14:35:00.000Z",
      realm,
      resource,
    };
  }

  if (domain === "notice") {
    return {
      area,
      diagnostics: diagnostic("medium", "fanout_pressure"),
      realm,
      resource,
      subscriptions_active: 87,
    };
  }

  return {
    area,
    operations: [{ operation: "ReconcileInvoice" }, { operation: "RefreshProjection" }],
    realm,
    resource,
  };
}

export function timeline(domain: Domain, family: number, realm: string, area: string, resource: string) {
  return {
    area,
    derived: false,
    diagnostics: diagnostic("low", `${domain}_timeline`),
    domain,
    events: [
      {
        age_seconds: 18,
        area,
        attempts: domain === "queue" ? 2 : null,
        correlation_id: domain === "rpc" ? "corr-mock-001" : null,
        domain,
        family,
        kind: domain === "lease" ? "ownership_change" : "transition",
        message_id: domain === "queue" ? 8912 : null,
        observed_at: now,
        operation: domain === "rpc" ? "ReconcileInvoice" : null,
        owner_session: domain === "lease" ? "sess-worker-platform" : null,
        realm,
        resource,
        summary: `Mock ${domain} transition for ${realm}/${area}/${resource}`,
        worker_session: domain === "rpc" ? "sess-worker-platform" : null,
      },
    ],
    family,
    limit: 20,
    realm,
    resource,
  };
}

export function comparison(domain: Domain, family: number, realm: string, area: string, resource: string) {
  return {
    comparison_mode: "resource",
    delta: {
      age_seconds: 42,
      backlog: domain === "queue" ? 17 : null,
      contention_count: 2,
      dead_letters: domain === "queue" ? 2 : null,
      delayed: domain === "queue" ? 5 : null,
      failure_count: domain === "rpc" ? 3 : 0,
      inflight: domain === "queue" ? 6 : null,
      operations_total: 128,
      ready: domain === "queue" ? 14 : null,
      recent_transition_count: 4,
      subscriptions: domain === "notice" ? 87 : null,
      waiters: domain === "lease" ? 11 : null,
      workers: domain === "rpc" ? 3 : null,
    },
    derived: false,
    domain,
    left: {
      diagnostics: diagnostic("medium", `${domain}_left`),
      metrics: { backlog: domain === "queue" ? 37 : null, workers: domain === "rpc" ? 3 : null },
      scope: { area, family, realm, resource },
    },
    right: {
      diagnostics: diagnostic("low", `${domain}_right`),
      metrics: { backlog: domain === "queue" ? 20 : null, workers: domain === "rpc" ? 5 : null },
      scope: { area, family, realm, resource: "orders" },
    },
    summary: "Mock comparison shows pressure concentrated on the selected resource.",
  };
}

export function kvByte(value: string) {
  return { base64: btoa(value), len_bytes: value.length, utf8: value };
}

