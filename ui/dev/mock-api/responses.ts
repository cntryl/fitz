import {
  type Domain,
  type MockResponse,
  areaCollection,
  areas,
  broker,
  comparison,
  diagnostic,
  diagnostics,
  domainStats,
  domains,
  empty,
  fitzRoute,
  globalStats,
  json,
  kvByte,
  familyMetrics,
  mockAdminCredentials,
  now,
  operationForIndex,
  queueArea,
  queueRealm,
  realms,
  resourceCollection,
  resourceDetail,
  resources,
  routeFamilies,
  routeFamilyFrom,
  sessions,
  structuredMetrics,
  text,
  timeline,
  topology,
} from "./fixtures";
import { applyFamilyScenario } from "./scenarios";

export function domainResponse(
  familyValue: string,
  domain: Domain,
  rest: string[],
): MockResponse | null {
  const family = routeFamilyFrom(familyValue);

  if (rest[0] === "stats") return json(domainStats[domain]);
  if (rest[0] === "deliveries") return json(noticeDeliveries(family));
  if (rest[0] === "calls") return json(rpcCalls(family));
  if (rest[0] === "pending") return json({ requests: rpcPending(family) });
  if (rest[0] === "search") return json(leaseSearch(family));
  if (rest[0] === "missed") return json(scheduleMissed(family));

  if (rest[0] !== "realms") return null;
  if (rest.length === 1) return json({ realms: realms.map((realm) => ({ realm })) });

  const realm = rest[1] ?? realms[0];
  if (rest.length === 2) return domain === "queue" ? json(queueRealm(realm)) : json({ realm });
  if (domain === "stream" && rest[2] === "watermarks") {
    return json({
      area_count: areas.length,
      family_watermarks: routeFamilies.map((family) => ({
        family: routeFamilyFrom(family),
        watermark: 92000 + routeFamilyFrom(family),
      })),
      realm,
      resource_count: resources.length,
    });
  }
  if (rest[2] !== "areas") return null;
  if (rest.length === 3) return json(areaCollection(realm));

  const area = rest[3] ?? areas[0];
  if (rest.length === 4)
    return domain === "queue" ? json(queueArea(realm, area)) : json({ area, realm });
  if (domain === "stream" && rest[4] === "watermarks") {
    return json({
      area,
      family_watermarks: routeFamilies.map((family) => ({
        family: routeFamilyFrom(family),
        watermark: 92000 + routeFamilyFrom(family),
      })),
      realm,
      resource_count: resources.length,
    });
  }
  if (rest[4] !== "resources") return null;
  if (rest.length === 5) return json(resourceCollection(realm, area, domain));

  const resource = rest[5] ?? resources[0];
  const tail = rest.slice(6);
  if (tail.length === 0) return json(resourceDetail(domain, family, realm, area, resource));
  if (tail[0] === "compare") return json(comparison(domain, family, realm, area, resource));
  if (tail[0] === "events") return json(timeline(domain, family, realm, area, resource));
  if (tail[0] === "rows") {
    return json({
      area,
      has_more: false,
      items: [
        { key: kvByte("invoice:1001"), value: kvByte('{"status":"queued","amount":12800}') },
        { key: kvByte("invoice:1002"), value: kvByte('{"status":"paid","amount":4200}') },
      ],
      limit: 50,
      next_cursor: null,
      realm,
      resource,
      route_family: family,
      starts_with: kvByte(""),
    });
  }
  if (tail[0] === "value") {
    return json({
      area,
      found: true,
      key: kvByte("invoice:1001"),
      realm,
      resource,
      route_family: family,
      value: kvByte('{"status":"queued","amount":12800}'),
    });
  }
  if (tail[0] === "prefix") {
    return json({
      area,
      has_more: false,
      items: [{ key: kvByte("invoice:1001"), value: kvByte('{"status":"queued"}') }],
      limit: 50,
      prefix: kvByte("invoice:"),
      realm,
      resource,
      route_family: family,
    });
  }
  if (tail[0] === "transactions")
    return json({ transactions: kvTransactions(realm, area, resource) });
  if (tail[0] === "records") return json(streamRecords(family, realm, area, resource));
  if (tail[0] === "subscriptions") return json(noticeSubscriptions(family, realm, area, resource));
  if (tail[0] === "operations" && tail.length === 1) {
    return json({
      area,
      operations: [{ operation: "ReconcileInvoice" }, { operation: "RefreshProjection" }],
      realm,
      resource,
    });
  }
  if (tail[0] === "operations" && tail[2] === "workers")
    return json({ workers: rpcWorkers(family, realm, area, resource, tail[1]) });
  if (tail[0] === "operations" && tail[1])
    return json(rpcOperation(family, realm, area, resource, tail[1]));
  if (tail[0] === "workers")
    return json({ workers: rpcWorkers(family, realm, area, resource, "ReconcileInvoice") });
  if (tail[0] === "dead-letters")
    return json({ messages: queueDeadLetters(family, realm, area, resource) });
  if (tail[0] === "inflight")
    return json({ inflight: queueInflight(family, realm, area, resource) });
  if (tail[0] === "executions") return json(scheduleExecutions(family, realm, area, resource));

  return null;
}

export function streamRecords(family: number, realm: string, area: string, resource: string) {
  return {
    area,
    from_offset: 0,
    has_more: false,
    limit: 50,
    realm,
    records: [
      {
        area,
        area_offset: 0,
        body: kvByte('{"event":"invoice.created","amount":12800}'),
        created_at_ms: 1782743400000,
        metadata: kvByte('{"source":"mock"}'),
        realm,
        realm_offset: 0,
        resource,
        resource_offset: 0,
        route_family: family,
      },
      {
        area,
        area_offset: 1,
        body: kvByte('{"event":"invoice.paid","amount":4200}'),
        created_at_ms: 1782743460000,
        metadata: null,
        realm,
        realm_offset: 1,
        resource,
        resource_offset: 1,
        route_family: family,
      },
    ],
    resource,
    route_family: family,
  };
}

export function noticeDeliveries(family: number) {
  return {
    limit: 50,
    observations: resources.map((resource, index) => ({
      area: areas[index % areas.length],
      notifications_received: 180 + index * 21,
      publishes_per_minute: 16 + index * 4,
      publishes_total: 4900 + index * 340,
      realm: realms[index % realms.length],
      resource,
      route: fitzRoute(
        "notice",
        realms[index % realms.length],
        areas[index % areas.length],
        resource,
        operationForIndex(index),
      ),
      route_family: family,
      session_id: `sess-notice-${index}`,
      status: index === 0 ? "hot" : "active",
      subscription_id: 100 + index,
    })),
    route_family: family,
  };
}

export function noticeSubscriptions(family: number, realm: string, area: string, resource: string) {
  return {
    subscriptions: [
      {
        created_at: now,
        notifications_received: 248,
        pattern: fitzRoute("notice", realm, area, resource),
        realm,
        route_family: family,
        session_id: "sess-admin-acme",
        subscription_id: 101,
      },
    ],
  };
}

export function rpcCalls(family: number) {
  return {
    limit: 50,
    observations: [
      ...rpcPending(family).map((request) => ({
        age_seconds: request.age_seconds,
        area: "control",
        average_latency_ms: null,
        correlation_id: request.correlation_id,
        operation: "ReconcileInvoice",
        realm: "platform",
        registered_at: null,
        requests_handled: null,
        resource: "worker-pool",
        route: request.route,
        route_family: family,
        state: "pending",
        submitted_at: request.submitted_at,
        worker_session_id: request.worker_session_id,
      })),
      ...rpcWorkers(family, "platform", "control", "worker-pool", "ReconcileInvoice").map(
        (worker) => ({
          age_seconds: null,
          area: "control",
          average_latency_ms: worker.average_latency_ms,
          correlation_id: null,
          operation: "ReconcileInvoice",
          realm: "platform",
          registered_at: worker.registered_at,
          requests_handled: worker.requests_handled,
          resource: "worker-pool",
          route: worker.route,
          route_family: family,
          state: "worker_registered",
          submitted_at: null,
          worker_session_id: worker.session_id,
        }),
      ),
    ],
    route_family: family,
  };
}

export function rpcPending(family: number) {
  return [
    {
      age_seconds: 18,
      correlation_id: "corr-mock-001",
      route: "rpc://platform/control/worker-pool/ReconcileInvoice",
      route_family: family,
      submitted_at: now,
      worker_session_id: null,
    },
  ];
}

export function rpcWorkers(
  family: number,
  realm: string,
  area: string,
  resource: string,
  operation: string,
) {
  return [
    {
      average_latency_ms: 84.2,
      realm,
      registered_at: now,
      requests_handled: 1480,
      route: `rpc://${realm}/${area}/${resource}/${operation}`,
      route_family: family,
      session_id: "sess-worker-platform",
    },
  ];
}

export function rpcOperation(
  family: number,
  realm: string,
  area: string,
  resource: string,
  operation: string,
) {
  return {
    area,
    diagnostics: diagnostic("medium", "rpc_operation"),
    operation,
    realm,
    requests_pending: 4,
    resource,
    slowest_worker_average_latency_ms: 184.2,
    worker_latency_buckets: { over_100ms: 1, under_100ms: 3, under_25ms: 7, under_5ms: 12 },
    workers_registered: family === 1 ? 2 : 3,
  };
}

export function queueDeadLetters(family: number, realm: string, area: string, resource: string) {
  return [
    {
      area,
      attempts: 5,
      dead_lettered_at: now,
      family,
      message_id: 8912,
      realm,
      reason: "mock worker timeout after retries",
      resource,
    },
  ];
}

export function queueInflight(family: number, realm: string, area: string, resource: string) {
  return [
    {
      area,
      attempts: 2,
      expires_at: "2026-06-29T14:31:00.000Z",
      family,
      inflight_token: "inflight-mock-001",
      message_id: 8841,
      realm,
      resource,
      session_id: "sess-worker-platform",
    },
  ];
}

export function kvTransactions(realm: string, area: string, resource: string) {
  return [
    {
      area,
      idle_seconds: 7,
      mode: "read_write",
      operations_count: 4,
      realm,
      resource,
      started_at: now,
      tx_id: 42,
    },
  ];
}

export function leaseSearch(family: number) {
  return {
    items: [
      {
        acquired_at: now,
        area: "control",
        expires_at: "2026-06-29T14:32:00.000Z",
        owner_id: "owner-mock-1",
        owner_session_id: "sess-worker-platform",
        pending_waiters: 6,
        queued_token: 77,
        realm: "platform",
        renewals: 12,
        resource: "worker-pool",
        route_family: family,
        state: "owned",
      },
    ],
    limit: 50,
    route_family: family,
  };
}

export function scheduleMissed(family: number) {
  return {
    limit: 50,
    observations: [
      {
        age_seconds: 75,
        area: "payments",
        claimed_at: now,
        fire_at: "2026-06-29T14:28:45.000Z",
        fire_ms: 1782743325000,
        operation: "ReconcileInvoice",
        realm: "acme",
        resource: "invoices",
        route_family: family,
        status: "pending_ack_retry",
      },
    ],
    route_family: family,
  };
}

export function scheduleExecutions(family: number, realm: string, area: string, resource: string) {
  return {
    area,
    limit: 50,
    observations: [
      {
        area,
        cron: "*/5 * * * *",
        executions_total: 184,
        last_run: "2026-06-29T14:25:00.000Z",
        next_run: "2026-06-29T14:35:00.000Z",
        operation: "ReconcileInvoice",
        realm,
        resource,
        route_family: family,
        status: "scheduled",
      },
    ],
    realm,
    resource,
    route_family: family,
  };
}

export function search(url: URL) {
  const query = url.searchParams.get("q") ?? "mock";
  return {
    domain: url.searchParams.get("domain"),
    limit: Number(url.searchParams.get("limit") ?? 50),
    query,
    results: [
      {
        area: "payments",
        domain: "queue",
        health: "pressure",
        href: "/admin/queue/acme/payments/invoices",
        id: "queue:acme:payments:invoices",
        kind: "resource",
        matched_fields: ["resource", "diagnostics"],
        metadata: { backlog: "37", inflight: "11" },
        realm: "acme",
        resource: "invoices",
        route_family: "7",
        summary: "Invoices queue has visible backlog and dead-letter rows.",
        title: "acme / payments / invoices",
      },
      {
        area: "control",
        domain: "rpc",
        health: "pressure",
        href: "/admin/rpc/platform/control/worker-pool/ReconcileInvoice",
        id: "rpc:platform:control:worker-pool:ReconcileInvoice",
        kind: "operation",
        matched_fields: ["operation", "worker"],
        metadata: { pending: "14", workers: "3" },
        operation: "ReconcileInvoice",
        realm: "platform",
        resource: "worker-pool",
        route_family: "7",
        summary: "RPC worker pool has pending requests waiting for live workers.",
        title: "ReconcileInvoice",
      },
    ],
    route_family: url.searchParams.get("route_family"),
    total: 2,
    truncated: false,
  };
}

export function apiResponse(method: string, url: URL, requestBody?: unknown): MockResponse | null {
  if (method === "POST" && url.pathname === "/api/v1/runtime/drain") {
    return json({
      active_sessions: broker.sessions,
      close_reason: "mock drain",
      drain_deadline_epoch_ms: null,
      drain_grace_seconds: 30,
      drain_started_epoch_ms: Date.now(),
      lifecycle_state: "draining",
    });
  }

  if (method === "POST" && url.pathname === "/api/v1/session") {
    const credentials = requestBody as { password?: unknown; username?: unknown } | undefined;
    const authenticated =
      credentials?.username === mockAdminCredentials.username &&
      credentials.password === mockAdminCredentials.password;

    return authenticated ? empty() : json({ error: "Invalid username or password" }, 401);
  }
  if (method === "DELETE" && url.pathname === "/api/v1/session") return empty();
  if (url.pathname === "/api/v1/features") {
    return json({
      admin_auth_mode: "protected",
      admin_auth_required: true,
      route_families: routeFamilies,
      route_families_wildcard: false,
    });
  }
  if (url.pathname === "/api/v1/session") {
    return json({
      authenticated: true,
      route_families: routeFamilies,
      route_families_wildcard: false,
      username: mockAdminCredentials.username,
    });
  }
  const familyScopedRoute = url.pathname.match(
    /^\/api\/v1\/(\d+)\/(sessions|stats|topology|troubleshooting)$/,
  );
  if (familyScopedRoute) {
    switch (familyScopedRoute[2]) {
      case "sessions":
        return json(sessions);
      case "stats":
        return json(globalStats);
      case "topology":
        return json(topology);
      case "troubleshooting":
        return json(diagnostics);
      default:
        return null;
    }
  }
  if (url.pathname === "/api/v1/sessions") return json(sessions);
  if (url.pathname === "/api/v1/stats") return json(globalStats);
  if (url.pathname === "/api/v1/topology") return json(topology);
  if (url.pathname === "/api/v1/troubleshooting") return json(diagnostics);
  if (url.pathname === "/api/v1/search") return json(search(url));

  const parts = url.pathname.split("/").filter(Boolean).map(decodeURIComponent);
  const domain = parts[3] as Domain | undefined;

  if (parts[0] === "api" && parts[1] === "v1" && domain && domains.includes(domain)) {
    return domainResponse(parts[2] ?? "7", domain, parts.slice(4));
  }

  return null;
}

export function mockFitzResponse(
  method: string | undefined,
  requestUrl: string | undefined,
  requestBody?: unknown,
) {
  const requestMethod = method ?? "GET";
  const url = new URL(requestUrl ?? "/", "http://fitz.mock");

  if (requestMethod === "OPTIONS") return text("", 204);
  if (url.pathname === "/api/v1/all/metrics") return json(structuredMetrics);
  const familyMetricsMatch = url.pathname.match(/^\/api\/v1\/(\d+)\/metrics$/);
  if (familyMetricsMatch) {
    const family = routeFamilyFrom(familyMetricsMatch[1]);
    return applyFamilyScenario(json(familyMetrics(familyMetricsMatch[1])), family, url.pathname);
  }

  const response = apiResponse(requestMethod, url, requestBody);
  if (response) {
    const parts = url.pathname.split("/").filter(Boolean).map(decodeURIComponent);
    const pathFamily = url.pathname.match(/^\/api\/v1\/(\d+)(?:\/|$)/)?.[1];
    const queryFamily =
      url.pathname === "/api/v1/search" ? url.searchParams.get("route_family") : null;
    const familyValue = pathFamily ?? queryFamily;
    const domainValue =
      parts[3] ?? (url.pathname === "/api/v1/search" ? url.searchParams.get("domain") : null);
    const domain =
      domainValue && domains.includes(domainValue as Domain) ? (domainValue as Domain) : undefined;

    return familyValue
      ? applyFamilyScenario(response, routeFamilyFrom(familyValue), url.pathname, domain)
      : response;
  }

  if (url.pathname.startsWith("/api/")) {
    return json({ error: `Mock endpoint not implemented: ${requestMethod} ${url.pathname}` }, 404);
  }

  return null;
}
