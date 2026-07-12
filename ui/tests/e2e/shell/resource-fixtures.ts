import type { DiagnosticSnapshot } from "@/adapters";

export type ResourceScope = {
  area: string;
  realm: string;
  resource: string;
};

export type ResourceDomain = "kv" | "lease" | "notice" | "rpc" | "schedule" | "stream";

export function parseResourceScope(segments: string[]): ResourceScope | null {
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

export function parseRouteResourceScope(path: string): ResourceScope {
  let parts = path.split("?")[0].split("/").filter(Boolean);
  if (parts[0] === "admin") {
    parts = parts.slice(2);
  }

  return {
    area: decodeURIComponent(parts[2] ?? ""),
    realm: decodeURIComponent(parts[1] ?? ""),
    resource: decodeURIComponent(parts[3] ?? ""),
  };
}

export function leaseSearchRowsFixture(scope: ResourceScope, expiresOffsetSeconds = 120) {
  return {
    area: scope.area,
    items: [
      {
        acquired_at: "2026-05-21T13:00:00.000Z",
        area: scope.area,
        expires_at: new Date(Date.now() + expiresOffsetSeconds * 1000).toISOString(),
        owner_id: "owner-lease-primary",
        owner_session_id: "session-lease-primary",
        pending_waiters: 2,
        queued_token: 8,
        realm: scope.realm,
        resource: scope.resource,
        state: "owned",
      },
    ],
    limit: 50,
    realm: scope.realm,
    resource: scope.resource,
    route_family: 7,
  };
}

export function resourceDetailFixture(
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

export function resourceTimelineFixture(domain: string, scope: ResourceScope) {
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

export function scheduleExecutionObservationsFixture(scope: ResourceScope) {
  return {
    area: scope.area,
    limit: 20,
    observations: [
      {
        area: scope.area,
        cron: "*/5 * * * *",
        executions_total: 42,
        last_run: "2026-05-21T13:00:00.000Z",
        next_run: "2026-05-21T13:05:00.000Z",
        operation: "handoff",
        realm: scope.realm,
        resource: scope.resource,
        route_family: 7,
        status: "observed",
      },
    ],
    realm: scope.realm,
    resource: scope.resource,
    route_family: 7,
  };
}

export function scheduleMissedHandoffsFixture(scope: ResourceScope) {
  return {
    limit: 20,
    observations: [
      {
        age_seconds: 90,
        area: scope.area,
        claimed_at: "2026-05-21T12:59:30.000Z",
        fire_at: "2026-05-21T12:59:00.000Z",
        fire_ms: 1780001940000,
        operation: "handoff",
        realm: scope.realm,
        resource: scope.resource,
        route_family: 7,
        status: "pending",
      },
    ],
    route_family: 7,
  };
}

export function rpcCallsFixture(options: {
  area: string;
  limit: number;
  operation?: string | null;
  realm: string;
  resource: string;
}) {
  const operation = options.operation ?? "GetStatus";

  return {
    limit: options.limit,
    observations: [
      {
        area: options.area,
        average_latency_ms: 12,
        correlation_id: "corr-rpc-1",
        operation,
        realm: options.realm,
        request_submitted_at: "2026-05-21T13:00:00.000Z",
        requests_handled: 7,
        resource: options.resource,
        route_family: 7,
        state: "worker_registered",
        worker_registered_at: "2026-05-21T12:59:00.000Z",
        worker_session_id: "worker-1",
      },
      {
        area: options.area,
        average_latency_ms: null,
        correlation_id: "corr-rpc-2",
        operation,
        realm: options.realm,
        request_submitted_at: "2026-05-21T13:00:10.000Z",
        requests_handled: null,
        resource: options.resource,
        route_family: 7,
        state: "pending",
        worker_registered_at: null,
        worker_session_id: null,
      },
    ],
    route_family: 7,
  };
}

export function streamRecordsFixture(options: {
  area: string;
  limit: number;
  realm: string;
  resource: string;
}) {
  return {
    area: options.area,
    has_more: false,
    limit: options.limit,
    records: [
      {
        area: options.area,
        body: { base64: "eyJvayI6dHJ1ZX0=", len_bytes: 11, utf8: '{"ok":true}' },
        created_at_ms: 1780000000000,
        discriminator: null,
        metadata: null,
        realm: options.realm,
        realm_offset: 0,
        resource: options.resource,
        resource_offset: 0,
        route_family: 7,
      },
    ],
    realm: options.realm,
    resource: options.resource,
    route_family: 7,
    from_offset: 0,
  };
}
