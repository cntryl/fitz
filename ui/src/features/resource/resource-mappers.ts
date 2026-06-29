import type {
  KvResourceDetail,
  KvTransaction,
  LeaseResourceDetail,
  NoticeResourceDetail,
  NoticeSubscription,
  OperationCollection,
  ResourceComparison as ResourceComparisonDto,
  ResourceTimeline as ResourceTimelineDto,
  ResourceTimelineEvent as ResourceTimelineEventDto,
  RpcPendingRequest,
  RpcWorker,
  ScheduleResourceDetail,
  StreamAreaWatermark,
  StreamResourceDetail,
} from "@/adapters";
import type {
  DomainId,
  ResourceComparison,
  ResourceDetail,
  ResourceMetric,
  ResourceRef,
  ResourceRelatedTable,
  ResourceTimeline,
  ResourceTimelineEvent,
} from "./resource-models";

function formatMaybe(value: unknown) {
  if (value == null || value === "") return "n/a";
  if (typeof value === "string") return value;
  if (typeof value === "number") return Number.isFinite(value) ? value : "n/a";
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "symbol") return value.description ?? "symbol";
  if (typeof value === "function") return "function";
  if (typeof value === "object") return JSON.stringify(value);
  return "n/a";
}

function metric(label: string, value: unknown, caption?: string): ResourceMetric {
  return { label, value: formatMaybe(value), caption };
}

function formatMilliseconds(value: number | undefined) {
  return `${(value ?? 0).toFixed(1)}ms`;
}

function mapTimelineEvent(event: ResourceTimelineEventDto): ResourceTimelineEvent {
  return {
    ageSeconds: event.age_seconds ?? null,
    attempts: event.attempts ?? null,
    area: event.area,
    correlationId: event.correlation_id ?? null,
    kind: event.kind,
    messageId: event.message_id ?? null,
    observedAt: event.observed_at,
    operation: event.operation ?? null,
    ownerSession: event.owner_session ?? null,
    realm: event.realm,
    resource: event.resource,
    summary: event.summary,
    workerSession: event.worker_session ?? null,
  };
}

export function mapResourceTimeline(dto: ResourceTimelineDto): ResourceTimeline {
  return {
    area: dto.area,
    derived: dto.derived,
    events: dto.events.map(mapTimelineEvent),
    limit: dto.limit,
    realm: dto.realm,
    resource: dto.resource,
  };
}

function mapResourceComparisonScope(dtoScope: { area: string; realm: string; resource: string }) {
  return {
    area: dtoScope.area,
    realm: dtoScope.realm,
    resource: dtoScope.resource,
  };
}

export function mapResourceComparison(dto: ResourceComparisonDto): ResourceComparison {
  const metrics = Object.entries(dto.delta).map(([key, value]) =>
    metric(key.replace(/_/g, " "), value, "Delta against comparison target"),
  );

  return {
    comparisonMode: dto.comparison_mode,
    derived: dto.derived,
    metrics,
    summary: dto.summary,
    leftScope: mapResourceComparisonScope(dto.left.scope),
    rightScope: mapResourceComparisonScope(dto.right.scope),
  };
}

function countRowsByTitle(related: ResourceRelatedTable[], title: string): number | null {
  const entry = related.find((table) => table.title === title);

  return entry ? entry.rows.length : null;
}

function metricsForDetail(
  domain: DomainId,
  detail: unknown,
  related: ResourceRelatedTable[] = [],
): ResourceMetric[] {
  switch (domain) {
    case "kv": {
      const dto = detail as KvResourceDetail;
      return [
        metric("Estimated records", dto.estimated_record_count, "Committed KV inventory"),
        metric(
          "Logical storage bytes",
          dto.estimated_storage_bytes,
          "User key bytes + value bytes",
        ),
        metric(
          "Estimate complete",
          dto.estimate_complete,
          "False after bounded range-delete refresh",
        ),
        metric(
          "Read latency avg",
          formatMilliseconds(dto.read_latency_avg_ms),
          "Rolling read samples",
        ),
        metric(
          "Read latency p95",
          formatMilliseconds(dto.read_latency_p95_ms),
          "Rolling read samples",
        ),
        metric(
          "Write latency avg",
          formatMilliseconds(dto.write_latency_avg_ms),
          "Rolling write commit samples",
        ),
        metric(
          "Write latency p95",
          formatMilliseconds(dto.write_latency_p95_ms),
          "Rolling write commit samples",
        ),
        metric(
          "Active transactions (broker-local/session-scoped)",
          dto.transactions_active,
          "Live in-memory transactions only",
        ),
        metric("Diagnostic severity", dto.diagnostics.severity, "Live broker diagnostics"),
        metric("Diagnostic trend", dto.diagnostics.trend, "Pressure trend"),
        metric("Contention", dto.diagnostics.contention_count, "Session-scoped contention"),
        metric("Recent transitions", dto.diagnostics.recent_transition_count, "Diagnostic recency"),
        metric("Waiters", dto.diagnostics.waiter_count, "Live demand"),
      ];
    }
    case "stream": {
      const dto = detail as StreamResourceDetail;
      return [
        metric("Offset", dto.offset, "Durable stream metadata"),
        metric("Watermark", dto.watermark, "Durable stream metadata"),
        metric("Size bytes", dto.size_bytes, "Durable stream metadata"),
        metric("Append sessions (live)", dto.sessions_active, "Live broker snapshot"),
        metric("Diagnostic severity", dto.diagnostics.severity, "Live broker diagnostics"),
        metric("Diagnostic trend", dto.diagnostics.trend, "Routing pressure"),
      ];
    }
    case "lease": {
      const dto = detail as LeaseResourceDetail;
      return [
        metric("Active leases (ephemeral coordination)", dto.active_leases, "Live ownership state"),
        metric("Waiters", dto.diagnostics.waiter_count, "Coordination demand"),
        metric(
          "Oldest lease age",
          `${dto.oldest_lease_age_seconds}s`,
          "How long the oldest lease has been held",
        ),
        metric("Diagnostic severity", dto.diagnostics.severity, "Live broker diagnostics"),
        metric("Diagnostic trend", dto.diagnostics.trend, "Coordination pressure"),
        metric("Recent transitions", dto.diagnostics.recent_transition_count, "Ownership churn"),
      ];
    }
    case "schedule": {
      const dto = detail as ScheduleResourceDetail;
      return [
        metric("Enabled", dto.enabled, "Durable timing intent exists when enabled"),
        metric("Executions", dto.executions_total, "Current broker counter"),
        metric("Next run", dto.next_run ?? "unknown", "Next timing window"),
        metric("Cron", dto.cron ?? "unset", "Schedule policy"),
        metric("Diagnostic severity", dto.diagnostics.severity, "Live broker diagnostics"),
        metric("Diagnostic trend", dto.diagnostics.trend, "Live pressure signal"),
      ];
    }
    case "notice": {
      const dto = detail as NoticeResourceDetail;
      return [
        metric(
          "Active subscriptions (live session fanout)",
          dto.subscriptions_active,
          "Session-scoped ephemeral fanout",
        ),
        metric("Diagnostic severity", dto.diagnostics.severity, "Live broker diagnostics"),
        metric("Diagnostic trend", dto.diagnostics.trend, "Transient pressure"),
        metric("Transitions", dto.diagnostics.recent_transition_count, "Recent fanout churn"),
        metric("Waiters", dto.diagnostics.waiter_count, "Subscriber demand"),
        metric("Contention", dto.diagnostics.contention_count, "Session pressure"),
      ];
    }
    case "rpc": {
      const dto = detail as OperationCollection;
      const workerCount = countRowsByTitle(related, "RPC workers") ?? 0;
      const pendingCount = countRowsByTitle(related, "RPC pending requests") ?? 0;

      return [
        metric("Registered operations", dto.operations.length, "Live operation scope"),
        metric("Active workers (live)", workerCount, "Live request/response handlers"),
        metric("Pending requests (live)", pendingCount, "In-memory, not durable"),
        metric("Realm", dto.realm, "Scope context"),
        metric("Area", dto.area, "Scope context"),
        metric("Resource", dto.resource, "Scope context"),
      ];
    }
  }
}

function tableFromRecords(
  title: string,
  columns: string[],
  rows: Array<object>,
): ResourceRelatedTable {
  return {
    columns,
    rows: rows.map((row) =>
      Object.fromEntries(
        columns.map((column) => [column, formatMaybe((row as Record<string, unknown>)[column])]),
      ),
    ),
    title,
  };
}

export function mapKvTransactions(transactions: KvTransaction[]): ResourceRelatedTable {
  return tableFromRecords(
    "KV transactions",
    ["tx_id", "mode", "operations_count", "idle_seconds"],
    transactions,
  );
}

export function mapStreamWatermarks(watermarks: StreamAreaWatermark[]): ResourceRelatedTable {
  return tableFromRecords("Stream watermarks", ["family", "watermark"], watermarks);
}

export function mapNoticeSubscriptions(subscriptions: NoticeSubscription[]): ResourceRelatedTable {
  return tableFromRecords(
    "Notice subscriptions",
    ["subscription_id", "session_id", "pattern", "notifications_received"],
    subscriptions,
  );
}

export function mapRpcOperations(operations: string[]): ResourceRelatedTable {
  return tableFromRecords(
    "RPC operations",
    ["operation"],
    operations.map((operation) => ({ operation })),
  );
}

export function mapRpcWorkers(workers: RpcWorker[]): ResourceRelatedTable {
  return tableFromRecords(
    "RPC workers",
    ["session_id", "route", "requests_handled", "average_latency_ms"],
    workers,
  );
}

export function mapRpcPending(requests: RpcPendingRequest[]): ResourceRelatedTable {
  return tableFromRecords(
    "RPC pending requests",
    ["correlation_id", "route", "age_seconds", "worker_session_id"],
    requests,
  );
}

export function mapResourceDetail(input: {
  comparison?: ResourceComparisonDto;
  detail: unknown;
  domain: DomainId;
  raw: unknown;
  ref: ResourceRef;
  related: ResourceRelatedTable[];
  timeline: ResourceTimelineDto;
}): ResourceDetail {
  return {
    comparison: input.comparison ? mapResourceComparison(input.comparison) : undefined,
    detailMetrics: metricsForDetail(input.domain, input.detail, input.related),
    domain: input.domain,
    raw: input.raw,
    ref: input.ref,
    related: input.related,
    timeline: mapResourceTimeline(input.timeline),
  };
}
