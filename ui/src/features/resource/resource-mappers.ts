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

function mapTimelineEvent(event: ResourceTimelineEventDto): ResourceTimelineEvent {
  return {
    ageSeconds: event.age_seconds ?? null,
    correlationId: event.correlation_id ?? null,
    kind: event.kind,
    observedAt: event.observed_at,
    summary: event.summary,
  };
}

export function mapResourceTimeline(dto: ResourceTimelineDto): ResourceTimeline {
  return {
    derived: dto.derived,
    events: dto.events.map(mapTimelineEvent),
    limit: dto.limit,
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
  };
}

function metricsForDetail(domain: DomainId, detail: unknown): ResourceMetric[] {
  switch (domain) {
    case "kv": {
      const dto = detail as KvResourceDetail;
      return [
        metric("Transactions active", dto.transactions_active),
        metric("Severity", dto.diagnostics.severity),
        metric("Trend", dto.diagnostics.trend),
        metric("Contention", dto.diagnostics.contention_count),
      ];
    }
    case "stream": {
      const dto = detail as StreamResourceDetail;
      return [
        metric("Offset", dto.offset, "Durable stream metadata"),
        metric("Watermark", dto.watermark, "Durable stream metadata"),
        metric("Size bytes", dto.size_bytes, "Durable stream metadata"),
        metric("Append sessions", dto.sessions_active, "Live broker snapshot"),
      ];
    }
    case "lease": {
      const dto = detail as LeaseResourceDetail;
      return [
        metric("Active leases", dto.active_leases),
        metric("Oldest lease age", `${dto.oldest_lease_age_seconds}s`),
        metric("Severity", dto.diagnostics.severity),
        metric("Waiters", dto.diagnostics.waiter_count),
      ];
    }
    case "schedule": {
      const dto = detail as ScheduleResourceDetail;
      return [
        metric("Enabled", dto.enabled),
        metric("Executions", dto.executions_total, "Current broker counter"),
        metric("Next run", dto.next_run),
        metric("Cron", dto.cron),
      ];
    }
    case "notice": {
      const dto = detail as NoticeResourceDetail;
      return [
        metric("Subscriptions", dto.subscriptions_active, "Live session-scoped fanout"),
        metric("Severity", dto.diagnostics.severity),
        metric("Trend", dto.diagnostics.trend),
        metric("Transitions", dto.diagnostics.recent_transition_count),
      ];
    }
    case "rpc": {
      const dto = detail as OperationCollection;
      return [
        metric("Operations", dto.operations.length),
        metric("Realm", dto.realm),
        metric("Area", dto.area),
        metric("Resource", dto.resource),
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
      Object.fromEntries(columns.map((column) => [column, formatMaybe((row as Record<string, unknown>)[column])])),
    ),
    title,
  };
}

export function mapKvTransactions(transactions: KvTransaction[]): ResourceRelatedTable {
  return tableFromRecords("KV transactions", ["tx_id", "mode", "operations_count", "idle_seconds"], transactions);
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
  return tableFromRecords("RPC workers", ["session_id", "route", "requests_handled", "average_latency_ms"], workers);
}

export function mapRpcPending(requests: RpcPendingRequest[]): ResourceRelatedTable {
  return tableFromRecords("RPC pending requests", ["correlation_id", "route", "age_seconds", "worker_session_id"], requests);
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
    detailMetrics: metricsForDetail(input.domain, input.detail),
    domain: input.domain,
    raw: input.raw,
    ref: input.ref,
    related: input.related,
    timeline: mapResourceTimeline(input.timeline),
  };
}
