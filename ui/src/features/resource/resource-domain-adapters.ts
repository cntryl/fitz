import {
  apiv1,
  type KvResourceDetail,
  type KvTransaction,
  type LeaseResourceDetail,
  type NoticeResourceDetail,
  type NoticeSubscription,
  type OperationCollection,
  type ResourceComparison as ResourceComparisonDto,
  type ResourceEntry,
  type ResourceTimeline as ResourceTimelineDto,
  type RpcPendingRequest,
  type RpcWorker,
  type ScheduleResourceDetail,
  type StreamAreaWatermark,
  type StreamResourceDetail,
} from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "@/shared/navigation/domains";
import type {
  DomainId,
  ResourceMetric,
  ResourceRef,
  ResourceRelatedTable,
} from "./resource-models";

type ResourceEntryWithOperation = ResourceEntry & { operation?: string };

export interface ResourceDomainAdapter<Detail = unknown> {
  domain: DomainId;
  listRealms(options: ServiceRequestOptions): Promise<Array<{ realm: string }>>;
  listAreas(realm: string, options: ServiceRequestOptions): Promise<Array<{ area: string }>>;
  listResources(
    ref: Omit<ResourceRef, "resource">,
    options: ServiceRequestOptions,
  ): Promise<ResourceEntryWithOperation[]>;
  loadDetail(ref: ResourceRef, options: ServiceRequestOptions): Promise<Detail>;
  loadTimeline(ref: ResourceRef, options: ServiceRequestOptions): Promise<ResourceTimelineDto>;
  loadComparison(
    ref: ResourceRef,
    against: ResourceRef | null,
    options: ServiceRequestOptions,
  ): Promise<ResourceComparisonDto | undefined>;
  loadRelated(ref: ResourceRef, options: ServiceRequestOptions): Promise<ResourceRelatedTable[]>;
  mapDetailMetrics(detail: Detail, related: ResourceRelatedTable[]): ResourceMetric[];
}

function family() {
  return apiRouteFamilySegment();
}

function comparisonQuery(against: ResourceRef) {
  return {
    against_area: against.area,
    against_realm: against.realm,
    against_resource: against.resource,
  };
}

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

function mapKvTransactions(transactions: KvTransaction[]): ResourceRelatedTable {
  return tableFromRecords(
    "KV transactions",
    ["tx_id", "mode", "operations_count", "idle_seconds"],
    transactions,
  );
}

function mapStreamWatermarks(watermarks: StreamAreaWatermark[]): ResourceRelatedTable {
  return tableFromRecords("Stream watermarks", ["family", "watermark"], watermarks);
}

function mapNoticeSubscriptions(subscriptions: NoticeSubscription[]): ResourceRelatedTable {
  return tableFromRecords(
    "Notice subscriptions",
    ["subscription_id", "session_id", "pattern", "notifications_received"],
    subscriptions,
  );
}

function mapRpcOperations(operations: string[]): ResourceRelatedTable {
  return tableFromRecords(
    "RPC operations",
    ["operation"],
    operations.map((operation) => ({ operation })),
  );
}

function mapRpcWorkers(workers: RpcWorker[]): ResourceRelatedTable {
  return tableFromRecords(
    "RPC workers",
    ["session_id", "route", "requests_handled", "average_latency_ms"],
    workers,
  );
}

function mapRpcPending(requests: RpcPendingRequest[]): ResourceRelatedTable {
  return tableFromRecords(
    "RPC pending requests",
    ["correlation_id", "route", "age_seconds", "worker_session_id"],
    requests,
  );
}

function countRowsByTitle(related: ResourceRelatedTable[], title: string): number | null {
  const entry = related.find((table) => table.title === title);

  return entry ? entry.rows.length : null;
}

const kvResourceAdapter: ResourceDomainAdapter<KvResourceDetail> = {
  domain: "kv",
  async listRealms(options) {
    return unwrapResponse(await apiv1.listKvRealms(family(), options), "Unable to load KV realms")
      .realms;
  },
  async listAreas(realm, options) {
    return unwrapResponse(
      await apiv1.listKvAreas(family(), realm, options),
      "Unable to load KV areas",
    ).areas;
  },
  async listResources(ref, options) {
    return unwrapResponse(
      await apiv1.listKvResources(family(), ref.realm, ref.area, options),
      "Unable to load KV resources",
    ).resources;
  },
  async loadDetail(ref, options) {
    return unwrapResponse(
      await apiv1.getKvResource(family(), ref.realm, ref.area, ref.resource, options),
      "Unable to load KV resource",
    );
  },
  async loadTimeline(ref, options) {
    return unwrapResponse(
      await apiv1.listKvResourceEvents(
        family(),
        ref.realm,
        ref.area,
        ref.resource,
        { limit: 20 },
        options,
      ),
      "Unable to load KV timeline",
    );
  },
  async loadComparison(ref, against, options) {
    if (!against) return undefined;

    return unwrapResponse(
      await apiv1.compareKvResourceSnapshots(
        family(),
        ref.realm,
        ref.area,
        ref.resource,
        comparisonQuery(against),
        options,
      ),
      "Unable to compare KV resource",
    );
  },
  async loadRelated(ref, options) {
    return [
      mapKvTransactions(
        unwrapResponse(
          await apiv1.listKvTransactions(family(), ref.realm, ref.area, ref.resource, options),
          "Unable to load KV transactions",
        ).transactions,
      ),
    ];
  },
  mapDetailMetrics(detail) {
    return [
      metric("Estimated records", detail.estimated_record_count, "Committed KV inventory"),
      metric(
        "Logical storage bytes",
        detail.estimated_storage_bytes,
        "User key bytes + value bytes",
      ),
      metric(
        "Estimate complete",
        detail.estimate_complete,
        "False after bounded range-delete refresh",
      ),
      metric(
        "Read latency avg",
        formatMilliseconds(detail.read_latency_avg_ms),
        "Rolling read samples",
      ),
      metric(
        "Read latency p95",
        formatMilliseconds(detail.read_latency_p95_ms),
        "Rolling read samples",
      ),
      metric(
        "Write latency avg",
        formatMilliseconds(detail.write_latency_avg_ms),
        "Rolling write commit samples",
      ),
      metric(
        "Write latency p95",
        formatMilliseconds(detail.write_latency_p95_ms),
        "Rolling write commit samples",
      ),
      metric(
        "Active transactions (broker-local/session-scoped)",
        detail.transactions_active,
        "Live in-memory transactions only",
      ),
      metric("Diagnostic severity", detail.diagnostics.severity, "Live broker diagnostics"),
      metric("Diagnostic trend", detail.diagnostics.trend, "Pressure trend"),
      metric("Contention", detail.diagnostics.contention_count, "Session-scoped contention"),
      metric(
        "Recent transitions",
        detail.diagnostics.recent_transition_count,
        "Diagnostic recency",
      ),
      metric("Waiters", detail.diagnostics.waiter_count, "Live demand"),
    ];
  },
};

const streamResourceAdapter: ResourceDomainAdapter<StreamResourceDetail> = {
  domain: "stream",
  async listRealms(options) {
    return unwrapResponse(
      await apiv1.listStreamRealms(family(), options),
      "Unable to load stream realms",
    ).realms;
  },
  async listAreas(realm, options) {
    return unwrapResponse(
      await apiv1.listStreamAreas(family(), realm, options),
      "Unable to load stream areas",
    ).areas;
  },
  async listResources(ref, options) {
    return unwrapResponse(
      await apiv1.listStreamResources(family(), ref.realm, ref.area, options),
      "Unable to load stream resources",
    ).resources;
  },
  async loadDetail(ref, options) {
    return unwrapResponse(
      await apiv1.getStreamResource(family(), ref.realm, ref.area, ref.resource, options),
      "Unable to load stream resource",
    );
  },
  async loadTimeline(ref, options) {
    return unwrapResponse(
      await apiv1.listStreamResourceEvents(
        family(),
        ref.realm,
        ref.area,
        ref.resource,
        { limit: 20 },
        options,
      ),
      "Unable to load stream timeline",
    );
  },
  async loadComparison(ref, against, options) {
    if (!against) return undefined;

    return unwrapResponse(
      await apiv1.compareStreamResourceSnapshots(
        family(),
        ref.realm,
        ref.area,
        ref.resource,
        comparisonQuery(against),
        options,
      ),
      "Unable to compare stream resource",
    );
  },
  async loadRelated(ref, options) {
    return [
      mapStreamWatermarks(
        unwrapResponse(
          await apiv1.getStreamAreaWatermarks(family(), ref.realm, ref.area, options),
          "Unable to load stream watermarks",
        ).family_watermarks,
      ),
    ];
  },
  mapDetailMetrics(detail) {
    return [
      metric("Offset", detail.offset, "Durable stream metadata"),
      metric("Watermark", detail.watermark, "Durable stream metadata"),
      metric("Size bytes", detail.size_bytes, "Durable stream metadata"),
      metric("Append sessions (live)", detail.sessions_active, "Live broker snapshot"),
      metric("Diagnostic severity", detail.diagnostics.severity, "Live broker diagnostics"),
      metric("Diagnostic trend", detail.diagnostics.trend, "Routing pressure"),
    ];
  },
};

const leaseResourceAdapter: ResourceDomainAdapter<LeaseResourceDetail> = {
  domain: "lease",
  async listRealms(options) {
    return unwrapResponse(
      await apiv1.listLeaseRealms(family(), options),
      "Unable to load lease realms",
    ).realms;
  },
  async listAreas(realm, options) {
    return unwrapResponse(
      await apiv1.listLeaseAreas(family(), realm, options),
      "Unable to load lease areas",
    ).areas;
  },
  async listResources(ref, options) {
    return unwrapResponse(
      await apiv1.listLeaseResources(family(), ref.realm, ref.area, options),
      "Unable to load lease resources",
    ).resources;
  },
  async loadDetail(ref, options) {
    return unwrapResponse(
      await apiv1.getLeaseResource(family(), ref.realm, ref.area, ref.resource, options),
      "Unable to load lease resource",
    );
  },
  async loadTimeline(ref, options) {
    return unwrapResponse(
      await apiv1.listLeaseResourceEvents(
        family(),
        ref.realm,
        ref.area,
        ref.resource,
        { limit: 20 },
        options,
      ),
      "Unable to load lease timeline",
    );
  },
  async loadComparison(ref, against, options) {
    if (!against) return undefined;

    return unwrapResponse(
      await apiv1.compareLeaseResourceSnapshots(
        family(),
        ref.realm,
        ref.area,
        ref.resource,
        comparisonQuery(against),
        options,
      ),
      "Unable to compare lease resource",
    );
  },
  async loadRelated() {
    return [];
  },
  mapDetailMetrics(detail) {
    return [
      metric(
        "Active leases (ephemeral coordination)",
        detail.active_leases,
        "Live ownership state",
      ),
      metric("Waiters", detail.diagnostics.waiter_count, "Coordination demand"),
      metric(
        "Oldest lease age",
        `${detail.oldest_lease_age_seconds}s`,
        "How long the oldest lease has been held",
      ),
      metric("Diagnostic severity", detail.diagnostics.severity, "Live broker diagnostics"),
      metric("Diagnostic trend", detail.diagnostics.trend, "Coordination pressure"),
      metric("Recent transitions", detail.diagnostics.recent_transition_count, "Ownership churn"),
    ];
  },
};

const scheduleResourceAdapter: ResourceDomainAdapter<ScheduleResourceDetail> = {
  domain: "schedule",
  async listRealms(options) {
    return unwrapResponse(
      await apiv1.listScheduleRealms(family(), options),
      "Unable to load schedule realms",
    ).realms;
  },
  async listAreas(realm, options) {
    return unwrapResponse(
      await apiv1.listScheduleAreas(family(), realm, options),
      "Unable to load schedule areas",
    ).areas;
  },
  async listResources(ref, options) {
    return unwrapResponse(
      await apiv1.listScheduleResources(family(), ref.realm, ref.area, options),
      "Unable to load schedule resources",
    ).resources;
  },
  async loadDetail(ref, options) {
    return unwrapResponse(
      await apiv1.getScheduleResource(family(), ref.realm, ref.area, ref.resource, options),
      "Unable to load schedule resource",
    );
  },
  async loadTimeline(ref, options) {
    return unwrapResponse(
      await apiv1.listScheduleResourceEvents(
        family(),
        ref.realm,
        ref.area,
        ref.resource,
        { limit: 20 },
        options,
      ),
      "Unable to load schedule timeline",
    );
  },
  async loadComparison(ref, against, options) {
    if (!against) return undefined;

    return unwrapResponse(
      await apiv1.compareScheduleResourceSnapshots(
        family(),
        ref.realm,
        ref.area,
        ref.resource,
        comparisonQuery(against),
        options,
      ),
      "Unable to compare schedule resource",
    );
  },
  async loadRelated() {
    return [];
  },
  mapDetailMetrics(detail) {
    return [
      metric("Enabled", detail.enabled, "Durable timing intent exists when enabled"),
      metric("Executions", detail.executions_total, "Current broker counter"),
      metric("Next run", detail.next_run ?? "unknown", "Next timing window"),
      metric("Cron", detail.cron ?? "unset", "Schedule policy"),
      metric("Diagnostic severity", detail.diagnostics.severity, "Live broker diagnostics"),
      metric("Diagnostic trend", detail.diagnostics.trend, "Live pressure signal"),
    ];
  },
};

const noticeResourceAdapter: ResourceDomainAdapter<NoticeResourceDetail> = {
  domain: "notice",
  async listRealms(options) {
    return unwrapResponse(
      await apiv1.listNoticeRealms(family(), options),
      "Unable to load notice realms",
    ).realms;
  },
  async listAreas(realm, options) {
    return unwrapResponse(
      await apiv1.listNoticeAreas(family(), realm, options),
      "Unable to load notice areas",
    ).areas;
  },
  async listResources(ref, options) {
    return unwrapResponse(
      await apiv1.listNoticeResources(family(), ref.realm, ref.area, options),
      "Unable to load notice resources",
    ).resources;
  },
  async loadDetail(ref, options) {
    return unwrapResponse(
      await apiv1.getNoticeResource(family(), ref.realm, ref.area, ref.resource, options),
      "Unable to load notice resource",
    );
  },
  async loadTimeline(ref, options) {
    return unwrapResponse(
      await apiv1.listNoticeResourceEvents(
        family(),
        ref.realm,
        ref.area,
        ref.resource,
        { limit: 20 },
        options,
      ),
      "Unable to load notice timeline",
    );
  },
  async loadComparison(ref, against, options) {
    if (!against) return undefined;

    return unwrapResponse(
      await apiv1.compareNoticeResourceSnapshots(
        family(),
        ref.realm,
        ref.area,
        ref.resource,
        comparisonQuery(against),
        options,
      ),
      "Unable to compare notice resource",
    );
  },
  async loadRelated(ref, options) {
    return [
      mapNoticeSubscriptions(
        unwrapResponse(
          await apiv1.listNoticeSubscriptions(family(), ref.realm, ref.area, ref.resource, options),
          "Unable to load notice subscriptions",
        ).subscriptions,
      ),
    ];
  },
  mapDetailMetrics(detail) {
    return [
      metric(
        "Active subscriptions (live session fanout)",
        detail.subscriptions_active,
        "Session-scoped ephemeral fanout",
      ),
      metric("Diagnostic severity", detail.diagnostics.severity, "Live broker diagnostics"),
      metric("Diagnostic trend", detail.diagnostics.trend, "Transient pressure"),
      metric("Transitions", detail.diagnostics.recent_transition_count, "Recent fanout churn"),
      metric("Waiters", detail.diagnostics.waiter_count, "Subscriber demand"),
      metric("Contention", detail.diagnostics.contention_count, "Session pressure"),
    ];
  },
};

const rpcResourceAdapter: ResourceDomainAdapter<OperationCollection> = {
  domain: "rpc",
  async listRealms(options) {
    return unwrapResponse(await apiv1.listRpcRealms(family(), options), "Unable to load RPC realms")
      .realms;
  },
  async listAreas(realm, options) {
    return unwrapResponse(
      await apiv1.listRpcAreas(family(), realm, options),
      "Unable to load RPC areas",
    ).areas;
  },
  async listResources(ref, options) {
    return unwrapResponse(
      await apiv1.listRpcResources(family(), ref.realm, ref.area, options),
      "Unable to load RPC resources",
    ).resources;
  },
  async loadDetail(ref, options) {
    return unwrapResponse(
      await apiv1.getRpcResource(family(), ref.realm, ref.area, ref.resource, options),
      "Unable to load RPC resource",
    );
  },
  async loadTimeline(ref, options) {
    return unwrapResponse(
      await apiv1.listRpcResourceEvents(
        family(),
        ref.realm,
        ref.area,
        ref.resource,
        { limit: 20 },
        options,
      ),
      "Unable to load RPC timeline",
    );
  },
  async loadComparison(ref, against, options) {
    if (!against) return undefined;

    return unwrapResponse(
      await apiv1.compareRpcResourceSnapshots(
        family(),
        ref.realm,
        ref.area,
        ref.resource,
        comparisonQuery(against),
        options,
      ),
      "Unable to compare RPC resource",
    );
  },
  async loadRelated(ref, options) {
    const operations = unwrapResponse(
      await apiv1.listRpcOperations(family(), ref.realm, ref.area, ref.resource, options),
      "Unable to load RPC operations",
    ).operations.map((entry) => entry.operation);
    const pending = unwrapResponse(
      await apiv1.listRpcPendingRequests(family(), { realm: ref.realm }, options),
      "Unable to load RPC pending requests",
    ).requests;
    const firstOperation = operations[0];
    const workers = firstOperation
      ? unwrapResponse(
          await apiv1.listRpcOperationWorkers(
            family(),
            ref.realm,
            ref.area,
            ref.resource,
            firstOperation,
            options,
          ),
          "Unable to load RPC workers",
        ).workers
      : [];

    return [mapRpcOperations(operations), mapRpcWorkers(workers), mapRpcPending(pending)];
  },
  mapDetailMetrics(detail, related) {
    const workerCount = countRowsByTitle(related, "RPC workers") ?? 0;
    const pendingCount = countRowsByTitle(related, "RPC pending requests") ?? 0;

    return [
      metric("Registered operations", detail.operations.length, "Live operation scope"),
      metric("Active workers (live)", workerCount, "Live request/response handlers"),
      metric("Pending requests (live)", pendingCount, "In-memory, not durable"),
      metric("Realm", detail.realm, "Scope context"),
      metric("Area", detail.area, "Scope context"),
      metric("Resource", detail.resource, "Scope context"),
    ];
  },
};

export const resourceDomainAdapterRegistry = {
  kv: kvResourceAdapter,
  lease: leaseResourceAdapter,
  notice: noticeResourceAdapter,
  rpc: rpcResourceAdapter,
  schedule: scheduleResourceAdapter,
  stream: streamResourceAdapter,
} satisfies Record<DomainId, ResourceDomainAdapter>;

export const resourceDomainAdapterDomains = Object.keys(
  resourceDomainAdapterRegistry,
) as DomainId[];

export function getResourceDomainAdapter(domain: DomainId): ResourceDomainAdapter {
  const adapter = (
    resourceDomainAdapterRegistry as Partial<Record<DomainId, ResourceDomainAdapter>>
  )[domain];

  if (!adapter) {
    throw new Error(`Missing resource domain adapter for ${domain}`);
  }

  return adapter;
}
