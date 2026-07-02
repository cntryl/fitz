import type {
  ResourceComparison as ResourceComparisonDto,
  ResourceTimeline as ResourceTimelineDto,
  ResourceTimelineEvent as ResourceTimelineEventDto,
} from "@/adapters";
import type {
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

export function mapResourceDetail(input: {
  comparison?: ResourceComparisonDto;
  detailMetrics: ResourceMetric[];
  domain: ResourceDetail["domain"];
  raw: unknown;
  ref: ResourceRef;
  related: ResourceRelatedTable[];
  timeline: ResourceTimelineDto;
}): ResourceDetail {
  return {
    comparison: input.comparison ? mapResourceComparison(input.comparison) : undefined,
    detailMetrics: input.detailMetrics,
    domain: input.domain,
    raw: input.raw,
    ref: input.ref,
    related: input.related,
    timeline: mapResourceTimeline(input.timeline),
  };
}
