export type DomainId = "kv" | "stream" | "lease" | "schedule" | "notice" | "rpc";

export interface ResourceRef {
  area: string;
  realm: string;
  resource: string;
}

export interface ResourceMetric {
  label: string;
  value: string | number;
  caption?: string;
}

export interface ResourceTimelineEvent {
  ageSeconds?: number | null;
  correlationId?: string | null;
  kind: string;
  observedAt: string;
  summary: string;
}

export interface ResourceTimeline {
  derived: boolean;
  events: ResourceTimelineEvent[];
  limit: number;
}

export interface ResourceComparison {
  comparisonMode: string;
  derived: boolean;
  metrics: ResourceMetric[];
  summary: string;
}

export interface ResourceRelatedTable {
  columns: string[];
  rows: Array<Record<string, string | number>>;
  title: string;
}

export interface ResourceDetail {
  comparison?: ResourceComparison;
  detailMetrics: ResourceMetric[];
  domain: DomainId;
  raw: unknown;
  ref: ResourceRef;
  related: ResourceRelatedTable[];
  timeline: ResourceTimeline;
}

export interface ResourceInventoryArea {
  area: string;
  resources: string[];
}

export interface ResourceInventoryRealm {
  areas: ResourceInventoryArea[];
  realm: string;
}

export interface ResourceInventory {
  domain: DomainId;
  realms: ResourceInventoryRealm[];
}
