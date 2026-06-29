import type { GenericResourceDomainSegment } from "@/shared/navigation/domains";

export type DomainId = GenericResourceDomainSegment;

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

export interface ResourceScope {
  area: string;
  realm: string;
  resource: string;
}

export interface ResourceInventoryResource {
  estimateComplete?: boolean;
  estimatedRecordCount?: number;
  estimatedStorageBytes?: number;
  readLatencyAvgMs?: number;
  readLatencyP95Ms?: number;
  resource: string;
  transactionsActive?: number;
  writeLatencyAvgMs?: number;
  writeLatencyP95Ms?: number;
}

export interface ResourceTimelineEvent {
  ageSeconds?: number | null;
  attempts?: number | null;
  area: string;
  correlationId?: string | null;
  kind: string;
  messageId?: number | null;
  observedAt: string;
  operation?: string | null;
  ownerSession?: string | null;
  realm: string;
  resource: string;
  summary: string;
  workerSession?: string | null;
}

export interface ResourceTimeline {
  derived: boolean;
  events: ResourceTimelineEvent[];
  limit: number;
  area: string;
  realm: string;
  resource: string;
}

export interface ResourceComparison {
  comparisonMode: string;
  derived: boolean;
  metrics: ResourceMetric[];
  summary: string;
  leftScope?: ResourceScope;
  rightScope?: ResourceScope;
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
  resourceEntries: ResourceInventoryResource[];
}

export interface ResourceInventoryRealm {
  areas: ResourceInventoryArea[];
  realm: string;
}

export interface ResourceInventory {
  domain: DomainId;
  realms: ResourceInventoryRealm[];
}
