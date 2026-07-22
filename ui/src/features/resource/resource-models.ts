import type { GenericResourceDomainSegment } from "@/shared/navigation/domains";

export type DomainId = GenericResourceDomainSegment;

export interface ResourceInventoryResource {
  estimateComplete?: boolean;
  estimatedRecordCount?: number;
  estimatedStorageBytes?: number;
  operation?: string;
  readLatencyAvgMs?: number;
  readLatencyP95Ms?: number;
  resource: string;
  transactionsActive?: number;
  writeLatencyAvgMs?: number;
  writeLatencyP95Ms?: number;
  messagesDeadLettered?: number;
  messagesDelayed?: number;
  messagesInflight?: number;
  messagesReady?: number;
  oldestBacklogAgeSeconds?: number;
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
