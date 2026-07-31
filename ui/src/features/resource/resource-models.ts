import type { GenericResourceDomainSegment } from "@/shared/navigation/domains";

export type DomainId = GenericResourceDomainSegment;

export interface ResourceInventoryResource {
  activeLeases?: number;
  committedEventCount?: number;
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
  nextRun?: string | null;
  notificationsReceived?: number;
  oldestBacklogAgeSeconds?: number;
  oldestLeaseAgeSeconds?: number;
  pendingClaims?: number;
  publishesPerMinute?: number;
  requestsPending?: number;
  schedulesActive?: number;
  sessionsActive?: number;
  sizeBytes?: number;
  slowestWorkerAverageLatencyMs?: number | null;
  subscriptionsActive?: number;
  waiters?: number;
  workersRegistered?: number;
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
