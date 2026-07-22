export interface QueueResourceRef {
  realm: string;
  area: string;
  resource: string;
}

export type QueueStatus = "idle" | "draining" | "backlogged" | "falling_behind";

export interface QueueOperationalSummary {
  completeSuccessTotal: number;
  enqueueSuccessTotal: number;
  inRatePerSecond: number;
  messagesDeadLettered: number;
  messagesDelayed: number;
  messagesInflight: number;
  messagesReady: number;
  messagesTotal: number;
  oldestBacklogAgeSeconds: number;
  outRatePerSecond: number;
  status: QueueStatus;
  subscriptionsActive: number;
}

export interface QueueRealmSummary extends QueueOperationalSummary {
  areaCount: number;
  queueCount: number;
  realm: string;
}

export interface QueueAreaSummary extends QueueOperationalSummary {
  area: string;
  queueCount: number;
  realm: string;
}

export interface QueueResourceSummary extends QueueOperationalSummary {
  area: string;
  familyCount: number;
  realm: string;
  resource: string;
}

export interface QueueStatsSummary {
  inflightActive: number;
  messagesDeadLettered: number;
  messagesDelayed: number;
  messagesPending: number;
  messagesReady: number;
  oldestBacklogAgeSeconds: number;
  operationsPerSecond: number;
}

export interface QueueOverview {
  realms: QueueRealmSummary[];
  stats: QueueStatsSummary;
}

export interface QueueRealmDetail extends QueueOperationalSummary {
  areaCount: number;
  areas: QueueAreaSummary[];
  queueCount: number;
  queues: QueueResourceSummary[];
  realm: string;
}

export interface QueueAreaDetail extends QueueOperationalSummary {
  area: string;
  queueCount: number;
  queues: QueueResourceSummary[];
  realm: string;
}

export interface DeadLetterFilters {
  family?: number;
}

export interface DeadLetterMessage {
  realm: string;
  area: string;
  resource: string;
  family: number;
  messageId: number;
  attempts: number;
  reason: string;
  deadLetteredAt: string;
}

export interface QueueInventoryArea {
  area: string;
  resourceEntries: QueueResourceSummary[];
  resources: string[];
}

export interface QueueInventoryRealm {
  areas: QueueInventoryArea[];
  realm: string;
}

export interface QueueInventory {
  domain: "queue";
  realms: QueueInventoryRealm[];
}
