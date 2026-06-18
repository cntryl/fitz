export interface QueueResourceRef {
  realm: string;
  area: string;
  resource: string;
}

export interface QueueRealmSummary {
  realm: string;
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
