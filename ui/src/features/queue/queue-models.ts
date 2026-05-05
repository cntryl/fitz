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
