export interface QueueResourceRef {
  realm: string;
  area: string;
  resource: string;
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
