import type { DeadLetterMessage, QueueResourceRef } from "./queue-models";

export interface QueueResourceDetail {
  area: string;
  realm: string;
  resource: string;
  messagesReady: number;
  messagesInflight: number;
  messagesDelayed: number;
  messagesDeadLettered: number;
  messagesTotal: number;
  oldestMessageAgeSeconds: number;
}

export interface QueueInflightMessage {
  area: string;
  attempts: number;
  expiresAt: string;
  family: number;
  inflightToken: string;
  messageId: number;
  realm: string;
  resource: string;
  sessionId: string;
}

export interface QueueResourceOverview {
  detail: QueueResourceDetail;
  inflight: QueueInflightMessage[];
  deadLetters: DeadLetterMessage[];
}

export type { QueueResourceRef };
