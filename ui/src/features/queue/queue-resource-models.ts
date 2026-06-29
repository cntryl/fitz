import type { DeadLetterMessage, QueueResourceRef, QueueStatus } from "./queue-models";

export interface QueueResourceComparisonScope {
  area: string;
  family?: number | null;
  realm: string;
  resource: string;
}

export interface QueueResourceComparisonMetrics {
  ageSeconds?: number | null;
  backlog?: number | null;
  deadLetters?: number | null;
  delayed?: number | null;
  inflight?: number | null;
  ready?: number | null;
  recentTransitionCount?: number | null;
  waiters?: number | null;
}

export interface QueueResourceComparisonSide {
  metrics: QueueResourceComparisonMetrics;
  scope: QueueResourceComparisonScope;
}

export interface QueueResourceComparison {
  comparisonMode: string;
  derived: boolean;
  delta: QueueResourceComparisonMetrics;
  left: QueueResourceComparisonSide;
  right: QueueResourceComparisonSide;
  summary: string;
}

export type QueueResourceTimelineKind =
  | "observation"
  | "transition"
  | "failure"
  | "retry"
  | "ownership_change"
  | "state_flip"
  | "registration";

export interface QueueResourceTimelineEvent {
  ageSeconds?: number | null;
  area: string;
  attempts?: number | null;
  correlationId?: string | null;
  kind: QueueResourceTimelineKind;
  messageId?: number | null;
  observedAt: string;
  operation?: string | null;
  ownerSession?: string | null;
  realm: string;
  resource: string;
  summary: string;
  workerSession?: string | null;
}

export interface QueueResourceTimeline {
  area: string;
  derived: boolean;
  events: QueueResourceTimelineEvent[];
  limit: number;
  realm: string;
  resource: string;
}

export interface QueueResourceDetail {
  area: string;
  realm: string;
  resource: string;
  completeSuccessTotal: number;
  enqueueSuccessTotal: number;
  inRatePerSecond: number;
  messagesReady: number;
  messagesInflight: number;
  messagesDelayed: number;
  messagesDeadLettered: number;
  messagesTotal: number;
  oldestBacklogAgeSeconds: number;
  oldestMessageAgeSeconds: number;
  outRatePerSecond: number;
  status: QueueStatus;
  subscriptionsActive: number;
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
  timeline: QueueResourceTimeline;
}

export type { QueueResourceRef };
