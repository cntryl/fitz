import type { QueueDeadLetter } from "../adapters/generated/types";

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

// Explicit mapper boundary: snake_case DTO fields stop here.
export function mapQueueDeadLetter(dto: QueueDeadLetter): DeadLetterMessage {
  return {
    realm: dto.realm,
    area: dto.area,
    resource: dto.resource,
    family: dto.family,
    messageId: dto.message_id,
    attempts: dto.attempts,
    reason: dto.reason,
    deadLetteredAt: dto.dead_lettered_at,
  };
}
