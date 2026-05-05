import type { QueueDeadLetter } from "@/adapters/generated/types";
import type { DeadLetterMessage } from "./queue-models";

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
