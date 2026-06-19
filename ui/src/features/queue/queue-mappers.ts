import type { QueueDeadLetter, QueueStats, RealmEntry } from "@/adapters";
import type {
  DeadLetterMessage,
  QueueOverview,
  QueueRealmSummary,
  QueueStatsSummary,
} from "./queue-models";

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

export function mapQueueRealm(dto: RealmEntry): QueueRealmSummary {
  return {
    realm: dto.realm,
  };
}

export function mapQueueStats(dto: QueueStats): QueueStatsSummary {
  return {
    inflightActive: dto.inflight_active,
    messagesDeadLettered: dto.messages_dead_lettered,
    messagesDelayed: dto.messages_delayed,
    messagesPending: dto.messages_pending,
    messagesReady: dto.messages_ready,
    oldestBacklogAgeSeconds: dto.oldest_backlog_age_seconds,
    operationsPerSecond: dto.operations_per_second,
  };
}

export function mapQueueOverview(realms: RealmEntry[], stats: QueueStats): QueueOverview {
  return {
    realms: realms.map(mapQueueRealm),
    stats: mapQueueStats(stats),
  };
}
