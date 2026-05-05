import type {
  QueueDeadLetter,
  QueueInflight,
  QueueResourceDetail as QueueResourceDetailDto,
} from "@/adapters";
import { mapQueueDeadLetter } from "./queue-mappers";
import type {
  QueueInflightMessage,
  QueueResourceDetail,
  QueueResourceOverview,
} from "./queue-resource-models";

export function mapQueueResourceDetail(dto: QueueResourceDetailDto): QueueResourceDetail {
  return {
    area: dto.area,
    realm: dto.realm,
    resource: dto.resource,
    messagesReady: dto.messages_ready,
    messagesInflight: dto.messages_inflight,
    messagesDelayed: dto.messages_delayed,
    messagesDeadLettered: dto.messages_dead_lettered,
    messagesTotal: dto.messages_total,
    oldestMessageAgeSeconds: dto.oldest_message_age_seconds,
  };
}

export function mapQueueInflight(dto: QueueInflight): QueueInflightMessage {
  return {
    area: dto.area,
    attempts: dto.attempts,
    expiresAt: dto.expires_at,
    family: dto.family,
    inflightToken: dto.inflight_token,
    messageId: dto.message_id,
    realm: dto.realm,
    resource: dto.resource,
    sessionId: dto.session_id,
  };
}

export function mapQueueResourceOverview(
  detail: QueueResourceDetailDto,
  inflight: QueueInflight[],
  deadLetters: QueueDeadLetter[],
): QueueResourceOverview {
  return {
    detail: mapQueueResourceDetail(detail),
    inflight: inflight.map(mapQueueInflight),
    deadLetters: deadLetters.map(mapQueueDeadLetter),
  };
}
