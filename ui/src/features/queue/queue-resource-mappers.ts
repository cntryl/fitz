import type {
  QueueDeadLetter,
  QueueInflight,
  QueueResourceDetail as QueueResourceDetailDto,
  ResourceTimeline,
  ResourceTimelineEvent,
} from "@/adapters";
import { mapQueueDeadLetter } from "./queue-mappers";
import type {
  QueueInflightMessage,
  QueueResourceDetail,
  QueueResourceOverview,
  QueueResourceTimeline as QueueResourceTimelineModel,
  QueueResourceTimelineEvent as QueueResourceTimelineEventModel,
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

function mapQueueResourceTimelineEvent(
  dto: ResourceTimelineEvent,
): QueueResourceTimelineEventModel {
  return {
    ageSeconds: dto.age_seconds ?? null,
    area: dto.area,
    attempts: dto.attempts ?? null,
    correlationId: dto.correlation_id ?? null,
    kind: dto.kind,
    messageId: dto.message_id ?? null,
    observedAt: dto.observed_at,
    operation: dto.operation ?? null,
    ownerSession: dto.owner_session ?? null,
    realm: dto.realm,
    resource: dto.resource,
    summary: dto.summary,
    workerSession: dto.worker_session ?? null,
  };
}

export function mapQueueResourceTimeline(dto: ResourceTimeline): QueueResourceTimelineModel {
  return {
    area: dto.area,
    derived: dto.derived,
    events: dto.events.map(mapQueueResourceTimelineEvent),
    limit: dto.limit,
    realm: dto.realm,
    resource: dto.resource,
  };
}

export function mapQueueResourceOverview(
  detail: QueueResourceDetailDto,
  inflight: QueueInflight[],
  deadLetters: QueueDeadLetter[],
  timeline: ResourceTimeline,
): QueueResourceOverview {
  return {
    detail: mapQueueResourceDetail(detail),
    inflight: inflight.map(mapQueueInflight),
    deadLetters: deadLetters.map(mapQueueDeadLetter),
    timeline: mapQueueResourceTimeline(timeline),
  };
}
