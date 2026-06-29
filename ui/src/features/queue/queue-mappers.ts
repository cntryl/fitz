import type {
  QueueAreaDetail as QueueAreaDetailDto,
  QueueAreaEntry,
  QueueDeadLetter,
  QueueRealmDetail as QueueRealmDetailDto,
  QueueRealmEntry,
  QueueResourceEntry,
  QueueStats,
} from "@/adapters";
import type {
  DeadLetterMessage,
  QueueAreaDetail,
  QueueAreaSummary,
  QueueOperationalSummary,
  QueueOverview,
  QueueRealmDetail,
  QueueRealmSummary,
  QueueResourceSummary,
  QueueStatsSummary,
} from "./queue-models";

type QueueOperationalDto = {
  complete_success_total: number;
  enqueue_success_total: number;
  in_rate_per_second: number;
  messages_dead_lettered: number;
  messages_delayed: number;
  messages_inflight: number;
  messages_ready: number;
  messages_total: number;
  oldest_backlog_age_seconds: number;
  out_rate_per_second: number;
  status: QueueOperationalSummary["status"];
  subscriptions_active: number;
};

function mapQueueOperational(dto: QueueOperationalDto): QueueOperationalSummary {
  return {
    completeSuccessTotal: dto.complete_success_total,
    enqueueSuccessTotal: dto.enqueue_success_total,
    inRatePerSecond: dto.in_rate_per_second,
    messagesDeadLettered: dto.messages_dead_lettered,
    messagesDelayed: dto.messages_delayed,
    messagesInflight: dto.messages_inflight,
    messagesReady: dto.messages_ready,
    messagesTotal: dto.messages_total,
    oldestBacklogAgeSeconds: dto.oldest_backlog_age_seconds,
    outRatePerSecond: dto.out_rate_per_second,
    status: dto.status,
    subscriptionsActive: dto.subscriptions_active,
  };
}

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

export function mapQueueRealm(dto: QueueRealmEntry): QueueRealmSummary {
  return {
    ...mapQueueOperational(dto),
    areaCount: dto.area_count,
    queueCount: dto.queue_count,
    realm: dto.realm,
  };
}

export function mapQueueArea(dto: QueueAreaEntry): QueueAreaSummary {
  return {
    ...mapQueueOperational(dto),
    area: dto.area,
    queueCount: dto.queue_count,
    realm: dto.realm,
  };
}

export function mapQueueResource(dto: QueueResourceEntry): QueueResourceSummary {
  return {
    ...mapQueueOperational(dto),
    area: dto.area,
    familyCount: dto.family_count,
    realm: dto.realm,
    resource: dto.resource,
  };
}

export function mapQueueRealmDetail(dto: QueueRealmDetailDto): QueueRealmDetail {
  return {
    ...mapQueueOperational(dto),
    areaCount: dto.area_count,
    areas: dto.areas.map(mapQueueArea),
    queueCount: dto.queue_count,
    queues: dto.queues.map(mapQueueResource),
    realm: dto.realm,
  };
}

export function mapQueueAreaDetail(dto: QueueAreaDetailDto): QueueAreaDetail {
  return {
    ...mapQueueOperational(dto),
    area: dto.area,
    queueCount: dto.queue_count,
    queues: dto.queues.map(mapQueueResource),
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

export function mapQueueOverview(realms: QueueRealmEntry[], stats: QueueStats): QueueOverview {
  return {
    realms: realms.map(mapQueueRealm),
    stats: mapQueueStats(stats),
  };
}
