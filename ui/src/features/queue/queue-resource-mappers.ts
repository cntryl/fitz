import type {
  QueueDeadLetter,
  QueueInflight,
  QueueResourceDetail as QueueResourceDetailDto,
  ResourceComparison,
  ResourceTimeline,
  ResourceTimelineEvent,
} from "@/adapters";
import { mapQueueDeadLetter } from "./queue-mappers";
import type {
  QueueResourceComparison,
  QueueResourceComparisonMetrics,
  QueueResourceComparisonScope,
  QueueResourceComparisonSide,
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
    completeSuccessTotal: dto.complete_success_total,
    enqueueSuccessTotal: dto.enqueue_success_total,
    inRatePerSecond: dto.in_rate_per_second,
    messagesReady: dto.messages_ready,
    messagesInflight: dto.messages_inflight,
    messagesDelayed: dto.messages_delayed,
    messagesDeadLettered: dto.messages_dead_lettered,
    messagesTotal: dto.messages_total,
    oldestBacklogAgeSeconds: dto.oldest_backlog_age_seconds,
    oldestMessageAgeSeconds: dto.oldest_message_age_seconds,
    outRatePerSecond: dto.out_rate_per_second,
    status: dto.status,
    subscriptionsActive: dto.subscriptions_active,
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

function mapQueueResourceComparisonScope(
  scope: ResourceComparison["left"]["scope"],
): QueueResourceComparisonScope {
  return {
    area: scope.area,
    family: scope.family ?? null,
    realm: scope.realm,
    resource: scope.resource,
  };
}

function mapQueueResourceComparisonMetrics(
  metrics: ResourceComparison["left"]["metrics"],
): QueueResourceComparisonMetrics {
  return {
    ageSeconds: metrics.age_seconds ?? null,
    backlog: metrics.backlog ?? null,
    deadLetters: metrics.dead_letters ?? null,
    delayed: metrics.delayed ?? null,
    inflight: metrics.inflight ?? null,
    ready: metrics.ready ?? null,
    recentTransitionCount: metrics.recent_transition_count ?? null,
    waiters: metrics.waiters ?? null,
  };
}

function mapQueueResourceComparisonSide(
  side: ResourceComparison["left"],
): QueueResourceComparisonSide {
  return {
    metrics: mapQueueResourceComparisonMetrics(side.metrics),
    scope: mapQueueResourceComparisonScope(side.scope),
  };
}

export function mapQueueResourceComparison(dto: ResourceComparison): QueueResourceComparison {
  return {
    comparisonMode: dto.comparison_mode,
    derived: dto.derived,
    delta: mapQueueResourceComparisonMetrics(dto.delta),
    left: mapQueueResourceComparisonSide(dto.left),
    right: mapQueueResourceComparisonSide(dto.right),
    summary: dto.summary,
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
