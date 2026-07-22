import type {
  NoticeDeliveryObservation,
  NoticeDeliveryObservationList,
  NoticeStats,
  RealmEntry,
} from "@/adapters";
import type { ResourceEntry } from "@/adapters";
import type {
  NoticeDeliveryRows,
  NoticeOverview,
  NoticeResourceOperationRows,
  NoticeRealmAreaSummary,
  NoticeRealmInventory,
  NoticeRealmSummary,
  NoticeStatsSummary,
  NoticeAreaResourceRows,
  NoticeResourceOperationRow,
} from "./notice-models";

export function mapNoticeRealm(dto: RealmEntry): NoticeRealmSummary {
  return {
    realm: dto.realm,
  };
}

export function mapNoticeRealmAreaSummary(
  realm: string,
  area: string,
  resources: ResourceEntry[],
): NoticeRealmAreaSummary {
  return {
    area,
    realm,
    resources: resources.map((entry) => entry.resource),
  };
}

export function mapNoticeRealmInventory(
  realm: string,
  areas: NoticeRealmAreaSummary[],
): NoticeRealmInventory {
  return {
    realm,
    areas,
  };
}

export function mapNoticeAreaResourceRows(
  realm: string,
  area: string,
  resources: ResourceEntry[],
): NoticeAreaResourceRows {
  return {
    area,
    realm,
    resources: resources.map((entry) => entry.resource),
  };
}

export function mapNoticeStats(dto: NoticeStats): NoticeStatsSummary {
  return {
    publishesPerSecond: dto.publishes_per_second,
    deliveryDropsTotal: dto.delivery_drops_total,
    routesActive: dto.routes_active,
    wildcardLimitRejectsTotal: dto.wildcard_limit_rejects_total,
    subscriptionsActive: dto.subscriptions_active,
    maxRouteSubscribers: dto.max_route_subscribers,
  };
}

export function mapNoticeOverview(realms: RealmEntry[], stats: NoticeStats): NoticeOverview {
  return {
    realms: realms.map(mapNoticeRealm),
    stats: mapNoticeStats(stats),
  };
}

export function mapNoticeDeliveryRow(
  dto: NoticeDeliveryObservation,
): NoticeDeliveryRows["observations"][number] {
  return {
    area: dto.area ?? "",
    notificationsReceived: dto.notifications_received,
    publishesPerMinute: dto.publishes_per_minute,
    publishesTotal: dto.publishes_total,
    realm: dto.realm,
    resource: dto.resource ?? "",
    route: dto.route,
    sessionId: dto.session_id,
    status: dto.status,
    subscriptionId: dto.subscription_id,
  };
}

export function mapNoticeDeliveryRows(dto: NoticeDeliveryObservationList): NoticeDeliveryRows {
  return {
    area: dto.observations[0]?.area ?? "",
    limit: dto.limit,
    observations: dto.observations.map(mapNoticeDeliveryRow),
    realm: dto.observations[0]?.realm ?? "",
    routeFamily: dto.route_family,
  };
}

export function mapNoticeResourceOperationRows(
  dto: NoticeDeliveryObservationList,
): NoticeResourceOperationRows {
  const aggregation = new Map<string, NoticeResourceOperationRow & { subscribers: Set<string> }>();

  for (const observation of dto.observations) {
    const bucket = aggregation.get(observation.route);

    if (!bucket) {
      aggregation.set(observation.route, {
        operation: observation.route,
        activeSubscribers: 0,
        rollingMessageCount: 0,
        latencyMs: null,
        subscribers: new Set<string>(),
      });
    }

    const next = aggregation.get(observation.route);
    if (!next) {
      continue;
    }

    // Publish rate is route-scoped and repeated on every subscription observation.
    // Keep one route value instead of multiplying it by the subscriber count.
    next.rollingMessageCount = Math.max(next.rollingMessageCount, observation.publishes_per_minute);

    if (observation.subscription_id !== null || observation.session_id !== null) {
      next.subscribers.add(
        `${observation.subscription_id ?? "session"}:${observation.session_id ?? "session"}`,
      );
    }
  }

  const operations = Array.from(aggregation.values())
    .map((row) => ({
      operation: row.operation,
      activeSubscribers: row.subscribers.size,
      rollingMessageCount: row.rollingMessageCount,
      latencyMs: row.latencyMs,
    }))
    .sort((left, right) => left.operation.localeCompare(right.operation));

  return {
    area: dto.observations[0]?.area ?? "",
    limit: dto.limit,
    operations,
    realm: dto.observations[0]?.realm ?? "",
    resource: dto.observations[0]?.resource ?? "",
    routeFamily: dto.route_family,
  };
}
