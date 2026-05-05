import type { RealmEntry, ScheduleStats } from "@/adapters";
import type {
  ScheduleOverview,
  ScheduleRealmSummary,
  ScheduleStatsSummary,
} from "./schedule-models";

export function mapScheduleRealm(dto: RealmEntry): ScheduleRealmSummary {
  return {
    realm: dto.realm,
  };
}

export function mapScheduleStats(dto: ScheduleStats): ScheduleStatsSummary {
  return {
    ackFailuresTotal: dto.ack_failures_total,
    executionsPerMinute: dto.executions_per_minute,
    notifyFailuresTotal: dto.notify_failures_total,
    overdueNormalizationsTotal: dto.overdue_normalizations_total,
    pendingFireClaims: dto.pending_fire_claims,
    schedulesActive: dto.schedules_active,
    subscriptionsActive: dto.subscriptions_active,
  };
}

export function mapScheduleOverview(realms: RealmEntry[], stats: ScheduleStats): ScheduleOverview {
  return {
    realms: realms.map(mapScheduleRealm),
    stats: mapScheduleStats(stats),
  };
}
