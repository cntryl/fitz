export interface ScheduleRealmSummary {
  realm: string;
}

export interface ScheduleStatsSummary {
  ackFailuresTotal: number;
  executionsPerMinute: number;
  notifyFailuresTotal: number;
  overdueNormalizationsTotal: number;
  pendingFireClaims: number;
  schedulesActive: number;
  subscriptionsActive: number;
}

export interface ScheduleOverview {
  realms: ScheduleRealmSummary[];
  stats: ScheduleStatsSummary;
}
