export interface ScheduleRealmSummary {
  realm: string;
}

export interface ScheduleStatsSummary {
  ackFailuresTotal: number;
  cancelPersistenceFailuresTotal: number;
  createPersistenceFailuresTotal: number;
  executionsPerMinute: number;
  notifyFailuresTotal: number;
  overdueNormalizationsTotal: number;
  pendingFireClaims: number;
  schedulesActive: number;
  subscriptionsActive: number;
  upsertPersistenceFailuresTotal: number;
}

export interface ScheduleOverview {
  realms: ScheduleRealmSummary[];
  stats: ScheduleStatsSummary;
}
