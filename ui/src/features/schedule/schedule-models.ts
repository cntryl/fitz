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

export interface ScheduleRealmInventory {
  areas: Array<{
    area: string;
    resources: string[];
  }>;
  realm: string;
  resourceCount: number;
}

export interface ScheduleAreaInventory {
  area: string;
  realm: string;
  resources: string[];
  resourceCount: number;
}

export interface ScheduleResourceView {
  detail: import("@/adapters").ScheduleResourceDetail;
  executionObservations: import("@/adapters").ScheduleExecutionObservationList;
}

export interface ScheduleOperationView {
  executionObservations: import("@/adapters").ScheduleExecutionObservationList;
  missedHandoffs: import("@/adapters").ScheduleMissedObservationList;
}

export interface ScheduleExecutionObservationRequest {
  area: string;
  limit?: number;
  offset?: number;
  operation?: string;
  realm: string;
  resource: string;
  routeFamily: number | string;
}

export interface ScheduleMissedObservationRequest {
  area?: string;
  limit?: number;
  operation?: string;
  realm?: string;
  resource?: string;
  routeFamily: number | string;
}
