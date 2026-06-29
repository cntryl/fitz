export interface StreamRealmSummary {
  realm: string;
}

export interface StreamLagBucketsSummary {
  caughtUp: number;
  over100: number;
  under10: number;
  under100: number;
}

export interface StreamStatsSummary {
  eventsTotal: number;
  operationsPerSecond: number;
  watermarkLagBuckets: StreamLagBucketsSummary;
  streamsActive: number;
  subscriptionsActive: number;
}

export interface StreamOverview {
  realms: StreamRealmSummary[];
  stats: StreamStatsSummary;
}

export interface StreamRealmRollup {
  areaCount: number;
  areas: Array<{
    area: string;
    resources: string[];
  }>;
  familyWatermarks: Array<{
    family: number;
    watermark: number;
  }>;
  realm: string;
  resourceCount: number;
}

export interface StreamAreaRollup {
  area: string;
  familyWatermarks: Array<{
    family: number;
    watermark: number;
  }>;
  realm: string;
  resourceCount: number;
  resources: string[];
}

export interface StreamResourceView {
  detail: import("@/adapters").StreamResourceDetail;
  records: import("@/adapters").StreamRecordsResponse;
}

export interface StreamRecordSearchRequest {
  area?: string;
  discriminator?: string;
  fromOffset?: number;
  limit?: number;
  realm?: string;
  resource?: string;
  routeFamily: number | string;
}
