export interface RpcRealmSummary {
  realm: string;
}

export interface RpcStatsSummary {
  invalidSequenceErrorsDroppedTotal: number;
  invalidSequenceErrorsForwardedTotal: number;
  invalidSequenceResponsesTotal: number;
  failureTotal: number;
  operationsPerSecond: number;
  requestsPending: number;
  pendingRoutesActive: number;
  responsesDroppedClosedCallerTotal: number;
  responsesMissingPendingTotal: number;
  requestTimeoutsTotal: number;
  workersRegistered: number;
}

export interface RpcOverview {
  realms: RpcRealmSummary[];
  stats: RpcStatsSummary;
}

export interface RpcCallSearchRequest {
  area?: string;
  correlationId?: string;
  limit?: number;
  operation?: string;
  query?: string;
  realm?: string;
  resource?: string;
  routeFamily: number;
}
