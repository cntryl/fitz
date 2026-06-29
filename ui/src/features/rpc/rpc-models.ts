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

export interface RpcAreaInventory {
  areas: Array<{
    area: string;
    realm: string;
    resources: string[];
  }>;
  realm: string;
}

export interface RpcResourceInventory {
  area: string;
  realm: string;
  resources: string[];
}

export interface RpcResourceOperationRows {
  area: string;
  operations: Array<{
    averageLatencyMs: number | null;
    operation: string;
    pendingRequests: number;
    requestsHandled: number;
    workers: number;
  }>;
  realm: string;
  resource: string;
}

export interface RpcOperationView {
  calls: import("@/adapters").RpcCallObservationList;
  detail: import("@/adapters").RpcOperationDetail;
}

export interface RpcCallSearchRequest {
  area?: string;
  correlationId?: string;
  limit?: number;
  operation?: string;
  query?: string;
  realm?: string;
  resource?: string;
  routeFamily: number | string;
}
