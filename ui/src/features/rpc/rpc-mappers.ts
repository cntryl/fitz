import type { RealmEntry, RpcStats } from "@/adapters";
import type { RpcOverview, RpcRealmSummary, RpcStatsSummary } from "./rpc-models";

export function mapRpcRealm(dto: RealmEntry): RpcRealmSummary {
  return {
    realm: dto.realm,
  };
}

export function mapRpcStats(dto: RpcStats): RpcStatsSummary {
  return {
    invalidSequenceErrorsDroppedTotal: dto.invalid_sequence_errors_dropped_total,
    invalidSequenceErrorsForwardedTotal: dto.invalid_sequence_errors_forwarded_total,
    invalidSequenceResponsesTotal: dto.invalid_sequence_responses_total,
    operationsPerSecond: dto.operations_per_second,
    requestsPending: dto.requests_pending,
    responsesDroppedClosedCallerTotal: dto.responses_dropped_closed_caller_total,
    responsesMissingPendingTotal: dto.responses_missing_pending_total,
    workersRegistered: dto.workers_registered,
  };
}

export function mapRpcOverview(realms: RealmEntry[], stats: RpcStats): RpcOverview {
  return {
    realms: realms.map(mapRpcRealm),
    stats: mapRpcStats(stats),
  };
}
