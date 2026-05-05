import type { RealmEntry, RpcStats } from "@/adapters";
import type { RpcOverview, RpcRealmSummary, RpcStatsSummary } from "./rpc-models";

export function mapRpcRealm(dto: RealmEntry): RpcRealmSummary {
  return {
    realm: dto.realm,
  };
}

export function mapRpcStats(dto: RpcStats): RpcStatsSummary {
  return {
    operationsPerSecond: dto.operations_per_second,
    requestsPending: dto.requests_pending,
    workersRegistered: dto.workers_registered,
  };
}

export function mapRpcOverview(realms: RealmEntry[], stats: RpcStats): RpcOverview {
  return {
    realms: realms.map(mapRpcRealm),
    stats: mapRpcStats(stats),
  };
}
