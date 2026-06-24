import { apiv1 } from "@/adapters";
import type { RpcCallObservationList } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapRpcOverview } from "./rpc-mappers";
import type { RpcCallSearchRequest, RpcOverview } from "./rpc-models";

async function getOverview(options: ServiceRequestOptions = {}): Promise<RpcOverview> {
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listRpcRealms(options),
    apiv1.getRpcStats(options),
  ]);

  return mapRpcOverview(
    unwrapResponse(realmsResponse, "Unable to load RPC realms").realms,
    unwrapResponse(statsResponse, "Unable to load RPC statistics"),
  );
}

async function searchCalls(
  request: RpcCallSearchRequest,
  options: ServiceRequestOptions = {},
): Promise<RpcCallObservationList> {
  return unwrapResponse(
    await apiv1.searchRpcCalls(
      {
        area: request.area,
        correlation_id: request.correlationId,
        limit: request.limit,
        operation: request.operation,
        q: request.query,
        realm: request.realm,
        resource: request.resource,
        route_family: request.routeFamily,
      },
      options,
    ),
    "Unable to search RPC call evidence",
  );
}

export const rpcService = {
  getOverview,
  searchCalls,
};
