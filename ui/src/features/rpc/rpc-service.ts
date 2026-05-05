import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapRpcOverview } from "./rpc-mappers";
import type { RpcOverview } from "./rpc-models";

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

export const rpcService = {
  getOverview,
};
