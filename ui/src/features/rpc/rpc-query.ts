import { createQuery, queryScope } from "@askrjs/askr/data";
import { rpcService } from "./rpc-service";
import type { RpcOverview } from "./rpc-models";

const rpcQueries = queryScope("rpc");

const RPC_OVERVIEW_KEY = rpcQueries.key("overview");

export function createRpcOverviewQuery() {
  return createQuery<RpcOverview>({
    key: RPC_OVERVIEW_KEY,
    fetch: rpcService.getOverview,
  });
}
