import { createQuery } from "@askrjs/askr/data";
import { rpcService } from "./rpc-service";
import type { RpcOverview } from "./rpc-models";

const RPC_OVERVIEW_KEY = "rpc:overview";

export function createRpcOverviewQuery() {
  return createQuery<RpcOverview>({
    key: RPC_OVERVIEW_KEY,
    fetch: ({ signal }) => rpcService.getOverview({ signal }),
  });
}
