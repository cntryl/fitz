import { apiv1 } from "@/adapters";
import type { RpcCallObservationList, RpcOperationDetail } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "@/shared/navigation/domains";
import { mapRpcOverview } from "./rpc-mappers";
import type {
  RpcAreaInventory,
  RpcCallSearchRequest,
  RpcOperationView,
  RpcOverview,
  RpcResourceInventory,
  RpcResourceOperationRows,
} from "./rpc-models";

async function getOverview(options: ServiceRequestOptions = {}): Promise<RpcOverview> {
  const family = apiRouteFamilySegment();
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listRpcRealms(family, options),
    apiv1.getRpcStats(family, options),
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
      apiRouteFamilySegment(request.routeFamily),
      {
        area: request.area,
        correlation_id: request.correlationId,
        limit: request.limit,
        operation: request.operation,
        q: request.query,
        realm: request.realm,
        resource: request.resource,
      },
      options,
    ),
    "Unable to search RPC call evidence",
  );
}

async function listRpcAreas(
  realm: string,
  options: ServiceRequestOptions = {},
): Promise<RpcAreaInventory> {
  const family = apiRouteFamilySegment();
  const areas = unwrapResponse(
    await apiv1.listRpcAreas(family, realm, options),
    "Unable to load RPC areas",
  ).areas;
  const rows = await Promise.all(
    areas.map(async ({ area }) => {
      const resources = unwrapResponse(
        await apiv1.listRpcResources(family, realm, area, options),
        "Unable to load RPC resources",
      ).resources;

      return {
        area,
        realm,
        resources: resources.map((entry) => entry.resource),
      };
    }),
  );

  return { areas: rows, realm };
}

async function listRpcResources(
  realm: string,
  area: string,
  options: ServiceRequestOptions = {},
): Promise<RpcResourceInventory> {
  const resources = unwrapResponse(
    await apiv1.listRpcResources(apiRouteFamilySegment(), realm, area, options),
    "Unable to load RPC resources",
  ).resources;

  return {
    area,
    realm,
    resources: resources.map((entry) => entry.resource),
  };
}

async function getResourceOperations(
  realm: string,
  area: string,
  resource: string,
  options: ServiceRequestOptions = {},
): Promise<RpcResourceOperationRows> {
  const family = apiRouteFamilySegment();
  const operations = unwrapResponse(
    await apiv1.getRpcResource(family, realm, area, resource, options),
    "Unable to load RPC resource",
  );
  const calls = unwrapResponse(
    await apiv1.searchRpcCalls(family, { area, limit: 200, realm, resource }, options),
    "Unable to load RPC call evidence",
  ).observations;

  return {
    area,
    operations: operations.operations.map(({ operation }) => {
      const rows = calls.filter((row) => row.operation === operation);
      const workers = rows.filter((row) => row.state === "worker_registered");
      const pending = rows.filter((row) => row.state === "pending");

      return {
        averageLatencyMs:
          workers.length === 0
            ? null
            : Math.max(...workers.map((row) => row.average_latency_ms ?? 0)),
        operation,
        pendingRequests: pending.length,
        requestsHandled: workers.reduce((sum, row) => sum + (row.requests_handled ?? 0), 0),
        workers: workers.length,
      };
    }),
    realm,
    resource,
  };
}

async function getOperation(
  realm: string,
  area: string,
  resource: string,
  operation: string,
  options: ServiceRequestOptions = {},
): Promise<RpcOperationDetail> {
  return unwrapResponse(
    await apiv1.getRpcOperation(apiRouteFamilySegment(), realm, area, resource, operation, options),
    "Unable to load RPC operation",
  );
}

async function getOperationView(
  request: Required<Pick<RpcCallSearchRequest, "area" | "operation" | "realm" | "resource">> &
    Pick<RpcCallSearchRequest, "limit" | "routeFamily">,
  options: ServiceRequestOptions = {},
): Promise<RpcOperationView> {
  const routeFamily = request.routeFamily;
  const [detail, calls] = await Promise.all([
    getOperation(request.realm, request.area, request.resource, request.operation, options),
    searchCalls(
      {
        area: request.area,
        limit: request.limit,
        operation: request.operation,
        realm: request.realm,
        resource: request.resource,
        routeFamily,
      },
      options,
    ),
  ]);

  return { calls, detail };
}

export const rpcService = {
  getOperation,
  getOperationView,
  getOverview,
  getResourceOperations,
  listRpcAreas,
  listRpcResources,
  searchCalls,
};
