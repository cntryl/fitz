import { apiParams, apiParamsQuery, apiv1 } from "@/adapters";
import type { RpcCallObservationList, RpcOperationDetail } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "@/shared/navigation/domains";
import {
  routeFamilyRequest,
  type RouteFamilyRequestOptions,
} from "@/shared/navigation/route-family-request";
import { mapRpcOverview } from "./rpc-mappers";
import type {
  RpcAreaInventory,
  RpcCallSearchRequest,
  RpcOperationView,
  RpcOverview,
  RpcResourceInventory,
  RpcResourceOperationRows,
} from "./rpc-models";

async function getOverview(options: RouteFamilyRequestOptions = {}): Promise<RpcOverview> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listRpcRealms(apiParams({ family }, requestOptions)),
    apiv1.getRpcStats(apiParams({ family }, requestOptions)),
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
      apiParamsQuery(
        { family: apiRouteFamilySegment(request.routeFamily) },
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
    ),
    "Unable to search RPC call evidence",
  );
}

async function listRpcAreas(
  realm: string,
  options: RouteFamilyRequestOptions = {},
): Promise<RpcAreaInventory> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const areas = unwrapResponse(
    await apiv1.listRpcAreas(apiParams({ family, realm }, requestOptions)),
    "Unable to load RPC areas",
  ).areas;
  const rows = await Promise.all(
    areas.map(async ({ area }) => {
      const resources = unwrapResponse(
        await apiv1.listRpcResources(apiParams({ area, family, realm }, requestOptions)),
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
  options: RouteFamilyRequestOptions = {},
): Promise<RpcResourceInventory> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const resources = unwrapResponse(
    await apiv1.listRpcResources(apiParams({ area, family, realm }, requestOptions)),
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
  options: RouteFamilyRequestOptions = {},
): Promise<RpcResourceOperationRows> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const operations = unwrapResponse(
    await apiv1.getRpcResource(apiParams({ area, family, realm, resource }, requestOptions)),
    "Unable to load RPC resource",
  );
  const calls = unwrapResponse(
    await apiv1.searchRpcCalls(
      apiParamsQuery({ family }, { area, limit: 200, realm, resource }, requestOptions),
    ),
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
  options: RouteFamilyRequestOptions = {},
): Promise<RpcOperationDetail> {
  const { family, requestOptions } = routeFamilyRequest(options);
  return unwrapResponse(
    await apiv1.getRpcOperation(
      apiParams({ area, family, operation, realm, resource }, requestOptions),
    ),
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
    getOperation(request.realm, request.area, request.resource, request.operation, {
      ...options,
      routeFamily,
    }),
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
