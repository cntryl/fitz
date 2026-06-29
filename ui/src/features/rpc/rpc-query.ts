import { createQuery, queryScope } from "@askrjs/askr/data";
import { stableQueryFetch, type QueryFetch } from "@/shared/query-fetch";
import { rpcService } from "./rpc-service";
import type {
  RpcAreaInventory,
  RpcOperationView,
  RpcOverview,
  RpcResourceInventory,
  RpcResourceOperationRows,
} from "./rpc-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const rpcQueries = queryScope("rpc");
const rpcRealmFetches = new Map<string, QueryFetch<RpcAreaInventory>>();
const rpcAreaFetches = new Map<string, QueryFetch<RpcResourceInventory>>();
const rpcResourceFetches = new Map<string, QueryFetch<RpcResourceOperationRows>>();
const rpcOperationFetches = new Map<string, QueryFetch<RpcOperationView>>();

export function rpcRealmQueryKey(realm: string, family = currentRouteFamilySegment()) {
  return rpcQueries.key("realm", family, realm);
}

export function rpcAreaQueryKey(realm: string, area: string, family = currentRouteFamilySegment()) {
  return rpcQueries.key("area", family, realm, area);
}

export function rpcResourceQueryKey(
  realm: string,
  area: string,
  resource: string,
  family = currentRouteFamilySegment(),
) {
  return rpcQueries.key("resource", family, realm, area, resource);
}

export function rpcOperationQueryKey(
  realm: string,
  area: string,
  resource: string,
  operation: string,
  limit = 50,
  family = currentRouteFamilySegment(),
) {
  return rpcQueries.key(
    "operation",
    family,
    realm,
    area,
    resource,
    encodeURIComponent(operation),
    String(limit),
  );
}

export function createRpcOverviewQuery() {
  const key = rpcQueries.key("overview", currentRouteFamilySegment());

  return createQuery<RpcOverview>({
    key,
    fetch: rpcService.getOverview,
  });
}

export function createRpcRealmQuery(realm: string) {
  const key = rpcRealmQueryKey(realm);

  return createQuery<RpcAreaInventory>({
    key,
    fetch: stableQueryFetch(
      rpcRealmFetches,
      key,
      () =>
        ({ signal }) =>
          rpcService.listRpcAreas(realm, { signal }),
    ),
  });
}

export function createRpcAreaQuery(realm: string, area: string) {
  const key = rpcAreaQueryKey(realm, area);

  return createQuery<RpcResourceInventory>({
    key,
    fetch: stableQueryFetch(
      rpcAreaFetches,
      key,
      () =>
        ({ signal }) =>
          rpcService.listRpcResources(realm, area, { signal }),
    ),
  });
}

export function createRpcResourceQuery(realm: string, area: string, resource: string) {
  const key = rpcResourceQueryKey(realm, area, resource);

  return createQuery<RpcResourceOperationRows>({
    key,
    fetch: stableQueryFetch(
      rpcResourceFetches,
      key,
      () =>
        ({ signal }) =>
          rpcService.getResourceOperations(realm, area, resource, { signal }),
    ),
  });
}

export function createRpcOperationQuery(request: {
  area: string;
  limit?: number;
  operation: string;
  realm: string;
  resource: string;
}) {
  const limit = request.limit ?? 50;
  const key = rpcOperationQueryKey(
    request.realm,
    request.area,
    request.resource,
    request.operation,
    limit,
  );

  return createQuery<RpcOperationView>({
    key,
    fetch: stableQueryFetch(
      rpcOperationFetches,
      key,
      () =>
        ({ signal }) =>
          rpcService.getOperationView(
            {
              area: request.area,
              limit,
              operation: request.operation,
              realm: request.realm,
              resource: request.resource,
              routeFamily: currentRouteFamilySegment(),
            },
            { signal },
          ),
    ),
  });
}
