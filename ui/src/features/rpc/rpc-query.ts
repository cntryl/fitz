import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
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

const rpcOverviewQuery = defineQuery<{ family: string }, RpcOverview>({
  key: ({ family }) => rpcQueries.key("overview", family),
  fetch: ({ signal }) => rpcService.getOverview({ signal }),
});

const rpcRealmQuery = defineQuery<{ family: string; realm: string }, RpcAreaInventory>({
  key: ({ family, realm }) => rpcRealmQueryKey(realm, family),
  fetch: ({ realm, signal }) => rpcService.listRpcAreas(realm, { signal }),
});

const rpcAreaQuery = defineQuery<
  { area: string; family: string; realm: string },
  RpcResourceInventory
>({
  key: ({ area, family, realm }) => rpcAreaQueryKey(realm, area, family),
  fetch: ({ area, realm, signal }) => rpcService.listRpcResources(realm, area, { signal }),
});

interface RpcResourceQueryInput {
  area: string;
  family: string;
  realm: string;
  resource: string;
}

const rpcResourceQuery = defineQuery<RpcResourceQueryInput, RpcResourceOperationRows>({
  key: ({ area, family, realm, resource }) => rpcResourceQueryKey(realm, area, resource, family),
  fetch: ({ area, realm, resource, signal }) =>
    rpcService.getResourceOperations(realm, area, resource, { signal }),
});

interface RpcOperationQueryInput extends RpcResourceQueryInput {
  limit: number;
  operation: string;
}

const rpcOperationQuery = defineQuery<RpcOperationQueryInput, RpcOperationView>({
  key: ({ area, family, limit, operation, realm, resource }) =>
    rpcOperationQueryKey(realm, area, resource, operation, limit, family),
  fetch: ({ area, family, limit, operation, realm, resource, signal }) =>
    rpcService.getOperationView(
      { area, limit, operation, realm, resource, routeFamily: family },
      { signal },
    ),
});

export function createRpcOverviewQuery() {
  return createQuery(rpcOverviewQuery, { family: currentRouteFamilySegment() });
}

export function createRpcRealmQuery(realm: string) {
  return createQuery(rpcRealmQuery, { family: currentRouteFamilySegment(), realm });
}

export function createRpcAreaQuery(realm: string, area: string) {
  return createQuery(rpcAreaQuery, { area, family: currentRouteFamilySegment(), realm });
}

export function createRpcResourceQuery(realm: string, area: string, resource: string) {
  return createQuery(rpcResourceQuery, {
    area,
    family: currentRouteFamilySegment(),
    realm,
    resource,
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
  return createQuery(rpcOperationQuery, {
    ...request,
    family: currentRouteFamilySegment(),
    limit,
  });
}
