import { apiParams, apiParamsQuery, apiv1 } from "@/adapters";
import type { LeaseSearchResponse } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "@/shared/navigation/domains";
import {
  routeFamilyRequest,
  type RouteFamilyRequestOptions,
} from "@/shared/navigation/route-family-request";
import {
  mapLeaseAreaResourceRows,
  mapLeaseAreaSummary,
  mapLeaseOverview,
  mapLeaseOwnershipSearchResult,
  mapLeaseRealmInventory,
} from "./lease-mappers";
import type {
  LeaseAreaResourceRows,
  LeaseOwnershipSearchRequest,
  LeaseOwnershipSearchResult,
  LeaseRealmInventory,
  LeaseOverview,
} from "./lease-models";

const INVENTORY_CONCURRENCY = 4;

async function mapWithConcurrency<T, R>(
  items: T[],
  worker: (item: T) => Promise<R>,
  concurrency = 4,
): Promise<R[]> {
  const results = Array.from<R | undefined>({ length: items.length });
  let index = 0;

  async function run() {
    const current = index++;

    if (current >= items.length) {
      return;
    }

    results[current] = await worker(items[current]);
    await run();
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => run()));

  return results as R[];
}

async function getOverview(options: RouteFamilyRequestOptions = {}): Promise<LeaseOverview> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listLeaseRealms(apiParams({ family }, requestOptions)),
    apiv1.getLeaseStats(apiParams({ family }, requestOptions)),
  ]);

  return mapLeaseOverview(
    unwrapResponse(realmsResponse, "Unable to load lease realms").realms,
    unwrapResponse(statsResponse, "Unable to load lease statistics"),
  );
}

async function listRealmResources(
  realm: string,
  options: RouteFamilyRequestOptions = {},
): Promise<LeaseRealmInventory> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const areaEntries = unwrapResponse(
    await apiv1.listLeaseAreas(apiParams({ family, realm }, requestOptions)),
    `Unable to load lease areas for ${realm}`,
  ).areas;

  const areaSummaries = await mapWithConcurrency(
    areaEntries,
    async ({ area }) =>
      mapLeaseAreaSummary(
        realm,
        area,
        unwrapResponse(
          await apiv1.listLeaseResources(apiParams({ area, family, realm }, requestOptions)),
          `Unable to load lease resources for ${realm}/${area}`,
        ).resources,
      ),
    INVENTORY_CONCURRENCY,
  );

  return mapLeaseRealmInventory(realm, areaSummaries);
}

async function listAreaResources(
  realm: string,
  area: string,
  options: RouteFamilyRequestOptions = {},
): Promise<LeaseAreaResourceRows> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const resources = unwrapResponse(
    await apiv1.listLeaseResources(apiParams({ area, family, realm }, requestOptions)),
    `Unable to load lease resources for ${realm}/${area}`,
  ).resources;

  return mapLeaseAreaResourceRows(realm, area, resources);
}

async function searchRows(
  request: LeaseOwnershipSearchRequest,
  options: ServiceRequestOptions = {},
): Promise<LeaseOwnershipSearchResult> {
  const response = unwrapResponse(
    await apiv1.searchLeaseOwnership(
      apiParamsQuery(
        { family: apiRouteFamilySegment(request.routeFamily) },
        {
          area: request.area,
          limit: request.limit,
          owner: request.owner,
          realm: request.realm,
          resource: request.resource,
          state: request.state,
        },
        options,
      ),
    ),
    "Unable to search lease ownership",
  );

  return mapLeaseOwnershipSearchResult(response, Date.now());
}

async function searchOwnership(
  request: LeaseOwnershipSearchRequest,
  options: ServiceRequestOptions = {},
): Promise<LeaseSearchResponse> {
  const response = await apiv1.searchLeaseOwnership(
    apiParamsQuery(
      { family: apiRouteFamilySegment(request.routeFamily) },
      {
        area: request.area,
        limit: request.limit,
        owner: request.owner,
        realm: request.realm,
        resource: request.resource,
        state: request.state,
      },
      options,
    ),
  );

  return unwrapResponse(response, "Unable to search lease ownership");
}

export const leaseService = {
  getOverview,
  listAreaResources,
  listRealmResources,
  searchOwnership,
  searchRows,
};
