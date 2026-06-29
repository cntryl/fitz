import { apiv1 } from "@/adapters";
import type { LeaseSearchResponse } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "@/shared/navigation/domains";
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

async function getOverview(options: ServiceRequestOptions = {}): Promise<LeaseOverview> {
  const family = apiRouteFamilySegment();
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listLeaseRealms(family, options),
    apiv1.getLeaseStats(family, options),
  ]);

  return mapLeaseOverview(
    unwrapResponse(realmsResponse, "Unable to load lease realms").realms,
    unwrapResponse(statsResponse, "Unable to load lease statistics"),
  );
}

async function listRealmResources(
  realm: string,
  options: ServiceRequestOptions = {},
): Promise<LeaseRealmInventory> {
  const family = apiRouteFamilySegment();
  const areaEntries = unwrapResponse(
    await apiv1.listLeaseAreas(family, realm, options),
    `Unable to load lease areas for ${realm}`,
  ).areas;

  const areaSummaries = await mapWithConcurrency(
    areaEntries,
    async ({ area }) =>
      mapLeaseAreaSummary(
        realm,
        area,
        unwrapResponse(
          await apiv1.listLeaseResources(family, realm, area, options),
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
  options: ServiceRequestOptions = {},
): Promise<LeaseAreaResourceRows> {
  const family = apiRouteFamilySegment();
  const resources = unwrapResponse(
    await apiv1.listLeaseResources(family, realm, area, options),
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
      apiRouteFamilySegment(request.routeFamily),
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
    "Unable to search lease ownership",
  );

  return mapLeaseOwnershipSearchResult(response, Date.now());
}

async function searchOwnership(
  request: LeaseOwnershipSearchRequest,
  options: ServiceRequestOptions = {},
): Promise<LeaseSearchResponse> {
  const response = await apiv1.searchLeaseOwnership(
    apiRouteFamilySegment(request.routeFamily),
    {
      area: request.area,
      limit: request.limit,
      owner: request.owner,
      realm: request.realm,
      resource: request.resource,
      state: request.state,
    },
    options,
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
