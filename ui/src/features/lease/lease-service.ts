import { apiv1 } from "@/adapters";
import type { LeaseSearchResponse } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapLeaseOverview } from "./lease-mappers";
import type { LeaseOverview, LeaseSearchRequest } from "./lease-models";

async function getOverview(options: ServiceRequestOptions = {}): Promise<LeaseOverview> {
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listLeaseRealms(options),
    apiv1.getLeaseStats(options),
  ]);

  return mapLeaseOverview(
    unwrapResponse(realmsResponse, "Unable to load lease realms").realms,
    unwrapResponse(statsResponse, "Unable to load lease statistics"),
  );
}

async function searchOwnership(
  request: LeaseSearchRequest,
  options: ServiceRequestOptions = {},
): Promise<LeaseSearchResponse> {
  return unwrapResponse(
    await apiv1.searchLeaseOwnership(
      {
        area: request.area,
        limit: request.limit,
        owner: request.owner,
        realm: request.realm,
        resource: request.resource,
        route_family: request.routeFamily,
        state: request.state,
      },
      options,
    ),
    "Unable to search lease ownership",
  );
}

export const leaseService = {
  getOverview,
  searchOwnership,
};
