import { apiQuery, apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapAdminSearchResponse } from "./search-mappers";
import type { AdminSearchRequest, AdminSearchResults } from "./search-models";

function routeFamilyParam(routeFamily: string | undefined) {
  return routeFamily || undefined;
}

async function searchAdminState(
  request: AdminSearchRequest,
  options: ServiceRequestOptions = {},
): Promise<AdminSearchResults> {
  const response = await apiv1.searchAdminState(
    apiQuery(
      {
        area: request.area,
        domain: request.domain,
        limit: request.limit,
        operation: request.operation,
        q: request.query,
        realm: request.realm,
        resource: request.resource,
        route_family: routeFamilyParam(request.routeFamily),
      },
      options,
    ),
  );

  return mapAdminSearchResponse(unwrapResponse(response, "Unable to search admin state"));
}

export const searchService = {
  searchAdminState,
};
