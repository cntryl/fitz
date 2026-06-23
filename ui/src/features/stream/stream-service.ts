import { apiv1 } from "@/adapters";
import type { StreamRecordsResponse } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapStreamOverview } from "./stream-mappers";
import type { StreamOverview, StreamRecordSearchRequest } from "./stream-models";

async function getOverview(options: ServiceRequestOptions = {}): Promise<StreamOverview> {
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listStreamRealms(options),
    apiv1.getStreamStats(options),
  ]);

  return mapStreamOverview(
    unwrapResponse(realmsResponse, "Unable to load stream realms").realms,
    unwrapResponse(statsResponse, "Unable to load stream statistics"),
  );
}

async function searchRecords(
  request: StreamRecordSearchRequest,
  options: ServiceRequestOptions = {},
): Promise<StreamRecordsResponse> {
  return unwrapResponse(
    await apiv1.searchStreamRecords(
      {
        area: request.area,
        discriminator: request.discriminator,
        from_offset: request.fromOffset,
        limit: request.limit,
        realm: request.realm,
        resource: request.resource,
        route_family: request.routeFamily,
      },
      options,
    ),
    "Unable to search stream records",
  );
}

async function readResourceRecords(
  request: Required<Pick<StreamRecordSearchRequest, "area" | "realm" | "resource" | "routeFamily">> &
    Pick<StreamRecordSearchRequest, "discriminator" | "fromOffset" | "limit">,
  options: ServiceRequestOptions = {},
): Promise<StreamRecordsResponse> {
  return unwrapResponse(
    await apiv1.readStreamResourceRecords(
      request.realm,
      request.area,
      request.resource,
      {
        discriminator: request.discriminator,
        from_offset: request.fromOffset,
        limit: request.limit,
        route_family: request.routeFamily,
      },
      options,
    ),
    "Unable to read stream records",
  );
}

export const streamService = {
  getOverview,
  readResourceRecords,
  searchRecords,
};
