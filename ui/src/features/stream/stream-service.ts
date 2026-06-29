import { apiv1 } from "@/adapters";
import type { StreamRecordsResponse } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "@/shared/navigation/domains";
import { mapStreamOverview } from "./stream-mappers";
import type {
  StreamAreaRollup,
  StreamOverview,
  StreamRealmRollup,
  StreamRecordSearchRequest,
  StreamResourceView,
} from "./stream-models";

async function getOverview(options: ServiceRequestOptions = {}): Promise<StreamOverview> {
  const family = apiRouteFamilySegment();
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listStreamRealms(family, options),
    apiv1.getStreamStats(family, options),
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
      apiRouteFamilySegment(request.routeFamily),
      {
        area: request.area,
        discriminator: request.discriminator,
        from_offset: request.fromOffset,
        limit: request.limit,
        realm: request.realm,
        resource: request.resource,
      },
      options,
    ),
    "Unable to search stream records",
  );
}

async function readResourceRecords(
  request: Required<
    Pick<StreamRecordSearchRequest, "area" | "realm" | "resource" | "routeFamily">
  > &
    Pick<StreamRecordSearchRequest, "discriminator" | "fromOffset" | "limit">,
  options: ServiceRequestOptions = {},
): Promise<StreamRecordsResponse> {
  return unwrapResponse(
    await apiv1.readStreamResourceRecords(
      apiRouteFamilySegment(request.routeFamily),
      request.realm,
      request.area,
      request.resource,
      {
        discriminator: request.discriminator,
        from_offset: request.fromOffset,
        limit: request.limit,
      },
      options,
    ),
    "Unable to read stream records",
  );
}

async function getRealmRollup(
  realm: string,
  options: ServiceRequestOptions = {},
): Promise<StreamRealmRollup> {
  const family = apiRouteFamilySegment();
  const [watermarks, areas] = await Promise.all([
    apiv1.getStreamRealmWatermarks(family, realm, options),
    apiv1.listStreamAreas(family, realm, options),
  ]);
  const areaRows = await Promise.all(
    unwrapResponse(areas, "Unable to load stream areas").areas.map(async ({ area }) => {
      const resources = unwrapResponse(
        await apiv1.listStreamResources(family, realm, area, options),
        "Unable to load stream resources",
      ).resources;

      return {
        area,
        resources: resources.map((entry) => entry.resource),
      };
    }),
  );

  const watermarkDetail = unwrapResponse(watermarks, "Unable to load stream realm watermarks");

  return {
    areaCount: watermarkDetail.area_count,
    areas: areaRows,
    familyWatermarks: watermarkDetail.family_watermarks.map((entry) => ({
      family: entry.family,
      watermark: entry.watermark,
    })),
    realm,
    resourceCount: watermarkDetail.resource_count,
  };
}

async function getAreaRollup(
  realm: string,
  area: string,
  options: ServiceRequestOptions = {},
): Promise<StreamAreaRollup> {
  const family = apiRouteFamilySegment();
  const [watermarks, resources] = await Promise.all([
    apiv1.getStreamAreaWatermarks(family, realm, area, options),
    apiv1.listStreamResources(family, realm, area, options),
  ]);
  const watermarkDetail = unwrapResponse(watermarks, "Unable to load stream area watermarks");

  return {
    area,
    familyWatermarks: watermarkDetail.family_watermarks.map((entry) => ({
      family: entry.family,
      watermark: entry.watermark,
    })),
    realm,
    resourceCount: watermarkDetail.resource_count,
    resources: unwrapResponse(resources, "Unable to load stream resources").resources.map(
      (entry) => entry.resource,
    ),
  };
}

async function getResourceView(
  request: Required<
    Pick<StreamRecordSearchRequest, "area" | "realm" | "resource" | "routeFamily">
  > &
    Pick<StreamRecordSearchRequest, "discriminator" | "fromOffset" | "limit">,
  options: ServiceRequestOptions = {},
): Promise<StreamResourceView> {
  const family = apiRouteFamilySegment(request.routeFamily);
  const [detail, records] = await Promise.all([
    apiv1.getStreamResource(family, request.realm, request.area, request.resource, options),
    apiv1.readStreamResourceRecords(
      family,
      request.realm,
      request.area,
      request.resource,
      {
        discriminator: request.discriminator,
        from_offset: request.fromOffset,
        limit: request.limit,
      },
      options,
    ),
  ]);

  return {
    detail: unwrapResponse(detail, "Unable to load stream resource"),
    records: unwrapResponse(records, "Unable to read stream records"),
  };
}

export const streamService = {
  getAreaRollup,
  getOverview,
  getRealmRollup,
  getResourceView,
  readResourceRecords,
  searchRecords,
};
