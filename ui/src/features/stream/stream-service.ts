import { apiParams, apiParamsQuery, apiv1 } from "@/adapters";
import type { StreamRecordsResponse } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "@/shared/navigation/domains";
import {
  routeFamilyRequest,
  type RouteFamilyRequestOptions,
} from "@/shared/navigation/route-family-request";
import { mapStreamOverview } from "./stream-mappers";
import type {
  StreamAreaRollup,
  StreamOverview,
  StreamRealmRollup,
  StreamRecordSearchRequest,
  StreamResourceView,
} from "./stream-models";

async function getOverview(options: RouteFamilyRequestOptions = {}): Promise<StreamOverview> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listStreamRealms(apiParams({ family }, requestOptions)),
    apiv1.getStreamStats(apiParams({ family }, requestOptions)),
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
      apiParamsQuery(
        { family: apiRouteFamilySegment(request.routeFamily) },
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
      apiParamsQuery(
        {
          area: request.area,
          family: apiRouteFamilySegment(request.routeFamily),
          realm: request.realm,
          resource: request.resource,
        },
        {
          discriminator: request.discriminator,
          from_offset: request.fromOffset,
          limit: request.limit,
        },
        options,
      ),
    ),
    "Unable to read stream records",
  );
}

async function getRealmRollup(
  realm: string,
  options: RouteFamilyRequestOptions = {},
): Promise<StreamRealmRollup> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const [watermarks, areas] = await Promise.all([
    apiv1.getStreamRealmWatermarks(apiParams({ family, realm }, requestOptions)),
    apiv1.listStreamAreas(apiParams({ family, realm }, requestOptions)),
  ]);
  const areaRows = await Promise.all(
    unwrapResponse(areas, "Unable to load stream areas").areas.map(async ({ area }) => {
      const resources = unwrapResponse(
        await apiv1.listStreamResources(apiParams({ area, family, realm }, requestOptions)),
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
  options: RouteFamilyRequestOptions = {},
): Promise<StreamAreaRollup> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const [watermarks, resources] = await Promise.all([
    apiv1.getStreamAreaWatermarks(apiParams({ area, family, realm }, requestOptions)),
    apiv1.listStreamResources(apiParams({ area, family, realm }, requestOptions)),
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
    apiv1.getStreamResource(
      apiParams(
        { area: request.area, family, realm: request.realm, resource: request.resource },
        options,
      ),
    ),
    apiv1.readStreamResourceRecords(
      apiParamsQuery(
        { area: request.area, family, realm: request.realm, resource: request.resource },
        {
          discriminator: request.discriminator,
          from_offset: request.fromOffset,
          limit: request.limit,
        },
        options,
      ),
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
