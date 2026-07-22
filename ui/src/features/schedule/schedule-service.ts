import { apiParams, apiParamsQuery, apiv1 } from "@/adapters";
import type { ScheduleExecutionObservationList, ScheduleMissedObservationList } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "@/shared/navigation/domains";
import { mapScheduleOverview } from "./schedule-mappers";
import type {
  ScheduleExecutionObservationRequest,
  ScheduleAreaInventory,
  ScheduleMissedObservationRequest,
  ScheduleOverview,
  ScheduleRealmInventory,
  ScheduleResourceView,
} from "./schedule-models";

async function getOverview(options: ServiceRequestOptions = {}): Promise<ScheduleOverview> {
  const family = apiRouteFamilySegment();
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listScheduleRealms(apiParams({ family }, options)),
    apiv1.getScheduleStats(apiParams({ family }, options)),
  ]);

  return mapScheduleOverview(
    unwrapResponse(realmsResponse, "Unable to load schedule realms").realms,
    unwrapResponse(statsResponse, "Unable to load schedule statistics"),
  );
}

async function listExecutionObservations(
  request: ScheduleExecutionObservationRequest,
  options: ServiceRequestOptions = {},
): Promise<ScheduleExecutionObservationList> {
  return unwrapResponse(
    await apiv1.listScheduleExecutionObservations(
      apiParamsQuery(
        {
          area: request.area,
          family: apiRouteFamilySegment(request.routeFamily),
          realm: request.realm,
          resource: request.resource,
        },
        {
          limit: request.limit,
        },
        options,
      ),
    ),
    "Unable to load schedule handoff observations",
  );
}

async function listScheduleAreas(
  realm: string,
  options: ServiceRequestOptions = {},
): Promise<ScheduleRealmInventory> {
  const family = apiRouteFamilySegment();
  const areas = unwrapResponse(
    await apiv1.listScheduleAreas(apiParams({ family, realm }, options)),
    "Unable to load schedule areas",
  ).areas;
  const areaRows = await Promise.all(
    areas.map(async ({ area }) => {
      const resources = unwrapResponse(
        await apiv1.listScheduleResources(apiParams({ area, family, realm }, options)),
        "Unable to load schedule resources",
      ).resources.map((entry) => entry.resource);

      return { area, resources };
    }),
  );

  return {
    areas: areaRows,
    realm,
    resourceCount: areaRows.reduce((sum, area) => sum + area.resources.length, 0),
  };
}

async function listScheduleResources(
  realm: string,
  area: string,
  options: ServiceRequestOptions = {},
): Promise<ScheduleAreaInventory> {
  const family = apiRouteFamilySegment();
  const resources = unwrapResponse(
    await apiv1.listScheduleResources(apiParams({ area, family, realm }, options)),
    "Unable to load schedule resources",
  ).resources.map((entry) => entry.resource);

  return {
    area,
    realm,
    resourceCount: resources.length,
    resources,
  };
}

async function getScheduleResource(
  request: Required<
    Pick<ScheduleExecutionObservationRequest, "area" | "realm" | "resource" | "routeFamily">
  > &
    Pick<ScheduleExecutionObservationRequest, "limit">,
  options: ServiceRequestOptions = {},
): Promise<ScheduleResourceView> {
  const family = apiRouteFamilySegment(request.routeFamily);
  const limit = request.limit ?? 20;
  const [detail, executionObservations, missedHandoffs] = await Promise.all([
    apiv1.getScheduleResource(
      apiParams(
        { area: request.area, family, realm: request.realm, resource: request.resource },
        options,
      ),
    ),
    apiv1.listScheduleExecutionObservations(
      apiParamsQuery(
        { area: request.area, family, realm: request.realm, resource: request.resource },
        { limit },
        options,
      ),
    ),
    apiv1.searchScheduleMissedHandoffs(
      apiParamsQuery(
        { family },
        {
          area: request.area,
          limit,
          realm: request.realm,
          resource: request.resource,
        },
        options,
      ),
    ),
  ]);

  return {
    detail: unwrapResponse(detail, "Unable to load schedule resource"),
    executionObservations: unwrapResponse(
      executionObservations,
      "Unable to load schedule handoff observations",
    ),
    missedHandoffs: unwrapResponse(missedHandoffs, "Unable to load pending schedule handoffs"),
  };
}

async function searchMissedHandoffs(
  request: ScheduleMissedObservationRequest,
  options: ServiceRequestOptions = {},
): Promise<ScheduleMissedObservationList> {
  return unwrapResponse(
    await apiv1.searchScheduleMissedHandoffs(
      apiParamsQuery(
        { family: apiRouteFamilySegment(request.routeFamily) },
        {
          area: request.area,
          limit: request.limit,
          realm: request.realm,
          resource: request.resource,
        },
        options,
      ),
    ),
    "Unable to load pending schedule handoffs",
  );
}

export const scheduleService = {
  getScheduleResource,
  getOverview,
  listScheduleAreas,
  listExecutionObservations,
  listScheduleResources,
  searchMissedHandoffs,
};
