import { apiv1 } from "@/adapters";
import type { ScheduleExecutionObservationList, ScheduleMissedObservationList } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapScheduleOverview } from "./schedule-mappers";
import type {
  ScheduleExecutionObservationRequest,
  ScheduleMissedObservationRequest,
  ScheduleOverview,
} from "./schedule-models";

async function getOverview(options: ServiceRequestOptions = {}): Promise<ScheduleOverview> {
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listScheduleRealms(options),
    apiv1.getScheduleStats(options),
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
      request.realm,
      request.area,
      request.resource,
      {
        limit: request.limit,
        route_family: request.routeFamily,
      },
      options,
    ),
    "Unable to load schedule handoff observations",
  );
}

async function searchMissedHandoffs(
  request: ScheduleMissedObservationRequest,
  options: ServiceRequestOptions = {},
): Promise<ScheduleMissedObservationList> {
  return unwrapResponse(
    await apiv1.searchScheduleMissedHandoffs(
      {
        area: request.area,
        limit: request.limit,
        realm: request.realm,
        resource: request.resource,
        route_family: request.routeFamily,
      },
      options,
    ),
    "Unable to load pending schedule handoffs",
  );
}

export const scheduleService = {
  getOverview,
  listExecutionObservations,
  searchMissedHandoffs,
};
