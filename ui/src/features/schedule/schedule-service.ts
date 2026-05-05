import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapScheduleOverview } from "./schedule-mappers";
import type { ScheduleOverview } from "./schedule-models";

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

export const scheduleService = {
  getOverview,
};
