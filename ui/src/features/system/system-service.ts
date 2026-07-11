import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapSystemOverview } from "./system-mappers";
import type { SystemOverview } from "./system-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

async function getOverview(
  family = currentRouteFamilySegment(),
  options: ServiceRequestOptions = {},
): Promise<SystemOverview> {
  const [statsResponse, metricsResponse] = await Promise.all([
    apiv1.getFamilyStats(family, options),
    apiv1.getFamilyMetrics(family, options),
  ]);

  return mapSystemOverview(
    unwrapResponse(statsResponse, "Unable to load global broker statistics"),
    unwrapResponse(metricsResponse, "Unable to load broker metrics"),
  );
}

export const systemService = {
  getOverview,
};
