import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapStructuredMetrics } from "./metrics-mappers";
import type { MetricsOverview } from "./metrics-models";

async function getOverview(
  family: string,
  options: ServiceRequestOptions = {},
): Promise<MetricsOverview> {
  const data = unwrapResponse(
    await apiv1.getFamilyMetrics(family, options),
    "Unable to load route-family metrics",
  );

  return mapStructuredMetrics(data);
}

export const metricsService = {
  getOverview,
};
