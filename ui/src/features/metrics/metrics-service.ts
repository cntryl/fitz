import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { parsePrometheusMetrics } from "./metrics-mappers";
import type { MetricsOverview } from "./metrics-models";

async function getOverview(options: ServiceRequestOptions = {}): Promise<MetricsOverview> {
  const data = unwrapResponse(await apiv1.getMetrics(options), "Unable to load metrics");
  const raw = typeof data === "string" ? data : JSON.stringify(data, null, 2);

  return parsePrometheusMetrics(raw);
}

export const metricsService = {
  getOverview,
};
