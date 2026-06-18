import { createQuery, queryScope } from "@askrjs/askr/data";
import { metricsService } from "./metrics-service";
import type { MetricsOverview } from "./metrics-models";

const metricsQueries = queryScope("metrics");

export function createMetricsOverviewQuery() {
  return createQuery<MetricsOverview>({
    key: metricsQueries.key("overview"),
    fetch: metricsService.getOverview,
  });
}
