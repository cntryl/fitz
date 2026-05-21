import { createQuery } from "@askrjs/askr/data";
import { metricsService } from "./metrics-service";
import type { MetricsOverview } from "./metrics-models";

export function createMetricsOverviewQuery() {
  return createQuery<MetricsOverview>({
    key: "metrics:overview",
    fetch: ({ signal }) => metricsService.getOverview({ signal }),
  });
}
