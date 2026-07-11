import { createQuery, queryScope } from "@askrjs/askr/data";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";
import { metricsService } from "./metrics-service";
import type { MetricsOverview } from "./metrics-models";

const metricsQueries = queryScope("metrics");

export function createMetricsOverviewQuery(family = currentRouteFamilySegment()) {
  return createQuery<MetricsOverview>({
    key: metricsQueries.key("overview", family),
    fetch: (options) => metricsService.getOverview(family, options),
  });
}
