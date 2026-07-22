import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";
import { metricsService } from "./metrics-service";
import type { MetricsOverview } from "./metrics-models";

const metricsQueries = queryScope("metrics");
const metricsOverviewQuery = defineQuery<{ family: string }, MetricsOverview>({
  key: ({ family }) => metricsQueries.key("overview", family),
  fetch: ({ family, signal }) => metricsService.getOverview(family, { signal }),
});

export function createMetricsOverviewQuery(family = currentRouteFamilySegment()) {
  return createQuery(metricsOverviewQuery, { family });
}
