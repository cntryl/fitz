import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
import { systemService } from "./system-service";
import type { SystemOverview } from "./system-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const systemQueries = queryScope("system");

export const SYSTEM_OVERVIEW_KEY = systemQueries.key("overview");

const systemOverviewQuery = defineQuery<{ family: string }, SystemOverview>({
  key: ({ family }) => systemOverviewQueryKey(family),
  fetch: ({ family, signal }) => systemService.getOverview(family, { signal }),
});

export function systemOverviewQueryKey(family = currentRouteFamilySegment()) {
  return systemQueries.key("overview", family);
}

export function createSystemOverviewQuery(family = currentRouteFamilySegment()) {
  return createQuery(systemOverviewQuery, { family });
}
