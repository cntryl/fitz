import { createQuery, queryScope } from "@askrjs/askr/data";
import { systemService } from "./system-service";
import type { SystemOverview } from "./system-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const systemQueries = queryScope("system");

export function systemOverviewQueryKey(family = currentRouteFamilySegment()) {
  return systemQueries.key("overview", family);
}

export function createSystemOverviewQuery(family = currentRouteFamilySegment()) {
  return createQuery<SystemOverview>({
    key: systemOverviewQueryKey(family),
    fetch: (options) => systemService.getOverview(family, options),
  });
}
