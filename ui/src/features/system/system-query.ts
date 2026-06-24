import { createQuery, queryScope } from "@askrjs/askr/data";
import { systemService } from "./system-service";
import type { SystemOverview } from "./system-models";

const systemQueries = queryScope("system");

export const SYSTEM_OVERVIEW_KEY = systemQueries.key("overview");

export function createSystemOverviewQuery() {
  return createQuery<SystemOverview>({
    key: SYSTEM_OVERVIEW_KEY,
    fetch: systemService.getOverview,
  });
}
