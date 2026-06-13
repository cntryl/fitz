import { createQuery } from "@askrjs/askr/data";
import { systemService } from "./system-service";
import type { SystemOverview } from "./system-models";

export const SYSTEM_OVERVIEW_KEY = "system:overview";

export function createSystemOverviewQuery() {
  return createQuery<SystemOverview>({
    key: SYSTEM_OVERVIEW_KEY,
    fetch: ({ signal }) => systemService.getOverview({ signal }),
  });
}
