import { createQuery } from "@askrjs/askr/data";
import { scheduleService } from "./schedule-service";
import type { ScheduleOverview } from "./schedule-models";

const SCHEDULE_OVERVIEW_KEY = "schedule:overview";

export function createScheduleOverviewQuery() {
  return createQuery<ScheduleOverview>({
    key: SCHEDULE_OVERVIEW_KEY,
    fetch: ({ signal }) => scheduleService.getOverview({ signal }),
  });
}
