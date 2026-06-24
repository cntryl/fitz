import { createQuery, queryScope } from "@askrjs/askr/data";
import { scheduleService } from "./schedule-service";
import type { ScheduleOverview } from "./schedule-models";

const scheduleQueries = queryScope("schedule");

const SCHEDULE_OVERVIEW_KEY = scheduleQueries.key("overview");

export function createScheduleOverviewQuery() {
  return createQuery<ScheduleOverview>({
    key: SCHEDULE_OVERVIEW_KEY,
    fetch: scheduleService.getOverview,
  });
}
