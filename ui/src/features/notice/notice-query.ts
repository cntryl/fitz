import { createQuery, queryScope } from "@askrjs/askr/data";
import { noticeService } from "./notice-service";
import type { NoticeOverview } from "./notice-models";

const noticeQueries = queryScope("notice");

const NOTICE_OVERVIEW_KEY = noticeQueries.key("overview");

export function createNoticeOverviewQuery() {
  return createQuery<NoticeOverview>({
    key: NOTICE_OVERVIEW_KEY,
    fetch: noticeService.getOverview,
  });
}
