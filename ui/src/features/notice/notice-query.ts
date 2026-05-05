import { createQuery } from "@askrjs/askr/data";
import { noticeService } from "./notice-service";
import type { NoticeOverview } from "./notice-models";

const NOTICE_OVERVIEW_KEY = "notice:overview";

export function createNoticeOverviewQuery() {
  return createQuery<NoticeOverview>({
    key: NOTICE_OVERVIEW_KEY,
    fetch: ({ signal }) => noticeService.getOverview({ signal }),
  });
}
