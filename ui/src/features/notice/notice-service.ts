import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapNoticeOverview } from "./notice-mappers";
import type { NoticeOverview } from "./notice-models";

async function getOverview(options: ServiceRequestOptions = {}): Promise<NoticeOverview> {
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listNoticeRealms(options),
    apiv1.getNoticeStats(options),
  ]);

  return mapNoticeOverview(
    unwrapResponse(realmsResponse, "Unable to load notice realms").realms,
    unwrapResponse(statsResponse, "Unable to load notice statistics"),
  );
}

export const noticeService = {
  getOverview,
};
