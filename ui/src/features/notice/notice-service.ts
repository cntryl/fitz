import { apiv1 } from "@/adapters";
import type { NoticeDeliveryObservationList } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapNoticeOverview } from "./notice-mappers";
import type { NoticeDeliverySearchRequest, NoticeOverview } from "./notice-models";

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

async function searchDeliveries(
  request: NoticeDeliverySearchRequest,
  options: ServiceRequestOptions = {},
): Promise<NoticeDeliveryObservationList> {
  return unwrapResponse(
    await apiv1.searchNoticeDeliveries(
      {
        area: request.area,
        limit: request.limit,
        q: request.query,
        realm: request.realm,
        resource: request.resource,
        route_family: request.routeFamily,
      },
      options,
    ),
    "Unable to search notice delivery evidence",
  );
}

export const noticeService = {
  getOverview,
  searchDeliveries,
};
