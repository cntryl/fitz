import { apiParams, apiParamsQuery, apiv1 } from "@/adapters";
import type { NoticeDeliveryObservationList } from "@/adapters";
import {
  mapNoticeAreaResourceRows,
  mapNoticeDeliveryRows,
  mapNoticeRealmAreaSummary,
  mapNoticeRealmInventory,
  mapNoticeResourceOperationRows,
} from "./notice-mappers";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "@/shared/navigation/domains";
import { mapNoticeOverview } from "./notice-mappers";
import type {
  NoticeDeliveryRows,
  NoticeDeliverySearchRequest,
  NoticeOverview,
  NoticeAreaResourceRows,
  NoticeRealmInventory,
  NoticeResourceOperationRows,
} from "./notice-models";

const NOTICE_INVENTORY_CONCURRENCY = 4;
type NoticeServiceOptions = ServiceRequestOptions & { routeFamily?: number | string };

function splitRouteFamilyOption(options: NoticeServiceOptions) {
  const { routeFamily, ...requestOptions } = options;

  return {
    family: apiRouteFamilySegment(routeFamily),
    requestOptions,
  };
}

async function mapWithConcurrency<T, R>(
  items: T[],
  worker: (item: T) => Promise<R>,
  concurrency = 4,
): Promise<R[]> {
  const results = Array.from<R | undefined>({ length: items.length });
  let index = 0;

  async function run() {
    const current = index++;

    if (current >= items.length) {
      return;
    }

    results[current] = await worker(items[current]);
    await run();
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => run()));

  return results as R[];
}

async function getOverview(options: ServiceRequestOptions = {}): Promise<NoticeOverview> {
  const family = apiRouteFamilySegment();
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listNoticeRealms(apiParams({ family }, options)),
    apiv1.getNoticeStats(apiParams({ family }, options)),
  ]);

  return mapNoticeOverview(
    unwrapResponse(realmsResponse, "Unable to load notice realms").realms,
    unwrapResponse(statsResponse, "Unable to load notice statistics"),
  );
}

async function listNoticeAreas(
  realm: string,
  options: NoticeServiceOptions = {},
): Promise<NoticeRealmInventory> {
  const { family, requestOptions } = splitRouteFamilyOption(options);
  const areaEntries = unwrapResponse(
    await apiv1.listNoticeAreas(apiParams({ family, realm }, requestOptions)),
    `Unable to load notice areas for ${realm}`,
  ).areas;

  const summaries = await mapWithConcurrency(
    areaEntries,
    async ({ area }) => {
      const resources = unwrapResponse(
        await apiv1.listNoticeResources(apiParams({ area, family, realm }, requestOptions)),
        `Unable to load notice resources for ${realm}/${area}`,
      ).resources;

      return mapNoticeRealmAreaSummary(realm, area, resources);
    },
    NOTICE_INVENTORY_CONCURRENCY,
  );

  return mapNoticeRealmInventory(realm, summaries);
}

async function listNoticeResources(
  realm: string,
  area: string,
  options: NoticeServiceOptions = {},
): Promise<NoticeAreaResourceRows> {
  const { family, requestOptions } = splitRouteFamilyOption(options);
  const resources = unwrapResponse(
    await apiv1.listNoticeResources(apiParams({ area, family, realm }, requestOptions)),
    `Unable to load notice resources for ${realm}/${area}`,
  ).resources;

  return mapNoticeAreaResourceRows(realm, area, resources);
}

async function searchDeliveries(
  request: NoticeDeliverySearchRequest,
  options: ServiceRequestOptions = {},
): Promise<NoticeDeliveryObservationList> {
  return unwrapResponse(
    await apiv1.searchNoticeDeliveries(
      apiParamsQuery(
        { family: apiRouteFamilySegment(request.routeFamily) },
        {
          area: request.area,
          limit: request.limit,
          q: request.query,
          realm: request.realm,
          resource: request.resource,
        },
        options,
      ),
    ),
    "Unable to search notice delivery evidence",
  );
}

async function searchResourceRows(
  request: NoticeDeliverySearchRequest,
  options: ServiceRequestOptions = {},
): Promise<NoticeResourceOperationRows> {
  const response = await searchDeliveries(request, options);
  return mapNoticeResourceOperationRows(response);
}

async function searchOperationRows(
  request: NoticeDeliverySearchRequest,
  options: ServiceRequestOptions = {},
): Promise<NoticeDeliveryRows> {
  const response = await searchDeliveries(
    {
      ...request,
      query: request.query ?? request.operation,
    },
    options,
  );
  return mapNoticeDeliveryRows(response);
}

export const noticeService = {
  getOverview,
  listNoticeAreas,
  listNoticeResources,
  searchDeliveries,
  searchResourceRows,
  searchOperationRows,
};
