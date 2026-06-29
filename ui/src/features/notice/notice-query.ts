import { createQuery, queryScope } from "@askrjs/askr/data";
import { stableQueryFetch, type QueryFetch } from "@/shared/query-fetch";
import { noticeService } from "./notice-service";
import type {
  NoticeAreaResourceRows,
  NoticeDeliveryRows,
  NoticeOverview,
  NoticeRealmInventory,
  NoticeResourceOperationRows,
} from "./notice-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const noticeQueries = queryScope("notice");
const noticeRealmFetches = new Map<string, QueryFetch<NoticeRealmInventory>>();
const noticeAreaFetches = new Map<string, QueryFetch<NoticeAreaResourceRows>>();
const noticeResourceRowsFetches = new Map<string, QueryFetch<NoticeResourceOperationRows>>();
const noticeOperationRowsFetches = new Map<string, QueryFetch<NoticeDeliveryRows>>();

export const NOTICE_OVERVIEW_KEY = noticeQueries.key("overview", currentRouteFamilySegment());

export function noticeOverviewQueryKey(family = currentRouteFamilySegment()) {
  return noticeQueries.key("overview", family);
}

export function noticeRealmQueryKey(realm: string, family = currentRouteFamilySegment()) {
  return noticeQueries.key("notice-realm", family, realm);
}

export function noticeAreaQueryKey(
  realm: string,
  area: string,
  family = currentRouteFamilySegment(),
) {
  return noticeQueries.key("notice-area", family, realm, area);
}

export function noticeResourceRowsQueryKey(
  realm: string,
  area: string,
  resource: string,
  limit = 0,
  family = currentRouteFamilySegment(),
) {
  return noticeQueries.key(
    "notice-resource-rows",
    family,
    realm,
    area,
    resource,
    String(limit ?? 0),
  );
}

export function noticeOperationRowsQueryKey(
  realm: string,
  area: string,
  resource: string,
  operation: string,
  limit = 0,
  family = currentRouteFamilySegment(),
) {
  return noticeQueries.key(
    "notice-operation-rows",
    family,
    realm,
    area,
    resource,
    encodeURIComponent(operation),
    String(limit ?? 0),
  );
}

export function createNoticeOverviewQuery() {
  const key = noticeOverviewQueryKey();

  return createQuery<NoticeOverview>({
    key,
    fetch: noticeService.getOverview,
  });
}

export function createNoticeRealmQuery(realm: string) {
  const key = noticeRealmQueryKey(realm);

  return createQuery<NoticeRealmInventory>({
    key,
    fetch: stableQueryFetch(
      noticeRealmFetches,
      key,
      () =>
        ({ signal }) =>
          noticeService.listNoticeAreas(realm, { signal }),
    ),
  });
}

export function createNoticeAreaQuery(realm: string, area: string) {
  const key = noticeAreaQueryKey(realm, area);

  return createQuery<NoticeAreaResourceRows>({
    key,
    fetch: stableQueryFetch(
      noticeAreaFetches,
      key,
      () =>
        ({ signal }) =>
          noticeService.listNoticeResources(realm, area, { signal }),
    ),
  });
}

export function createNoticeResourceRowsQuery(request: {
  realm: string;
  area: string;
  resource: string;
  limit?: number;
}) {
  const limit = request.limit ?? 50;
  const key = noticeResourceRowsQueryKey(request.realm, request.area, request.resource, limit);

  return createQuery<NoticeResourceOperationRows>({
    key,
    fetch: stableQueryFetch(
      noticeResourceRowsFetches,
      key,
      () =>
        ({ signal }) =>
          noticeService.searchResourceRows(
            {
              area: request.area,
              limit,
              realm: request.realm,
              resource: request.resource,
            },
            { signal },
          ),
    ),
  });
}

export function createNoticeOperationRowsQuery(request: {
  realm: string;
  area: string;
  resource: string;
  operation: string;
  limit?: number;
}) {
  const limit = request.limit ?? 50;
  const key = noticeOperationRowsQueryKey(
    request.realm,
    request.area,
    request.resource,
    request.operation,
    limit,
  );

  return createQuery<NoticeDeliveryRows>({
    key,
    fetch: stableQueryFetch(
      noticeOperationRowsFetches,
      key,
      () =>
        ({ signal }) =>
          noticeService.searchOperationRows(
            {
              area: request.area,
              limit,
              operation: request.operation,
              query: request.operation,
              realm: request.realm,
              resource: request.resource,
            },
            { signal },
          ),
    ),
  });
}
