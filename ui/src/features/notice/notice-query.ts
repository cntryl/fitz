import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
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

const noticeOverviewQuery = defineQuery<{ family: string }, NoticeOverview>({
  key: ({ family }) => noticeOverviewQueryKey(family),
  fetch: ({ signal }) => noticeService.getOverview({ signal }),
});

const noticeRealmQuery = defineQuery<{ family: string; realm: string }, NoticeRealmInventory>({
  key: ({ family, realm }) => noticeRealmQueryKey(realm, family),
  fetch: ({ realm, signal }) => noticeService.listNoticeAreas(realm, { signal }),
});

const noticeAreaQuery = defineQuery<
  { area: string; family: string; realm: string },
  NoticeAreaResourceRows
>({
  key: ({ area, family, realm }) => noticeAreaQueryKey(realm, area, family),
  fetch: ({ area, realm, signal }) => noticeService.listNoticeResources(realm, area, { signal }),
});

interface NoticeResourceRowsQueryInput {
  area: string;
  family: string;
  limit: number;
  realm: string;
  resource: string;
}

const noticeResourceRowsQuery = defineQuery<
  NoticeResourceRowsQueryInput,
  NoticeResourceOperationRows
>({
  key: ({ area, family, limit, realm, resource }) =>
    noticeResourceRowsQueryKey(realm, area, resource, limit, family),
  fetch: ({ area, limit, realm, resource, signal }) =>
    noticeService.searchResourceRows({ area, limit, realm, resource }, { signal }),
});

interface NoticeOperationRowsQueryInput extends NoticeResourceRowsQueryInput {
  operation: string;
}

const noticeOperationRowsQuery = defineQuery<NoticeOperationRowsQueryInput, NoticeDeliveryRows>({
  key: ({ area, family, limit, operation, realm, resource }) =>
    noticeOperationRowsQueryKey(realm, area, resource, operation, limit, family),
  fetch: ({ area, limit, operation, realm, resource, signal }) =>
    noticeService.searchOperationRows(
      { area, limit, operation, query: operation, realm, resource },
      { signal },
    ),
});

export function createNoticeOverviewQuery() {
  return createQuery(noticeOverviewQuery, { family: currentRouteFamilySegment() });
}

export function createNoticeRealmQuery(realm: string) {
  return createQuery(noticeRealmQuery, { family: currentRouteFamilySegment(), realm });
}

export function createNoticeAreaQuery(realm: string, area: string) {
  return createQuery(noticeAreaQuery, { area, family: currentRouteFamilySegment(), realm });
}

export function createNoticeResourceRowsQuery(request: {
  realm: string;
  area: string;
  resource: string;
  limit?: number;
}) {
  const limit = request.limit ?? 50;
  return createQuery(noticeResourceRowsQuery, {
    ...request,
    family: currentRouteFamilySegment(),
    limit,
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
  return createQuery(noticeOperationRowsQuery, {
    ...request,
    family: currentRouteFamilySegment(),
    limit,
  });
}
