import { createQuery, queryScope } from "@askrjs/askr/data";
import { queueService } from "./queue-service";
import { stableQueryFetch, type QueryFetch } from "@/shared/query-fetch";
import {
  currentRouteFamilySegment,
  DEFAULT_ROUTE_FAMILY_SEGMENT,
} from "@/shared/navigation/domains";
import type {
  DeadLetterFilters,
  DeadLetterMessage,
  QueueAreaDetail,
  QueueInventory,
  QueueOverview,
  QueueRealmDetail,
  QueueResourceRef,
} from "./queue-models";

export type { DeadLetterFilters, DeadLetterMessage, QueueResourceRef } from "./queue-models";

const queueQueries = queryScope("queue");
const queueDeadLetterFetches = new Map<string, QueryFetch<DeadLetterMessage[]>>();
const queueRealmFetches = new Map<string, QueryFetch<QueueRealmDetail>>();
const queueAreaFetches = new Map<string, QueryFetch<QueueAreaDetail>>();

export const QUEUE_OVERVIEW_KEY = queueQueries.key("overview");
export const QUEUE_INVENTORY_KEY = queueQueries.key("inventory");

export function queueOverviewQueryKey(family = currentRouteFamilySegment()) {
  return queueQueries.key("overview", family);
}

export function queueInventoryQueryKey(family = currentRouteFamilySegment()) {
  return queueQueries.key("inventory", family);
}

export function queueRealmQueryKey(realm: string) {
  return queueQueries.key("realm", currentRouteFamilySegment(), realm);
}

export function queueAreaQueryKey(realm: string, area: string) {
  return queueQueries.key("area", currentRouteFamilySegment(), realm, area);
}

export function queueDeadLettersQueryKey(
  resourceRef: QueueResourceRef,
  filters: DeadLetterFilters = {},
) {
  return queueQueries.key(
    "dead-letters",
    currentRouteFamilySegment(),
    resourceRef.realm,
    resourceRef.area,
    resourceRef.resource,
    filters.family ?? DEFAULT_ROUTE_FAMILY_SEGMENT,
  );
}

export function queueDeadLettersQueryPrefix(resourceRef: QueueResourceRef) {
  return queueQueries.prefix(
    "dead-letters",
    currentRouteFamilySegment(),
    resourceRef.realm,
    resourceRef.area,
    resourceRef.resource,
  );
}

export function createQueueOverviewQuery() {
  const key = queueOverviewQueryKey();

  return createQuery<QueueOverview>({
    key,
    fetch: queueService.getOverview,
  });
}

export function createQueueInventoryQuery() {
  const key = queueInventoryQueryKey();

  return createQuery<QueueInventory>({
    key,
    fetch: queueService.listInventory,
  });
}

export function createQueueRealmQuery(realm: string) {
  const key = queueRealmQueryKey(realm);

  return createQuery<QueueRealmDetail>({
    key,
    fetch: stableQueryFetch(
      queueRealmFetches,
      key,
      () =>
        ({ signal }) =>
          queueService.getRealm(realm, { signal }),
    ),
  });
}

export function createQueueAreaQuery(realm: string, area: string) {
  const key = queueAreaQueryKey(realm, area);

  return createQuery<QueueAreaDetail>({
    key,
    fetch: stableQueryFetch(
      queueAreaFetches,
      key,
      () =>
        ({ signal }) =>
          queueService.getArea(realm, area, { signal }),
    ),
  });
}

export function createQueueDeadLettersQuery(
  resourceRef: QueueResourceRef,
  filters: DeadLetterFilters = {},
) {
  const key = queueDeadLettersQueryKey(resourceRef, filters);

  return createQuery({
    key,
    fetch: stableQueryFetch(
      queueDeadLetterFetches,
      key,
      () =>
        ({ signal }) =>
          queueService.listDeadLetters(resourceRef, filters, { signal }),
    ),
  });
}
