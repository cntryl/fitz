import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
import { queueService } from "./queue-service";
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

export const QUEUE_OVERVIEW_KEY = queueQueries.key("overview");
export const QUEUE_INVENTORY_KEY = queueQueries.key("inventory");

export function queueOverviewQueryKey(family = currentRouteFamilySegment()) {
  return queueQueries.key("overview", family);
}

export function queueInventoryQueryKey(family = currentRouteFamilySegment()) {
  return queueQueries.key("inventory", family);
}

export function queueRealmQueryKey(realm: string, family = currentRouteFamilySegment()) {
  return queueQueries.key("realm", family, realm);
}

export function queueAreaQueryKey(
  realm: string,
  area: string,
  family = currentRouteFamilySegment(),
) {
  return queueQueries.key("area", family, realm, area);
}

export function queueDeadLettersQueryKey(
  resourceRef: QueueResourceRef,
  filters: DeadLetterFilters = {},
  family = currentRouteFamilySegment(),
) {
  return queueQueries.key(
    "dead-letters",
    family,
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

const queueOverviewQuery = defineQuery<{ family: string }, QueueOverview>({
  key: ({ family }) => queueOverviewQueryKey(family),
  fetch: ({ signal }) => queueService.getOverview({ signal }),
});

const queueInventoryQuery = defineQuery<{ family: string }, QueueInventory>({
  key: ({ family }) => queueInventoryQueryKey(family),
  fetch: ({ signal }) => queueService.listInventory({ signal }),
});

const queueRealmQuery = defineQuery<{ family: string; realm: string }, QueueRealmDetail>({
  key: ({ family, realm }) => queueRealmQueryKey(realm, family),
  fetch: ({ realm, signal }) => queueService.getRealm(realm, { signal }),
});

const queueAreaQuery = defineQuery<
  { area: string; family: string; realm: string },
  QueueAreaDetail
>({
  key: ({ area, family, realm }) => queueAreaQueryKey(realm, area, family),
  fetch: ({ area, realm, signal }) => queueService.getArea(realm, area, { signal }),
});

interface QueueDeadLettersQueryInput {
  family: string;
  filters: DeadLetterFilters;
  resourceRef: QueueResourceRef;
}

const queueDeadLettersQuery = defineQuery<QueueDeadLettersQueryInput, DeadLetterMessage[]>({
  key: ({ family, filters, resourceRef }) => queueDeadLettersQueryKey(resourceRef, filters, family),
  fetch: ({ filters, resourceRef, signal }) =>
    queueService.listDeadLetters(resourceRef, filters, { signal }),
});

export function createQueueOverviewQuery() {
  return createQuery(queueOverviewQuery, { family: currentRouteFamilySegment() });
}

export function createQueueInventoryQuery() {
  return createQuery(queueInventoryQuery, { family: currentRouteFamilySegment() });
}

export function createQueueRealmQuery(realm: string) {
  return createQuery(queueRealmQuery, { family: currentRouteFamilySegment(), realm });
}

export function createQueueAreaQuery(realm: string, area: string) {
  return createQuery(queueAreaQuery, { area, family: currentRouteFamilySegment(), realm });
}

export function createQueueDeadLettersQuery(
  resourceRef: QueueResourceRef,
  filters: DeadLetterFilters = {},
) {
  return createQuery(queueDeadLettersQuery, {
    family: currentRouteFamilySegment(),
    filters,
    resourceRef,
  });
}
