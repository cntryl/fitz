import { createQuery, queryScope } from "@askrjs/askr/data";
import { queueService } from "./queue-service";
import { stableQueryFetch, type QueryFetch } from "@/shared/query-fetch";
import type {
  DeadLetterFilters,
  DeadLetterMessage,
  QueueInventory,
  QueueOverview,
  QueueResourceRef,
} from "./queue-models";

export type { DeadLetterFilters, DeadLetterMessage, QueueResourceRef } from "./queue-models";

const queueQueries = queryScope("queue");
const queueDeadLetterFetches = new Map<string, QueryFetch<DeadLetterMessage[]>>();

export const QUEUE_OVERVIEW_KEY = queueQueries.key("overview");
export const QUEUE_INVENTORY_KEY = queueQueries.key("inventory");

export function queueDeadLettersQueryKey(
  resourceRef: QueueResourceRef,
  filters: DeadLetterFilters = {},
) {
  return queueQueries.key(
    "dead-letters",
    resourceRef.realm,
    resourceRef.area,
    resourceRef.resource,
    filters.family ?? "all",
  );
}

export function queueDeadLettersQueryPrefix(resourceRef: QueueResourceRef) {
  return queueQueries.prefix(
    "dead-letters",
    resourceRef.realm,
    resourceRef.area,
    resourceRef.resource,
  );
}

export function createQueueOverviewQuery() {
  return createQuery<QueueOverview>({
    key: QUEUE_OVERVIEW_KEY,
    fetch: queueService.getOverview,
  });
}

export function createQueueInventoryQuery() {
  return createQuery<QueueInventory>({
    key: QUEUE_INVENTORY_KEY,
    fetch: queueService.listInventory,
  });
}

export function createQueueDeadLettersQuery(
  resourceRef: QueueResourceRef,
  filters: DeadLetterFilters = {},
) {
  const key = queueDeadLettersQueryKey(resourceRef, filters);

  return createQuery({
    key,
    fetch: stableQueryFetch(queueDeadLetterFetches, key, () => ({ signal }) =>
      queueService.listDeadLetters(resourceRef, filters, { signal }),
    ),
  });
}
