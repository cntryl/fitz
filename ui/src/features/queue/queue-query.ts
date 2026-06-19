import { createQuery, queryScope } from "@askrjs/askr/data";
import { queueService } from "./queue-service";
import type {
  DeadLetterFilters,
  QueueInventory,
  QueueOverview,
  QueueResourceRef,
} from "./queue-models";

export type { DeadLetterFilters, DeadLetterMessage, QueueResourceRef } from "./queue-models";

const queueQueries = queryScope("queue");

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
    fetch: ({ signal }) => queueService.getOverview({ signal }),
  });
}

export function createQueueInventoryQuery() {
  return createQuery<QueueInventory>({
    key: QUEUE_INVENTORY_KEY,
    fetch: ({ signal }) => queueService.listInventory({ signal }),
  });
}

export function createQueueDeadLettersQuery(
  resourceRef: QueueResourceRef,
  filters: DeadLetterFilters = {},
) {
  return createQuery({
    key: queueDeadLettersQueryKey(resourceRef, filters),
    fetch: ({ signal }) => queueService.listDeadLetters(resourceRef, filters, { signal }),
  });
}
