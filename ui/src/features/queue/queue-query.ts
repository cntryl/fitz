import { createQuery, queryScope } from "@askrjs/askr/data";
import { queueService } from "./queue-service";
import type { DeadLetterFilters, QueueOverview, QueueResourceRef } from "./queue-models";

export type { DeadLetterFilters, DeadLetterMessage, QueueResourceRef } from "./queue-models";

const queueQueries = queryScope("queue");

export const QUEUE_OVERVIEW_KEY = queueQueries.key("overview");

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

export function createQueueDeadLettersQuery(
  resourceRef: QueueResourceRef,
  filters: DeadLetterFilters = {},
) {
  return createQuery({
    key: queueDeadLettersQueryKey(resourceRef, filters),
    fetch: ({ signal }) => queueService.listDeadLetters(resourceRef, filters, { signal }),
  });
}
