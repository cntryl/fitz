import { createQuery } from "@askrjs/askr/data";
import { queueService } from "./queue-service";
import type { DeadLetterFilters, QueueOverview, QueueResourceRef } from "./queue-models";

export type { DeadLetterFilters, DeadLetterMessage, QueueResourceRef } from "./queue-models";

const QUEUE_OVERVIEW_KEY = "queue:overview";

function queueDeadLettersQueryKey(resourceRef: QueueResourceRef, filters: DeadLetterFilters = {}) {
  return `queue:dead-letters:${resourceRef.realm}:${resourceRef.area}:${resourceRef.resource}:${
    filters.family ?? "all"
  }`;
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
