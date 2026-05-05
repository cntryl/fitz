import { createQuery } from "@askrjs/askr/data";
import { queueService } from "./queue-service";
import type { DeadLetterFilters, QueueResourceRef } from "./queue-models";

export type { DeadLetterFilters, DeadLetterMessage, QueueResourceRef } from "./queue-models";

function queueDeadLettersQueryKey(resourceRef: QueueResourceRef, filters: DeadLetterFilters = {}) {
  return `queue:dead-letters:${resourceRef.realm}:${resourceRef.area}:${resourceRef.resource}:${
    filters.family ?? "all"
  }`;
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
