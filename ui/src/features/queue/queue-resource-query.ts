import { createQuery } from "@askrjs/askr/data";
import { queueResourceService } from "./queue-resource-service";
import type {
  QueueResourceOverview,
  QueueResourceRef,
  QueueResourceTimeline,
} from "./queue-resource-models";

export type { QueueResourceRef } from "./queue-resource-models";

function queueResourceQueryKey(resourceRef: QueueResourceRef) {
  return `queue:resource:${resourceRef.realm}:${resourceRef.area}:${resourceRef.resource}`;
}

export function createQueueResourceQuery(resourceRef: QueueResourceRef) {
  return createQuery<QueueResourceOverview>({
    key: queueResourceQueryKey(resourceRef),
    fetch: ({ signal }) => queueResourceService.getResource(resourceRef, { signal }),
  });
}

export function createQueueResourceTimelineQuery(resourceRef: QueueResourceRef) {
  return createQuery<QueueResourceTimeline>({
    key: `${queueResourceQueryKey(resourceRef)}:timeline`,
    fetch: ({ signal }) => queueResourceService.getTimeline(resourceRef, { signal }),
  });
}
