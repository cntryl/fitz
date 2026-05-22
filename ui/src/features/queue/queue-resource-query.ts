import { createQuery } from "@askrjs/askr/data";
import { queueResourceService } from "./queue-resource-service";
import type {
  QueueResourceComparison,
  QueueResourceComparisonSide,
  QueueResourceOverview,
  QueueResourceRef,
  QueueResourceTimeline,
} from "./queue-resource-models";

export type { QueueResourceRef } from "./queue-resource-models";

export function queueResourceQueryKey(resourceRef: QueueResourceRef) {
  return `queue:resource:${resourceRef.realm}:${resourceRef.area}:${resourceRef.resource}`;
}

export function queueResourceTimelineQueryKey(resourceRef: QueueResourceRef) {
  return `${queueResourceQueryKey(resourceRef)}:timeline`;
}

export function queueResourceComparisonQueryKey(
  resourceRef: QueueResourceRef,
  againstResourceRef: QueueResourceComparisonSide["scope"],
) {
  return `${queueResourceQueryKey(resourceRef)}:compare:${againstResourceRef.realm}:${againstResourceRef.area}:${againstResourceRef.resource}:${againstResourceRef.family ?? "any"}`;
}

export function createQueueResourceQuery(resourceRef: QueueResourceRef) {
  return createQuery<QueueResourceOverview>({
    key: queueResourceQueryKey(resourceRef),
    fetch: ({ signal }) => queueResourceService.getResource(resourceRef, { signal }),
  });
}

export function createQueueResourceTimelineQuery(resourceRef: QueueResourceRef) {
  return createQuery<QueueResourceTimeline>({
    key: queueResourceTimelineQueryKey(resourceRef),
    fetch: ({ signal }) => queueResourceService.getTimeline(resourceRef, { signal }),
  });
}

export function createQueueResourceComparisonQuery(
  resourceRef: QueueResourceRef,
  againstResourceRef: QueueResourceComparisonSide["scope"],
) {
  return createQuery<QueueResourceComparison>({
    key: queueResourceComparisonQueryKey(resourceRef, againstResourceRef),
    fetch: ({ signal }) =>
      queueResourceService.compareResource(
        resourceRef,
        {
          area: againstResourceRef.area,
          family: againstResourceRef.family,
          realm: againstResourceRef.realm,
          resource: againstResourceRef.resource,
        },
        { signal },
      ),
  });
}
