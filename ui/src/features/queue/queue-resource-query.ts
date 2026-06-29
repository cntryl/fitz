import { createQuery, queryScope } from "@askrjs/askr/data";
import { queueResourceService } from "./queue-resource-service";
import { stableQueryFetch, type QueryFetch } from "@/shared/query-fetch";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";
import type {
  QueueResourceComparison,
  QueueResourceComparisonSide,
  QueueResourceOverview,
  QueueResourceRef,
  QueueResourceTimeline,
} from "./queue-resource-models";

export type { QueueResourceRef } from "./queue-resource-models";

const queueResourceQueries = queryScope("queue");
const queueResourceFetches = new Map<string, QueryFetch<QueueResourceOverview>>();
const queueResourceTimelineFetches = new Map<string, QueryFetch<QueueResourceTimeline>>();
const queueResourceComparisonFetches = new Map<string, QueryFetch<QueueResourceComparison>>();

export function queueResourceQueryKey(resourceRef: QueueResourceRef) {
  return queueResourceQueries.key(
    "resource",
    currentRouteFamilySegment(),
    resourceRef.realm,
    resourceRef.area,
    resourceRef.resource,
  );
}

export function queueResourceTimelineQueryKey(resourceRef: QueueResourceRef) {
  return queueResourceQueries.key(
    "resource",
    currentRouteFamilySegment(),
    resourceRef.realm,
    resourceRef.area,
    resourceRef.resource,
    "timeline",
  );
}

export function queueResourceComparisonQueryKey(
  resourceRef: QueueResourceRef,
  againstResourceRef: QueueResourceComparisonSide["scope"],
) {
  return queueResourceQueries.key(
    "resource",
    currentRouteFamilySegment(),
    resourceRef.realm,
    resourceRef.area,
    resourceRef.resource,
    "compare",
    againstResourceRef.realm,
    againstResourceRef.area,
    againstResourceRef.resource,
    againstResourceRef.family ?? "any",
  );
}

export function createQueueResourceQuery(resourceRef: QueueResourceRef) {
  const key = queueResourceQueryKey(resourceRef);

  return createQuery<QueueResourceOverview>({
    key,
    fetch: stableQueryFetch(
      queueResourceFetches,
      key,
      () =>
        ({ signal }) =>
          queueResourceService.getResource(resourceRef, { signal }),
    ),
  });
}

export function createQueueResourceTimelineQuery(resourceRef: QueueResourceRef) {
  const key = queueResourceTimelineQueryKey(resourceRef);

  return createQuery<QueueResourceTimeline>({
    key,
    fetch: stableQueryFetch(
      queueResourceTimelineFetches,
      key,
      () =>
        ({ signal }) =>
          queueResourceService.getTimeline(resourceRef, { signal }),
    ),
  });
}

export function createQueueResourceComparisonQuery(
  resourceRef: QueueResourceRef,
  againstResourceRef: QueueResourceComparisonSide["scope"],
) {
  const key = queueResourceComparisonQueryKey(resourceRef, againstResourceRef);

  return createQuery<QueueResourceComparison>({
    key,
    fetch: stableQueryFetch(
      queueResourceComparisonFetches,
      key,
      () =>
        ({ signal }) =>
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
    ),
  });
}
