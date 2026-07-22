import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
import { queueResourceService } from "./queue-resource-service";
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

export function queueResourceQueryKey(
  resourceRef: QueueResourceRef,
  family = currentRouteFamilySegment(),
) {
  return queueResourceQueries.key(
    "resource",
    family,
    resourceRef.realm,
    resourceRef.area,
    resourceRef.resource,
  );
}

export function queueResourceTimelineQueryKey(
  resourceRef: QueueResourceRef,
  family = currentRouteFamilySegment(),
) {
  return queueResourceQueries.key(
    "resource",
    family,
    resourceRef.realm,
    resourceRef.area,
    resourceRef.resource,
    "timeline",
  );
}

export function queueResourceComparisonQueryKey(
  resourceRef: QueueResourceRef,
  againstResourceRef: QueueResourceComparisonSide["scope"],
  family = currentRouteFamilySegment(),
) {
  return queueResourceQueries.key(
    "resource",
    family,
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

interface QueueResourceQueryInput {
  family: string;
  resourceRef: QueueResourceRef;
}

interface QueueResourceComparisonQueryInput extends QueueResourceQueryInput {
  againstResourceRef: QueueResourceComparisonSide["scope"];
}

const queueResourceQuery = defineQuery<QueueResourceQueryInput, QueueResourceOverview>({
  key: ({ family, resourceRef }) => queueResourceQueryKey(resourceRef, family),
  fetch: ({ resourceRef, signal }) => queueResourceService.getResource(resourceRef, { signal }),
});

const queueResourceTimelineQuery = defineQuery<QueueResourceQueryInput, QueueResourceTimeline>({
  key: ({ family, resourceRef }) => queueResourceTimelineQueryKey(resourceRef, family),
  fetch: ({ resourceRef, signal }) => queueResourceService.getTimeline(resourceRef, { signal }),
});

const queueResourceComparisonQuery = defineQuery<
  QueueResourceComparisonQueryInput,
  QueueResourceComparison
>({
  key: ({ againstResourceRef, family, resourceRef }) =>
    queueResourceComparisonQueryKey(resourceRef, againstResourceRef, family),
  fetch: ({ againstResourceRef, resourceRef, signal }) =>
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

export function createQueueResourceQuery(resourceRef: QueueResourceRef) {
  return createQuery(queueResourceQuery, { family: currentRouteFamilySegment(), resourceRef });
}

export function createQueueResourceTimelineQuery(resourceRef: QueueResourceRef) {
  return createQuery(queueResourceTimelineQuery, {
    family: currentRouteFamilySegment(),
    resourceRef,
  });
}

export function createQueueResourceComparisonQuery(
  resourceRef: QueueResourceRef,
  againstResourceRef: QueueResourceComparisonSide["scope"],
) {
  return createQuery(queueResourceComparisonQuery, {
    againstResourceRef,
    family: currentRouteFamilySegment(),
    resourceRef,
  });
}
