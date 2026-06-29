import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "@/shared/navigation/domains";
import {
  mapQueueResourceComparison,
  mapQueueResourceOverview,
  mapQueueResourceTimeline,
} from "./queue-resource-mappers";
import type {
  QueueResourceComparison,
  QueueResourceOverview,
  QueueResourceRef,
  QueueResourceTimeline,
} from "./queue-resource-models";

async function getResource(
  resourceRef: QueueResourceRef,
  options: ServiceRequestOptions = {},
): Promise<QueueResourceOverview> {
  const family = apiRouteFamilySegment();
  const [detailResponse, inflightResponse, deadLettersResponse, timelineResponse] =
    await Promise.all([
      apiv1.getQueueResource(
        family,
        resourceRef.realm,
        resourceRef.area,
        resourceRef.resource,
        options,
      ),
      apiv1.listQueueInflightEntries(
        family,
        resourceRef.realm,
        resourceRef.area,
        resourceRef.resource,
        options,
      ),
      apiv1.listQueueDeadLetters(
        family,
        resourceRef.realm,
        resourceRef.area,
        resourceRef.resource,
        options,
      ),
      apiv1.listQueueResourceEvents(
        family,
        resourceRef.realm,
        resourceRef.area,
        resourceRef.resource,
        { limit: 8 },
        options,
      ),
    ]);

  return mapQueueResourceOverview(
    unwrapResponse(detailResponse, "Unable to load queue resource"),
    unwrapResponse(inflightResponse, "Unable to load queue inflight entries").inflight,
    unwrapResponse(deadLettersResponse, "Unable to load queue dead-letter messages").messages,
    unwrapResponse(timelineResponse, "Unable to load queue resource timeline"),
  );
}

async function getTimeline(
  resourceRef: QueueResourceRef,
  options: ServiceRequestOptions = {},
): Promise<QueueResourceTimeline> {
  const family = apiRouteFamilySegment();
  const response = await apiv1.listQueueResourceEvents(
    family,
    resourceRef.realm,
    resourceRef.area,
    resourceRef.resource,
    { limit: 8 },
    options,
  );

  return mapQueueResourceTimeline(
    unwrapResponse(response, "Unable to load queue resource timeline"),
  );
}

async function compareResource(
  resourceRef: QueueResourceRef,
  againstResourceRef: QueueResourceRef & { family?: number | null },
  options: ServiceRequestOptions = {},
): Promise<QueueResourceComparison> {
  const response = await apiv1.compareQueueResourceSnapshots(
    apiRouteFamilySegment(),
    resourceRef.realm,
    resourceRef.area,
    resourceRef.resource,
    {
      against_area: againstResourceRef.area,
      against_family: againstResourceRef.family ?? undefined,
      against_realm: againstResourceRef.realm,
      against_resource: againstResourceRef.resource,
    },
    options,
  );

  return mapQueueResourceComparison(
    unwrapResponse(response, "Unable to compare queue resource snapshots"),
  );
}

// Services are the app contract boundary: no Askr resources and no FetchResponse leaks.
export const queueResourceService = {
  compareResource,
  getResource,
  getTimeline,
};
