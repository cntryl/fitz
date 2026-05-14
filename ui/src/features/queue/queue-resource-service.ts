import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapQueueResourceOverview, mapQueueResourceTimeline } from "./queue-resource-mappers";
import type {
  QueueResourceOverview,
  QueueResourceRef,
  QueueResourceTimeline,
} from "./queue-resource-models";

async function getResource(
  resourceRef: QueueResourceRef,
  options: ServiceRequestOptions = {},
): Promise<QueueResourceOverview> {
  const [detailResponse, inflightResponse, deadLettersResponse, timelineResponse] = await Promise.all([
    apiv1.getQueueResource(resourceRef.realm, resourceRef.area, resourceRef.resource, options),
    apiv1.listQueueInflightEntries(
      resourceRef.realm,
      resourceRef.area,
      resourceRef.resource,
      options,
    ),
    apiv1.listQueueDeadLetters(
      resourceRef.realm,
      resourceRef.area,
      resourceRef.resource,
      undefined,
      options,
    ),
    apiv1.listQueueResourceEvents(
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
  const response = await apiv1.listQueueResourceEvents(
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

// Services are the app contract boundary: no Askr resources and no FetchResponse leaks.
export const queueResourceService = {
  getResource,
  getTimeline,
};
