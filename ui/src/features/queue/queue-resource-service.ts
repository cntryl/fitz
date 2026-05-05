import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapQueueResourceOverview } from "./queue-resource-mappers";
import type { QueueResourceOverview, QueueResourceRef } from "./queue-resource-models";

async function getResource(
  resourceRef: QueueResourceRef,
  options: ServiceRequestOptions = {},
): Promise<QueueResourceOverview> {
  const [detailResponse, inflightResponse, deadLettersResponse] = await Promise.all([
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
  ]);

  return mapQueueResourceOverview(
    unwrapResponse(detailResponse, "Unable to load queue resource"),
    unwrapResponse(inflightResponse, "Unable to load queue inflight entries").inflight,
    unwrapResponse(deadLettersResponse, "Unable to load queue dead-letter messages").messages,
  );
}

// Services are the app contract boundary: no Askr resources and no FetchResponse leaks.
export const queueResourceService = {
  getResource,
};
