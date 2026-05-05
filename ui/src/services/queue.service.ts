import { apiv1 } from "../adapters";
import { unwrapResponse, type ServiceRequestOptions } from "./api.service";
import {
  mapQueueDeadLetter,
  type DeadLetterFilters,
  type DeadLetterMessage,
  type QueueResourceRef,
} from "./queue.mappers";

export type { DeadLetterFilters, DeadLetterMessage, QueueResourceRef } from "./queue.mappers";

async function listDeadLetters(
  resourceRef: QueueResourceRef,
  filters: DeadLetterFilters = {},
  options: ServiceRequestOptions = {},
): Promise<DeadLetterMessage[]> {
  const response = await apiv1.listQueueDeadLetters(
    resourceRef.realm,
    resourceRef.area,
    resourceRef.resource,
    filters.family === undefined ? undefined : { family: filters.family },
    options,
  );

  const dto = unwrapResponse(response, "Unable to load queue dead-letter messages");
  return dto.messages.map(mapQueueDeadLetter);
}

// Services are the app contract boundary: no Askr resources and no FetchResponse leaks.
export const queueService = {
  listDeadLetters,
};
