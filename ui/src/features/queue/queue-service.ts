import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapQueueDeadLetter, mapQueueOverview } from "./queue-mappers";
import type {
  DeadLetterFilters,
  DeadLetterMessage,
  QueueOverview,
  QueueResourceRef,
} from "./queue-models";

export type { DeadLetterFilters, DeadLetterMessage, QueueResourceRef } from "./queue-models";

async function getOverview(options: ServiceRequestOptions = {}): Promise<QueueOverview> {
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listQueueRealms(options),
    apiv1.getQueueStats(options),
  ]);

  return mapQueueOverview(
    unwrapResponse(realmsResponse, "Unable to load queue realms").realms,
    unwrapResponse(statsResponse, "Unable to load queue statistics"),
  );
}

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

async function replayDeadLetter(
  resourceRef: QueueResourceRef,
  messageId: number,
  family: number,
  options: ServiceRequestOptions = {},
): Promise<boolean> {
  return unwrapResponse(
    await apiv1.replayQueueDeadLetter(
      resourceRef.realm,
      resourceRef.area,
      resourceRef.resource,
      messageId,
      { family },
      options,
    ),
    "Unable to replay dead-letter message",
  );
}

async function purgeDeadLetter(
  resourceRef: QueueResourceRef,
  messageId: number,
  family: number,
  options: ServiceRequestOptions = {},
): Promise<boolean> {
  return unwrapResponse(
    await apiv1.purgeQueueDeadLetter(
      resourceRef.realm,
      resourceRef.area,
      resourceRef.resource,
      messageId,
      { family },
      options,
    ),
    "Unable to purge dead-letter message",
  );
}

// Services are the app contract boundary: no Askr resources and no FetchResponse leaks.
export const queueService = {
  getOverview,
  purgeDeadLetter,
  listDeadLetters,
  replayDeadLetter,
};
