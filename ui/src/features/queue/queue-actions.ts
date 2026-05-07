import { createMutation } from "@askrjs/askr/data";
import { queueService } from "./queue-service";
import type { DeadLetterMessage, QueueResourceRef } from "./queue-models";

const QUEUE_QUERY_PREFIX = "queue:";

export function createReplayQueueDeadLetterMutation(resourceRef: QueueResourceRef) {
  return createMutation<DeadLetterMessage, boolean>({
    action: async (message, { signal }) =>
      queueService.replayDeadLetter(resourceRef, message.messageId, message.family, { signal }),
    affects: () => [QUEUE_QUERY_PREFIX],
    afterSuccess: "invalidate",
  });
}

export function createPurgeQueueDeadLetterMutation(resourceRef: QueueResourceRef) {
  return createMutation<DeadLetterMessage, boolean>({
    action: async (message, { signal }) =>
      queueService.purgeDeadLetter(resourceRef, message.messageId, message.family, { signal }),
    affects: () => [QUEUE_QUERY_PREFIX],
    afterSuccess: "invalidate",
  });
}
