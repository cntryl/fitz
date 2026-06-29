import { createMutation } from "@askrjs/askr/data";
import { queueService } from "./queue-service";
import type { DeadLetterMessage, QueueResourceRef } from "./queue-models";
import {
  queueAreaQueryKey,
  queueDeadLettersQueryPrefix,
  queueOverviewQueryKey,
  queueRealmQueryKey,
} from "./queue-query";
import { queueResourceQueryKey, queueResourceTimelineQueryKey } from "./queue-resource-query";

export function affectedQueueKeys(resourceRef: QueueResourceRef) {
  return [
    queueOverviewQueryKey(),
    queueRealmQueryKey(resourceRef.realm),
    queueAreaQueryKey(resourceRef.realm, resourceRef.area),
    queueResourceQueryKey(resourceRef),
    queueResourceTimelineQueryKey(resourceRef),
    queueDeadLettersQueryPrefix(resourceRef),
  ];
}

export function createReplayQueueDeadLetterMutation(resourceRef: QueueResourceRef) {
  return createMutation<DeadLetterMessage, boolean>({
    action: async (message, { signal }) =>
      queueService.replayDeadLetter(resourceRef, message.messageId, message.family, { signal }),
    affects: () => affectedQueueKeys(resourceRef),
    afterSuccess: "invalidate",
  });
}

export function createPurgeQueueDeadLetterMutation(resourceRef: QueueResourceRef) {
  return createMutation<DeadLetterMessage, boolean>({
    action: async (message, { signal }) =>
      queueService.purgeDeadLetter(resourceRef, message.messageId, message.family, { signal }),
    affects: () => affectedQueueKeys(resourceRef),
    afterSuccess: "invalidate",
  });
}
