import { createQuery, type Query } from "@/shared/query/query";
import { queueService } from "./queue-service";
import type { DeadLetterFilters, DeadLetterMessage, QueueResourceRef } from "./queue-models";

const QUEUE_DEAD_LETTERS_STALE_MS = 15_000;
const QUEUE_DEAD_LETTERS_RETRY_ATTEMPTS = 1;
const QUEUE_DEAD_LETTERS_RETRY_DELAY_MS = 250;

export interface QueueDeadLettersQuery extends Query<DeadLetterMessage[]> {
  key: string;
  retryAttempts: number;
  staleTimeMs: number;
  isStale(): boolean;
}

export type { DeadLetterFilters, DeadLetterMessage, QueueResourceRef } from "./queue-models";

const fetchedAtByKey = new Map<string, number>();

function wait(ms: number, signal: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    if (signal.aborted) {
      reject(new DOMException("Request aborted", "AbortError"));
      return;
    }

    const timeoutId = setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timeoutId);
        reject(new DOMException("Request aborted", "AbortError"));
      },
      { once: true },
    );
  });
}

async function withQueryRetry<T>(load: () => Promise<T>, signal: AbortSignal): Promise<T> {
  let lastError: unknown;

  for (let attempt = 0; attempt <= QUEUE_DEAD_LETTERS_RETRY_ATTEMPTS; attempt += 1) {
    try {
      return await load();
    } catch (error) {
      lastError = error;

      if (signal.aborted || attempt === QUEUE_DEAD_LETTERS_RETRY_ATTEMPTS) {
        break;
      }

      await wait(QUEUE_DEAD_LETTERS_RETRY_DELAY_MS * (attempt + 1), signal);
    }
  }

  throw lastError;
}

export function queueDeadLettersQueryKey(
  resourceRef: QueueResourceRef,
  filters: DeadLetterFilters = {},
) {
  return `queue:dead-letters:${resourceRef.realm}:${resourceRef.area}:${resourceRef.resource}:${
    filters.family ?? "all"
  }`;
}

// Query boundary only: own keys, cancellation, retry, refresh, and stale policy.
export function createQueueDeadLettersQuery(
  resourceRef: QueueResourceRef,
  filters: DeadLetterFilters = {},
): QueueDeadLettersQuery {
  const key = queueDeadLettersQueryKey(resourceRef, filters);
  const result = createQuery({
    key,
    fetch: async ({ signal }) => {
      const messages = await withQueryRetry(
        () => queueService.listDeadLetters(resourceRef, filters, { signal }),
        signal,
      );

      fetchedAtByKey.set(key, Date.now());
      return messages;
    },
  });

  return Object.assign(result, {
    key,
    retryAttempts: QUEUE_DEAD_LETTERS_RETRY_ATTEMPTS,
    staleTimeMs: QUEUE_DEAD_LETTERS_STALE_MS,
    isStale: () => Date.now() - (fetchedAtByKey.get(key) ?? 0) > QUEUE_DEAD_LETTERS_STALE_MS,
  });
}
