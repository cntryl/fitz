import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapQueueDeadLetter, mapQueueOverview } from "./queue-mappers";
import type {
  DeadLetterFilters,
  DeadLetterMessage,
  QueueInventory,
  QueueOverview,
  QueueResourceRef,
} from "./queue-models";

export type { DeadLetterFilters, DeadLetterMessage, QueueResourceRef } from "./queue-models";

const INVENTORY_CONCURRENCY = 4;

async function mapWithConcurrency<T, R>(
  items: T[],
  worker: (item: T) => Promise<R>,
  concurrency: number,
): Promise<R[]> {
  const results = new Array<R | undefined>(items.length);
  let nextIndex = 0;

  async function runNext() {
    const currentIndex = nextIndex++;
    if (currentIndex >= items.length) {
      return;
    }

    results[currentIndex] = await worker(items[currentIndex]);
    await runNext();
  }

  const workers = Array.from({ length: Math.max(1, Math.min(concurrency, items.length)) }, () =>
    runNext(),
  );
  await Promise.all(workers);
  return results as R[];
}

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

async function listInventory(options: ServiceRequestOptions = {}): Promise<QueueInventory> {
  const realms = unwrapResponse(
    await apiv1.listQueueRealms(options),
    "Unable to load queue realms for inventory",
  ).realms;

  const inventoryRealms = await mapWithConcurrency(
    realms,
    async ({ realm }) => {
      const areas = unwrapResponse(
        await apiv1.listQueueAreas(realm, options),
        `Unable to load queue areas for ${realm}`,
      ).areas;

      const inventoryAreas = await mapWithConcurrency(
        areas,
        async ({ area }) => ({
          area,
          resources: unwrapResponse(
            await apiv1.listQueueResources(realm, area, options),
            `Unable to load queue resources for ${realm}/${area}`,
          ).resources.map((entry) => entry.resource),
        }),
        INVENTORY_CONCURRENCY,
      );

      return { areas: inventoryAreas, realm };
    },
    INVENTORY_CONCURRENCY,
  );

  return { domain: "queue", realms: inventoryRealms };
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
  listInventory,
  purgeDeadLetter,
  listDeadLetters,
  replayDeadLetter,
};
