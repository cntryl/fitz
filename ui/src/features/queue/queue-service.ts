import { apiParams, apiv1 } from "@/adapters";
import { ensureResponseOk, unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "@/shared/navigation/domains";
import {
  mapQueueAreaDetail,
  mapQueueDeadLetter,
  mapQueueOverview,
  mapQueueRealmDetail,
  mapQueueResource,
} from "./queue-mappers";
import type {
  DeadLetterFilters,
  DeadLetterMessage,
  QueueAreaDetail,
  QueueInventory,
  QueueOverview,
  QueueRealmDetail,
  QueueResourceRef,
} from "./queue-models";

export type { DeadLetterFilters, DeadLetterMessage, QueueResourceRef } from "./queue-models";

const INVENTORY_CONCURRENCY = 4;

async function mapWithConcurrency<T, R>(
  items: T[],
  worker: (item: T) => Promise<R>,
  concurrency: number,
): Promise<R[]> {
  const results = Array.from<R | undefined>({ length: items.length });
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
  const family = apiRouteFamilySegment();
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listQueueRealms(apiParams({ family }, options)),
    apiv1.getQueueStats(apiParams({ family }, options)),
  ]);

  return mapQueueOverview(
    unwrapResponse(realmsResponse, "Unable to load queue realms").realms,
    unwrapResponse(statsResponse, "Unable to load queue statistics"),
  );
}

async function getRealm(
  realm: string,
  options: ServiceRequestOptions = {},
): Promise<QueueRealmDetail> {
  const response = await apiv1.getQueueRealm(
    apiParams({ family: apiRouteFamilySegment(), realm }, options),
  );

  return mapQueueRealmDetail(unwrapResponse(response, `Unable to load queue realm ${realm}`));
}

async function getArea(
  realm: string,
  area: string,
  options: ServiceRequestOptions = {},
): Promise<QueueAreaDetail> {
  const response = await apiv1.getQueueArea(
    apiParams({ area, family: apiRouteFamilySegment(), realm }, options),
  );

  return mapQueueAreaDetail(unwrapResponse(response, `Unable to load queue area ${realm}/${area}`));
}

async function listDeadLetters(
  resourceRef: QueueResourceRef,
  filters: DeadLetterFilters = {},
  options: ServiceRequestOptions = {},
): Promise<DeadLetterMessage[]> {
  const response = await apiv1.listQueueDeadLetters(
    apiParams(
      {
        area: resourceRef.area,
        family: apiRouteFamilySegment(filters.family),
        realm: resourceRef.realm,
        resource: resourceRef.resource,
      },
      options,
    ),
  );

  const dto = unwrapResponse(response, "Unable to load queue dead-letter messages");
  return dto.messages.map(mapQueueDeadLetter);
}

async function listInventory(options: ServiceRequestOptions = {}): Promise<QueueInventory> {
  const family = apiRouteFamilySegment();
  const realms = unwrapResponse(
    await apiv1.listQueueRealms(apiParams({ family }, options)),
    "Unable to load queue realms for inventory",
  ).realms;

  const inventoryRealms = await mapWithConcurrency(
    realms,
    async ({ realm }) => {
      const areas = unwrapResponse(
        await apiv1.listQueueAreas(apiParams({ family, realm }, options)),
        `Unable to load queue areas for ${realm}`,
      ).areas;

      const inventoryAreas = await mapWithConcurrency(
        areas,
        async ({ area }) => {
          const resourceEntries = unwrapResponse(
            await apiv1.listQueueResources(apiParams({ area, family, realm }, options)),
            `Unable to load queue resources for ${realm}/${area}`,
          ).resources.map(mapQueueResource);

          return {
            area,
            resourceEntries,
            resources: resourceEntries.map((entry) => entry.resource),
          };
        },
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
  const response = await apiv1.replayQueueDeadLetter(
    apiParams(
      {
        area: resourceRef.area,
        family: apiRouteFamilySegment(family),
        message_id: messageId,
        realm: resourceRef.realm,
        resource: resourceRef.resource,
      },
      options,
    ),
  );
  ensureResponseOk(response, "Unable to replay dead-letter message");
  return true;
}

async function purgeDeadLetter(
  resourceRef: QueueResourceRef,
  messageId: number,
  family: number,
  options: ServiceRequestOptions = {},
): Promise<boolean> {
  const response = await apiv1.purgeQueueDeadLetter(
    apiParams(
      {
        area: resourceRef.area,
        family: apiRouteFamilySegment(family),
        message_id: messageId,
        realm: resourceRef.realm,
        resource: resourceRef.resource,
      },
      options,
    ),
  );
  ensureResponseOk(response, "Unable to purge dead-letter message");
  return true;
}

// Services are the app contract boundary: no Askr resources and no FetchResult leaks.
export const queueService = {
  getArea,
  getOverview,
  getRealm,
  listInventory,
  purgeDeadLetter,
  listDeadLetters,
  replayDeadLetter,
};
