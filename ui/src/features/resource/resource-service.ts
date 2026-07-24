import type { ResourceEntry } from "@/adapters";
import type { ServiceRequestOptions } from "@/shared/errors/api";
import { getResourceInventoryAdapter } from "./resource-domain-adapters";
import type { DomainId, ResourceInventory, ResourceInventoryResource } from "./resource-models";

type ResourceEntryWithOperation = ResourceEntry & { operation?: string };
const RESOURCE_INVENTORY_CONCURRENCY = 4;

async function mapWithConcurrency<T, R>(items: T[], worker: (item: T) => Promise<R>): Promise<R[]> {
  const results = Array.from<R | undefined>({ length: items.length });
  let nextIndex = 0;

  async function run() {
    while (nextIndex < items.length) {
      const currentIndex = nextIndex++;
      results[currentIndex] = await worker(items[currentIndex]);
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(RESOURCE_INVENTORY_CONCURRENCY, items.length) }, () => run()),
  );

  return results as R[];
}

function mapInventoryResource(entry: ResourceEntryWithOperation): ResourceInventoryResource {
  const resource: ResourceInventoryResource = {
    resource: entry.resource,
  };

  if (entry.estimate_complete !== undefined) resource.estimateComplete = entry.estimate_complete;
  if (entry.estimated_record_count !== undefined) {
    resource.estimatedRecordCount = entry.estimated_record_count;
  }
  if (entry.estimated_storage_bytes !== undefined) {
    resource.estimatedStorageBytes = entry.estimated_storage_bytes;
  }
  if (entry.operation !== undefined) resource.operation = entry.operation;
  if (entry.read_latency_avg_ms !== undefined) {
    resource.readLatencyAvgMs = entry.read_latency_avg_ms;
  }
  if (entry.read_latency_p95_ms !== undefined) {
    resource.readLatencyP95Ms = entry.read_latency_p95_ms;
  }
  if (entry.transactions_active !== undefined) {
    resource.transactionsActive = entry.transactions_active;
  }
  if (entry.write_latency_avg_ms !== undefined) {
    resource.writeLatencyAvgMs = entry.write_latency_avg_ms;
  }
  if (entry.write_latency_p95_ms !== undefined) {
    resource.writeLatencyP95Ms = entry.write_latency_p95_ms;
  }

  return resource;
}

export async function getResourceInventory(
  domain: DomainId,
  options: ServiceRequestOptions = {},
): Promise<ResourceInventory> {
  const adapter = getResourceInventoryAdapter(domain);
  const realms = await adapter.listRealms(options);
  const inventoryRealms = await mapWithConcurrency(realms, async ({ realm }) => {
    const areas = await adapter.listAreas(realm, options);
    const inventoryAreas = await mapWithConcurrency(areas, async ({ area }) => {
      const resourceEntries = (await adapter.listResources({ area, realm }, options)).map(
        mapInventoryResource,
      );

      return {
        area,
        resourceEntries,
        resources: resourceEntries.map((entry) => entry.resource),
      };
    });

    return { areas: inventoryAreas, realm };
  });

  return { domain, realms: inventoryRealms };
}

export const resourceService = {
  getResourceInventory,
};
