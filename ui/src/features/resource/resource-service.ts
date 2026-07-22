import type { ResourceEntry } from "@/adapters";
import type { ServiceRequestOptions } from "@/shared/errors/api";
import { getResourceInventoryAdapter } from "./resource-domain-adapters";
import type { DomainId, ResourceInventory, ResourceInventoryResource } from "./resource-models";

type ResourceEntryWithOperation = ResourceEntry & { operation?: string };

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
  const inventoryRealms = await Promise.all(
    realms.map(async ({ realm }) => {
      const areas = await adapter.listAreas(realm, options);
      const inventoryAreas = await Promise.all(
        areas.map(async ({ area }) => {
          const resourceEntries = (await adapter.listResources({ area, realm }, options)).map(
            mapInventoryResource,
          );

          return {
            area,
            resourceEntries,
            resources: resourceEntries.map((entry) => entry.resource),
          };
        }),
      );

      return { areas: inventoryAreas, realm };
    }),
  );

  return { domain, realms: inventoryRealms };
}

export const resourceService = {
  getResourceInventory,
};
