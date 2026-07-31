import type { ServiceRequestOptions } from "@/shared/errors/api";
import {
  getResourceInventoryAdapter,
  type InventoryResourceEntry,
} from "./resource-domain-adapters";
import type { DomainId, ResourceInventory, ResourceInventoryResource } from "./resource-models";

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

export function mapInventoryResource(entry: InventoryResourceEntry): ResourceInventoryResource {
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
  if ("active_leases" in entry) resource.activeLeases = entry.active_leases;
  if ("committed_event_count" in entry) {
    resource.committedEventCount = entry.committed_event_count;
  }
  if ("next_run" in entry) resource.nextRun = entry.next_run;
  if ("notifications_received" in entry) {
    resource.notificationsReceived = entry.notifications_received;
  }
  if ("oldest_lease_age_seconds" in entry) {
    resource.oldestLeaseAgeSeconds = entry.oldest_lease_age_seconds;
  }
  if ("pending_claims" in entry) resource.pendingClaims = entry.pending_claims;
  if ("publishes_per_minute" in entry) {
    resource.publishesPerMinute = entry.publishes_per_minute;
  }
  if ("requests_pending" in entry) resource.requestsPending = entry.requests_pending;
  if ("schedules_active" in entry) resource.schedulesActive = entry.schedules_active;
  if ("sessions_active" in entry) resource.sessionsActive = entry.sessions_active;
  if ("size_bytes" in entry) resource.sizeBytes = entry.size_bytes;
  if ("slowest_worker_average_latency_ms" in entry) {
    resource.slowestWorkerAverageLatencyMs = entry.slowest_worker_average_latency_ms;
  }
  if ("subscriptions_active" in entry) {
    resource.subscriptionsActive = entry.subscriptions_active;
  }
  if ("waiters" in entry) resource.waiters = entry.waiters;
  if ("workers_registered" in entry) resource.workersRegistered = entry.workers_registered;

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
