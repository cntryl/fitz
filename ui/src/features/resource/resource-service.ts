import { apiv1, type ResourceEntry } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "@/shared/navigation/domains";
import {
  mapKvTransactions,
  mapNoticeSubscriptions,
  mapResourceDetail,
  mapRpcOperations,
  mapRpcPending,
  mapRpcWorkers,
  mapStreamWatermarks,
} from "./resource-mappers";
import type {
  DomainId,
  ResourceDetail,
  ResourceInventory,
  ResourceInventoryResource,
  ResourceRef,
} from "./resource-models";

async function listRealms(domain: DomainId, options: ServiceRequestOptions) {
  const family = apiRouteFamilySegment();

  switch (domain) {
    case "kv":
      return unwrapResponse(await apiv1.listKvRealms(family, options), "Unable to load KV realms")
        .realms;
    case "stream":
      return unwrapResponse(
        await apiv1.listStreamRealms(family, options),
        "Unable to load stream realms",
      ).realms;
    case "lease":
      return unwrapResponse(
        await apiv1.listLeaseRealms(family, options),
        "Unable to load lease realms",
      ).realms;
    case "schedule":
      return unwrapResponse(
        await apiv1.listScheduleRealms(family, options),
        "Unable to load schedule realms",
      ).realms;
    case "notice":
      return unwrapResponse(
        await apiv1.listNoticeRealms(family, options),
        "Unable to load notice realms",
      ).realms;
    case "rpc":
      return unwrapResponse(await apiv1.listRpcRealms(family, options), "Unable to load RPC realms")
        .realms;
  }
}

async function listAreas(domain: DomainId, realm: string, options: ServiceRequestOptions) {
  const family = apiRouteFamilySegment();

  switch (domain) {
    case "kv":
      return unwrapResponse(
        await apiv1.listKvAreas(family, realm, options),
        "Unable to load KV areas",
      ).areas;
    case "stream":
      return unwrapResponse(
        await apiv1.listStreamAreas(family, realm, options),
        "Unable to load stream areas",
      ).areas;
    case "lease":
      return unwrapResponse(
        await apiv1.listLeaseAreas(family, realm, options),
        "Unable to load lease areas",
      ).areas;
    case "schedule":
      return unwrapResponse(
        await apiv1.listScheduleAreas(family, realm, options),
        "Unable to load schedule areas",
      ).areas;
    case "notice":
      return unwrapResponse(
        await apiv1.listNoticeAreas(family, realm, options),
        "Unable to load notice areas",
      ).areas;
    case "rpc":
      return unwrapResponse(
        await apiv1.listRpcAreas(family, realm, options),
        "Unable to load RPC areas",
      ).areas;
  }
}

async function listResources(
  domain: DomainId,
  ref: Omit<ResourceRef, "resource">,
  options: ServiceRequestOptions,
) {
  const family = apiRouteFamilySegment();

  switch (domain) {
    case "kv":
      return unwrapResponse(
        await apiv1.listKvResources(family, ref.realm, ref.area, options),
        "Unable to load KV resources",
      ).resources;
    case "stream":
      return unwrapResponse(
        await apiv1.listStreamResources(family, ref.realm, ref.area, options),
        "Unable to load stream resources",
      ).resources;
    case "lease":
      return unwrapResponse(
        await apiv1.listLeaseResources(family, ref.realm, ref.area, options),
        "Unable to load lease resources",
      ).resources;
    case "schedule":
      return unwrapResponse(
        await apiv1.listScheduleResources(family, ref.realm, ref.area, options),
        "Unable to load schedule resources",
      ).resources;
    case "notice":
      return unwrapResponse(
        await apiv1.listNoticeResources(family, ref.realm, ref.area, options),
        "Unable to load notice resources",
      ).resources;
    case "rpc":
      return unwrapResponse(
        await apiv1.listRpcResources(family, ref.realm, ref.area, options),
        "Unable to load RPC resources",
      ).resources;
  }
}

async function getResourceDetail(
  domain: DomainId,
  ref: ResourceRef,
  options: ServiceRequestOptions,
) {
  const family = apiRouteFamilySegment();

  switch (domain) {
    case "kv":
      return unwrapResponse(
        await apiv1.getKvResource(family, ref.realm, ref.area, ref.resource, options),
        "Unable to load KV resource",
      );
    case "stream":
      return unwrapResponse(
        await apiv1.getStreamResource(family, ref.realm, ref.area, ref.resource, options),
        "Unable to load stream resource",
      );
    case "lease":
      return unwrapResponse(
        await apiv1.getLeaseResource(family, ref.realm, ref.area, ref.resource, options),
        "Unable to load lease resource",
      );
    case "schedule":
      return unwrapResponse(
        await apiv1.getScheduleResource(family, ref.realm, ref.area, ref.resource, options),
        "Unable to load schedule resource",
      );
    case "notice":
      return unwrapResponse(
        await apiv1.getNoticeResource(family, ref.realm, ref.area, ref.resource, options),
        "Unable to load notice resource",
      );
    case "rpc":
      return unwrapResponse(
        await apiv1.getRpcResource(family, ref.realm, ref.area, ref.resource, options),
        "Unable to load RPC resource",
      );
  }
}

async function getTimeline(domain: DomainId, ref: ResourceRef, options: ServiceRequestOptions) {
  const family = apiRouteFamilySegment();

  switch (domain) {
    case "kv":
      return unwrapResponse(
        await apiv1.listKvResourceEvents(
          family,
          ref.realm,
          ref.area,
          ref.resource,
          { limit: 20 },
          options,
        ),
        "Unable to load KV timeline",
      );
    case "stream":
      return unwrapResponse(
        await apiv1.listStreamResourceEvents(
          family,
          ref.realm,
          ref.area,
          ref.resource,
          { limit: 20 },
          options,
        ),
        "Unable to load stream timeline",
      );
    case "lease":
      return unwrapResponse(
        await apiv1.listLeaseResourceEvents(
          family,
          ref.realm,
          ref.area,
          ref.resource,
          { limit: 20 },
          options,
        ),
        "Unable to load lease timeline",
      );
    case "schedule":
      return unwrapResponse(
        await apiv1.listScheduleResourceEvents(
          family,
          ref.realm,
          ref.area,
          ref.resource,
          { limit: 20 },
          options,
        ),
        "Unable to load schedule timeline",
      );
    case "notice":
      return unwrapResponse(
        await apiv1.listNoticeResourceEvents(
          family,
          ref.realm,
          ref.area,
          ref.resource,
          { limit: 20 },
          options,
        ),
        "Unable to load notice timeline",
      );
    case "rpc":
      return unwrapResponse(
        await apiv1.listRpcResourceEvents(
          family,
          ref.realm,
          ref.area,
          ref.resource,
          { limit: 20 },
          options,
        ),
        "Unable to load RPC timeline",
      );
  }
}

async function compare(
  domain: DomainId,
  ref: ResourceRef,
  against: ResourceRef | null,
  options: ServiceRequestOptions,
) {
  if (!against) return undefined;

  const family = apiRouteFamilySegment();
  const query = {
    against_area: against.area,
    against_realm: against.realm,
    against_resource: against.resource,
  };

  switch (domain) {
    case "kv":
      return unwrapResponse(
        await apiv1.compareKvResourceSnapshots(
          family,
          ref.realm,
          ref.area,
          ref.resource,
          query,
          options,
        ),
        "Unable to compare KV resource",
      );
    case "stream":
      return unwrapResponse(
        await apiv1.compareStreamResourceSnapshots(
          family,
          ref.realm,
          ref.area,
          ref.resource,
          query,
          options,
        ),
        "Unable to compare stream resource",
      );
    case "lease":
      return unwrapResponse(
        await apiv1.compareLeaseResourceSnapshots(
          family,
          ref.realm,
          ref.area,
          ref.resource,
          query,
          options,
        ),
        "Unable to compare lease resource",
      );
    case "schedule":
      return unwrapResponse(
        await apiv1.compareScheduleResourceSnapshots(
          family,
          ref.realm,
          ref.area,
          ref.resource,
          query,
          options,
        ),
        "Unable to compare schedule resource",
      );
    case "notice":
      return unwrapResponse(
        await apiv1.compareNoticeResourceSnapshots(
          family,
          ref.realm,
          ref.area,
          ref.resource,
          query,
          options,
        ),
        "Unable to compare notice resource",
      );
    case "rpc":
      return unwrapResponse(
        await apiv1.compareRpcResourceSnapshots(
          family,
          ref.realm,
          ref.area,
          ref.resource,
          query,
          options,
        ),
        "Unable to compare RPC resource",
      );
  }
}

async function getRelated(domain: DomainId, ref: ResourceRef, options: ServiceRequestOptions) {
  const family = apiRouteFamilySegment();

  switch (domain) {
    case "kv":
      return [
        mapKvTransactions(
          unwrapResponse(
            await apiv1.listKvTransactions(family, ref.realm, ref.area, ref.resource, options),
            "Unable to load KV transactions",
          ).transactions,
        ),
      ];
    case "stream":
      return [
        mapStreamWatermarks(
          unwrapResponse(
            await apiv1.getStreamAreaWatermarks(family, ref.realm, ref.area, options),
            "Unable to load stream watermarks",
          ).family_watermarks,
        ),
      ];
    case "notice":
      return [
        mapNoticeSubscriptions(
          unwrapResponse(
            await apiv1.listNoticeSubscriptions(family, ref.realm, ref.area, ref.resource, options),
            "Unable to load notice subscriptions",
          ).subscriptions,
        ),
      ];
    case "rpc": {
      const operations = unwrapResponse(
        await apiv1.listRpcOperations(family, ref.realm, ref.area, ref.resource, options),
        "Unable to load RPC operations",
      ).operations.map((entry) => entry.operation);
      const pending = unwrapResponse(
        await apiv1.listRpcPendingRequests(family, { realm: ref.realm }, options),
        "Unable to load RPC pending requests",
      ).requests;
      const firstOperation = operations[0];
      const workers = firstOperation
        ? unwrapResponse(
            await apiv1.listRpcOperationWorkers(
              family,
              ref.realm,
              ref.area,
              ref.resource,
              firstOperation,
              options,
            ),
            "Unable to load RPC workers",
          ).workers
        : [];
      return [mapRpcOperations(operations), mapRpcWorkers(workers), mapRpcPending(pending)];
    }
    case "lease":
    case "schedule":
      return [];
  }
}

function mapInventoryResource(entry: ResourceEntry): ResourceInventoryResource {
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
  if (entry.read_latency_avg_ms !== undefined)
    resource.readLatencyAvgMs = entry.read_latency_avg_ms;
  if (entry.read_latency_p95_ms !== undefined)
    resource.readLatencyP95Ms = entry.read_latency_p95_ms;
  if (entry.transactions_active !== undefined)
    resource.transactionsActive = entry.transactions_active;
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
  const realms = await listRealms(domain, options);
  const inventoryRealms = await Promise.all(
    realms.map(async ({ realm }) => {
      const areas = await listAreas(domain, realm, options);
      const inventoryAreas = await Promise.all(
        areas.map(async ({ area }) => {
          const resourceEntries = (await listResources(domain, { area, realm }, options)).map(
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

export async function getResource(
  domain: DomainId,
  ref: ResourceRef,
  against: ResourceRef | null,
  options: ServiceRequestOptions = {},
): Promise<ResourceDetail> {
  const [detail, timeline, comparison, related] = await Promise.all([
    getResourceDetail(domain, ref, options),
    getTimeline(domain, ref, options),
    compare(domain, ref, against, options),
    getRelated(domain, ref, options),
  ]);

  return mapResourceDetail({
    comparison,
    detail,
    domain,
    raw: { comparison, detail, related, timeline },
    ref,
    related,
    timeline,
  });
}

export const resourceService = {
  getResource,
  getResourceInventory,
};
