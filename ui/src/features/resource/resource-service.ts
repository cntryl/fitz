import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import {
  mapKvTransactions,
  mapNoticeSubscriptions,
  mapResourceDetail,
  mapRpcOperations,
  mapRpcPending,
  mapRpcWorkers,
  mapStreamWatermarks,
} from "./resource-mappers";
import type { DomainId, ResourceDetail, ResourceInventory, ResourceRef } from "./resource-models";

async function listRealms(domain: DomainId, options: ServiceRequestOptions) {
  switch (domain) {
    case "kv": return unwrapResponse(await apiv1.listKvRealms(options), "Unable to load KV realms").realms;
    case "stream": return unwrapResponse(await apiv1.listStreamRealms(options), "Unable to load stream realms").realms;
    case "lease": return unwrapResponse(await apiv1.listLeaseRealms(options), "Unable to load lease realms").realms;
    case "schedule": return unwrapResponse(await apiv1.listScheduleRealms(options), "Unable to load schedule realms").realms;
    case "notice": return unwrapResponse(await apiv1.listNoticeRealms(options), "Unable to load notice realms").realms;
    case "rpc": return unwrapResponse(await apiv1.listRpcRealms(options), "Unable to load RPC realms").realms;
  }
}

async function listAreas(domain: DomainId, realm: string, options: ServiceRequestOptions) {
  switch (domain) {
    case "kv": return unwrapResponse(await apiv1.listKvAreas(realm, options), "Unable to load KV areas").areas;
    case "stream": return unwrapResponse(await apiv1.listStreamAreas(realm, options), "Unable to load stream areas").areas;
    case "lease": return unwrapResponse(await apiv1.listLeaseAreas(realm, options), "Unable to load lease areas").areas;
    case "schedule": return unwrapResponse(await apiv1.listScheduleAreas(realm, options), "Unable to load schedule areas").areas;
    case "notice": return unwrapResponse(await apiv1.listNoticeAreas(realm, options), "Unable to load notice areas").areas;
    case "rpc": return unwrapResponse(await apiv1.listRpcAreas(realm, options), "Unable to load RPC areas").areas;
  }
}

async function listResources(domain: DomainId, ref: Omit<ResourceRef, "resource">, options: ServiceRequestOptions) {
  switch (domain) {
    case "kv": return unwrapResponse(await apiv1.listKvResources(ref.realm, ref.area, options), "Unable to load KV resources").resources;
    case "stream": return unwrapResponse(await apiv1.listStreamResources(ref.realm, ref.area, options), "Unable to load stream resources").resources;
    case "lease": return unwrapResponse(await apiv1.listLeaseResources(ref.realm, ref.area, options), "Unable to load lease resources").resources;
    case "schedule": return unwrapResponse(await apiv1.listScheduleResources(ref.realm, ref.area, options), "Unable to load schedule resources").resources;
    case "notice": return unwrapResponse(await apiv1.listNoticeResources(ref.realm, ref.area, options), "Unable to load notice resources").resources;
    case "rpc": return unwrapResponse(await apiv1.listRpcResources(ref.realm, ref.area, options), "Unable to load RPC resources").resources;
  }
}

async function getResourceDetail(domain: DomainId, ref: ResourceRef, options: ServiceRequestOptions) {
  switch (domain) {
    case "kv": return unwrapResponse(await apiv1.getKvResource(ref.realm, ref.area, ref.resource, options), "Unable to load KV resource");
    case "stream": return unwrapResponse(await apiv1.getStreamResource(ref.realm, ref.area, ref.resource, options), "Unable to load stream resource");
    case "lease": return unwrapResponse(await apiv1.getLeaseResource(ref.realm, ref.area, ref.resource, options), "Unable to load lease resource");
    case "schedule": return unwrapResponse(await apiv1.getScheduleResource(ref.realm, ref.area, ref.resource, options), "Unable to load schedule resource");
    case "notice": return unwrapResponse(await apiv1.getNoticeResource(ref.realm, ref.area, ref.resource, options), "Unable to load notice resource");
    case "rpc": return unwrapResponse(await apiv1.getRpcResource(ref.realm, ref.area, ref.resource, options), "Unable to load RPC resource");
  }
}

async function getTimeline(domain: DomainId, ref: ResourceRef, options: ServiceRequestOptions) {
  switch (domain) {
    case "kv": return unwrapResponse(await apiv1.listKvResourceEvents(ref.realm, ref.area, ref.resource, { limit: 20 }, options), "Unable to load KV timeline");
    case "stream": return unwrapResponse(await apiv1.listStreamResourceEvents(ref.realm, ref.area, ref.resource, { limit: 20 }, options), "Unable to load stream timeline");
    case "lease": return unwrapResponse(await apiv1.listLeaseResourceEvents(ref.realm, ref.area, ref.resource, { limit: 20 }, options), "Unable to load lease timeline");
    case "schedule": return unwrapResponse(await apiv1.listScheduleResourceEvents(ref.realm, ref.area, ref.resource, { limit: 20 }, options), "Unable to load schedule timeline");
    case "notice": return unwrapResponse(await apiv1.listNoticeResourceEvents(ref.realm, ref.area, ref.resource, { limit: 20 }, options), "Unable to load notice timeline");
    case "rpc": return unwrapResponse(await apiv1.listRpcResourceEvents(ref.realm, ref.area, ref.resource, { limit: 20 }, options), "Unable to load RPC timeline");
  }
}

async function compare(domain: DomainId, ref: ResourceRef, against: ResourceRef | null, options: ServiceRequestOptions) {
  if (!against) return undefined;

  const query = {
    against_area: against.area,
    against_realm: against.realm,
    against_resource: against.resource,
  };

  switch (domain) {
    case "kv": return unwrapResponse(await apiv1.compareKvResourceSnapshots(ref.realm, ref.area, ref.resource, query, options), "Unable to compare KV resource");
    case "stream": return unwrapResponse(await apiv1.compareStreamResourceSnapshots(ref.realm, ref.area, ref.resource, query, options), "Unable to compare stream resource");
    case "lease": return unwrapResponse(await apiv1.compareLeaseResourceSnapshots(ref.realm, ref.area, ref.resource, query, options), "Unable to compare lease resource");
    case "schedule": return unwrapResponse(await apiv1.compareScheduleResourceSnapshots(ref.realm, ref.area, ref.resource, query, options), "Unable to compare schedule resource");
    case "notice": return unwrapResponse(await apiv1.compareNoticeResourceSnapshots(ref.realm, ref.area, ref.resource, query, options), "Unable to compare notice resource");
    case "rpc": return unwrapResponse(await apiv1.compareRpcResourceSnapshots(ref.realm, ref.area, ref.resource, query, options), "Unable to compare RPC resource");
  }
}

async function getRelated(domain: DomainId, ref: ResourceRef, options: ServiceRequestOptions) {
  switch (domain) {
    case "kv":
      return [mapKvTransactions(unwrapResponse(await apiv1.listKvTransactions(ref.realm, ref.area, ref.resource, options), "Unable to load KV transactions").transactions)];
    case "stream":
      return [mapStreamWatermarks(unwrapResponse(await apiv1.getStreamAreaWatermarks(ref.realm, ref.area, options), "Unable to load stream watermarks").family_watermarks)];
    case "notice":
      return [mapNoticeSubscriptions(unwrapResponse(await apiv1.listNoticeSubscriptions(ref.realm, ref.area, ref.resource, options), "Unable to load notice subscriptions").subscriptions)];
    case "rpc": {
      const operations = unwrapResponse(await apiv1.listRpcOperations(ref.realm, ref.area, ref.resource, options), "Unable to load RPC operations").operations.map((entry) => entry.operation);
      const pending = unwrapResponse(await apiv1.listRpcPendingRequests({ realm: ref.realm }, options), "Unable to load RPC pending requests").requests;
      const firstOperation = operations[0];
      const workers = firstOperation
        ? unwrapResponse(await apiv1.listRpcOperationWorkers(ref.realm, ref.area, ref.resource, firstOperation, options), "Unable to load RPC workers").workers
        : [];
      return [mapRpcOperations(operations), mapRpcWorkers(workers), mapRpcPending(pending)];
    }
    case "lease":
    case "schedule":
      return [];
  }
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
        areas.map(async ({ area }) => ({
          area,
          resources: (await listResources(domain, { area, realm }, options)).map((entry) => entry.resource),
        })),
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
