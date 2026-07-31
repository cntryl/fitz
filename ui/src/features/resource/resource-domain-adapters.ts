import {
  apiParams,
  apiv1,
  type LeaseResourceEntry,
  type NoticeResourceEntry,
  type ResourceEntry,
  type RpcResourceEntry,
  type ScheduleResourceEntry,
  type StreamResourceEntry,
} from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "@/shared/navigation/domains";
import type { DomainId } from "./resource-models";

export type InventoryResourceEntry = Partial<
  ResourceEntry &
    LeaseResourceEntry &
    NoticeResourceEntry &
    RpcResourceEntry &
    ScheduleResourceEntry &
    StreamResourceEntry
> & { operation?: string; resource: string };

export interface ResourceInventoryAdapter {
  domain: DomainId;
  listRealms(options: ServiceRequestOptions): Promise<Array<{ realm: string }>>;
  listAreas(realm: string, options: ServiceRequestOptions): Promise<Array<{ area: string }>>;
  listResources(
    ref: { area: string; realm: string },
    options: ServiceRequestOptions,
  ): Promise<InventoryResourceEntry[]>;
}

function family() {
  return apiRouteFamilySegment();
}

export const resourceInventoryAdapterRegistry = {
  kv: {
    domain: "kv",
    async listRealms(options) {
      return unwrapResponse(
        await apiv1.listKvRealms(apiParams({ family: family() }, options)),
        "Unable to load KV realms",
      ).realms;
    },
    async listAreas(realm, options) {
      return unwrapResponse(
        await apiv1.listKvAreas(apiParams({ family: family(), realm }, options)),
        "Unable to load KV areas",
      ).areas;
    },
    async listResources(ref, options) {
      return unwrapResponse(
        await apiv1.listKvResources(
          apiParams({ area: ref.area, family: family(), realm: ref.realm }, options),
        ),
        "Unable to load KV resources",
      ).resources;
    },
  },
  lease: {
    domain: "lease",
    async listRealms(options) {
      return unwrapResponse(
        await apiv1.listLeaseRealms(apiParams({ family: family() }, options)),
        "Unable to load lease realms",
      ).realms;
    },
    async listAreas(realm, options) {
      return unwrapResponse(
        await apiv1.listLeaseAreas(apiParams({ family: family(), realm }, options)),
        "Unable to load lease areas",
      ).areas;
    },
    async listResources(ref, options) {
      return unwrapResponse(
        await apiv1.listLeaseResources(
          apiParams({ area: ref.area, family: family(), realm: ref.realm }, options),
        ),
        "Unable to load lease resources",
      ).resources;
    },
  },
  notice: {
    domain: "notice",
    async listRealms(options) {
      return unwrapResponse(
        await apiv1.listNoticeRealms(apiParams({ family: family() }, options)),
        "Unable to load notice realms",
      ).realms;
    },
    async listAreas(realm, options) {
      return unwrapResponse(
        await apiv1.listNoticeAreas(apiParams({ family: family(), realm }, options)),
        "Unable to load notice areas",
      ).areas;
    },
    async listResources(ref, options) {
      return unwrapResponse(
        await apiv1.listNoticeResources(
          apiParams({ area: ref.area, family: family(), realm: ref.realm }, options),
        ),
        "Unable to load notice resources",
      ).resources;
    },
  },
  rpc: {
    domain: "rpc",
    async listRealms(options) {
      return unwrapResponse(
        await apiv1.listRpcRealms(apiParams({ family: family() }, options)),
        "Unable to load RPC realms",
      ).realms;
    },
    async listAreas(realm, options) {
      return unwrapResponse(
        await apiv1.listRpcAreas(apiParams({ family: family(), realm }, options)),
        "Unable to load RPC areas",
      ).areas;
    },
    async listResources(ref, options) {
      return unwrapResponse(
        await apiv1.listRpcResources(
          apiParams({ area: ref.area, family: family(), realm: ref.realm }, options),
        ),
        "Unable to load RPC resources",
      ).resources;
    },
  },
  schedule: {
    domain: "schedule",
    async listRealms(options) {
      return unwrapResponse(
        await apiv1.listScheduleRealms(apiParams({ family: family() }, options)),
        "Unable to load schedule realms",
      ).realms;
    },
    async listAreas(realm, options) {
      return unwrapResponse(
        await apiv1.listScheduleAreas(apiParams({ family: family(), realm }, options)),
        "Unable to load schedule areas",
      ).areas;
    },
    async listResources(ref, options) {
      return unwrapResponse(
        await apiv1.listScheduleResources(
          apiParams({ area: ref.area, family: family(), realm: ref.realm }, options),
        ),
        "Unable to load schedule resources",
      ).resources;
    },
  },
  stream: {
    domain: "stream",
    async listRealms(options) {
      return unwrapResponse(
        await apiv1.listStreamRealms(apiParams({ family: family() }, options)),
        "Unable to load stream realms",
      ).realms;
    },
    async listAreas(realm, options) {
      return unwrapResponse(
        await apiv1.listStreamAreas(apiParams({ family: family(), realm }, options)),
        "Unable to load stream areas",
      ).areas;
    },
    async listResources(ref, options) {
      return unwrapResponse(
        await apiv1.listStreamResources(
          apiParams({ area: ref.area, family: family(), realm: ref.realm }, options),
        ),
        "Unable to load stream resources",
      ).resources;
    },
  },
} satisfies Record<DomainId, ResourceInventoryAdapter>;

export function getResourceInventoryAdapter(domain: DomainId): ResourceInventoryAdapter {
  return resourceInventoryAdapterRegistry[domain];
}
