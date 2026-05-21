import { createQuery } from "@askrjs/askr/data";
import { resourceService } from "./resource-service";
import type { DomainId, ResourceDetail, ResourceInventory, ResourceRef } from "./resource-models";

function resourceKey(domain: DomainId, ref: ResourceRef) {
  return `${domain}:resource:${ref.realm}:${ref.area}:${ref.resource}`;
}

export function createResourceInventoryQuery(domain: DomainId) {
  return createQuery<ResourceInventory>({
    key: `${domain}:inventory`,
    fetch: ({ signal }) => resourceService.getResourceInventory(domain, { signal }),
  });
}

export function createResourceQuery(domain: DomainId, ref: ResourceRef, against: ResourceRef | null) {
  return createQuery<ResourceDetail>({
    key: `${resourceKey(domain, ref)}:against:${against?.realm ?? "none"}:${against?.area ?? "none"}:${against?.resource ?? "none"}`,
    fetch: ({ signal }) => resourceService.getResource(domain, ref, against, { signal }),
  });
}

export type { DomainId, ResourceRef } from "./resource-models";
