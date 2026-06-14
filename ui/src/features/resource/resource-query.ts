import { createQuery, queryScope } from "@askrjs/askr/data";
import { resourceService } from "./resource-service";
import type { DomainId, ResourceDetail, ResourceInventory, ResourceRef } from "./resource-models";

const resourceQueries = queryScope("resource");

function resourceKey(domain: DomainId, ref: ResourceRef, against: ResourceRef | null) {
  return resourceQueries.key(
    "detail",
    domain,
    ref.realm,
    ref.area,
    ref.resource,
    "against",
    against?.realm ?? "none",
    against?.area ?? "none",
    against?.resource ?? "none",
  );
}

export function createResourceInventoryQuery(domain: DomainId) {
  return createQuery<ResourceInventory>({
    key: resourceQueries.key("inventory", domain),
    fetch: ({ signal }) => resourceService.getResourceInventory(domain, { signal }),
  });
}

export function createResourceQuery(
  domain: DomainId,
  ref: ResourceRef,
  against: ResourceRef | null,
) {
  return createQuery<ResourceDetail>({
    key: resourceKey(domain, ref, against),
    fetch: ({ signal }) => resourceService.getResource(domain, ref, against, { signal }),
  });
}

export type { DomainId, ResourceRef } from "./resource-models";
