import { createQuery, queryScope } from "@askrjs/askr/data";
import { resourceService } from "./resource-service";
import type { DomainId, ResourceDetail, ResourceInventory, ResourceRef } from "./resource-models";
import { stableQueryFetch, type QueryFetch } from "@/shared/query-fetch";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const resourceQueries = queryScope("resource");
const resourceInventoryFetches = new Map<string, QueryFetch<ResourceInventory>>();
const resourceDetailFetches = new Map<string, QueryFetch<ResourceDetail>>();

function resourceKey(domain: DomainId, ref: ResourceRef, against: ResourceRef | null) {
  return resourceQueries.key(
    "detail",
    domain,
    currentRouteFamilySegment(),
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
  const key = resourceQueries.key("inventory", domain, currentRouteFamilySegment());

  return createQuery<ResourceInventory>({
    key,
    fetch: stableQueryFetch(
      resourceInventoryFetches,
      key,
      () =>
        ({ signal }) =>
          resourceService.getResourceInventory(domain, { signal }),
    ),
  });
}

export function createResourceQuery(
  domain: DomainId,
  ref: ResourceRef,
  against: ResourceRef | null,
) {
  const key = resourceKey(domain, ref, against);

  return createQuery<ResourceDetail>({
    key,
    fetch: stableQueryFetch(
      resourceDetailFetches,
      key,
      () =>
        ({ signal }) =>
          resourceService.getResource(domain, ref, against, { signal }),
    ),
  });
}

export type { DomainId, ResourceRef } from "./resource-models";
