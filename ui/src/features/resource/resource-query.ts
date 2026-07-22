import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
import { resourceService } from "./resource-service";
import type { DomainId, ResourceInventory } from "./resource-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const resourceQueries = queryScope("resource");
const resourceInventoryQuery = defineQuery<{ domain: DomainId; family: string }, ResourceInventory>(
  {
    key: ({ domain, family }) => resourceQueries.key("inventory", domain, family),
    fetch: ({ domain, signal }) => resourceService.getResourceInventory(domain, { signal }),
  },
);

export function createResourceInventoryQuery(domain: DomainId) {
  return createQuery(resourceInventoryQuery, { domain, family: currentRouteFamilySegment() });
}

export type { DomainId } from "./resource-models";
