import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
import { leaseService } from "./lease-service";
import {
  currentRouteFamilySegment,
  DEFAULT_ROUTE_FAMILY_SEGMENT,
} from "@/shared/navigation/domains";
import type {
  LeaseAreaResourceRows,
  LeaseOverview,
  LeaseOwnershipSearchResult,
  LeaseRealmInventory,
} from "./lease-models";

const leaseQueries = queryScope("lease");

export const LEASE_OVERVIEW_KEY = leaseQueries.key("overview", DEFAULT_ROUTE_FAMILY_SEGMENT);
export const LEASE_INVENTORY_KEY = LEASE_OVERVIEW_KEY;

export function leaseOverviewQueryKey(family = currentRouteFamilySegment()) {
  return leaseQueries.key("overview", family);
}

export function leaseRealmQueryKey(realm: string, family = currentRouteFamilySegment()) {
  return leaseQueries.key("realm", family, realm);
}

export function leaseAreaQueryKey(
  realm: string,
  area: string,
  family = currentRouteFamilySegment(),
) {
  return leaseQueries.key("area", family, realm, area);
}

export function leaseResourceRowsQueryKey(
  realm: string,
  area: string,
  resource: string,
  limit = 0,
  family = currentRouteFamilySegment(),
) {
  return leaseQueries.key("resource-rows", family, realm, area, resource, String(limit ?? 0));
}

const leaseOverviewQuery = defineQuery<{ family: string }, LeaseOverview>({
  key: ({ family }) => leaseOverviewQueryKey(family),
  fetch: ({ signal }) => leaseService.getOverview({ signal }),
});

const leaseRealmQuery = defineQuery<{ family: string; realm: string }, LeaseRealmInventory>({
  key: ({ family, realm }) => leaseRealmQueryKey(realm, family),
  fetch: ({ realm, signal }) => leaseService.listRealmResources(realm, { signal }),
});

const leaseAreaQuery = defineQuery<
  { area: string; family: string; realm: string },
  LeaseAreaResourceRows
>({
  key: ({ area, family, realm }) => leaseAreaQueryKey(realm, area, family),
  fetch: ({ area, realm, signal }) => leaseService.listAreaResources(realm, area, { signal }),
});

interface LeaseResourceRowsQueryInput {
  area: string;
  family: string;
  limit: number;
  realm: string;
  resource: string;
}

const leaseResourceRowsQuery = defineQuery<LeaseResourceRowsQueryInput, LeaseOwnershipSearchResult>(
  {
    key: ({ area, family, limit, realm, resource }) =>
      leaseResourceRowsQueryKey(realm, area, resource, limit, family),
    fetch: ({ area, limit, realm, resource, signal }) =>
      leaseService.searchRows({ area, limit, realm, resource }, { signal }),
  },
);

export function createLeaseOverviewQuery() {
  return createQuery(leaseOverviewQuery, { family: currentRouteFamilySegment() });
}

export function createLeaseRealmQuery(realm: string) {
  return createQuery(leaseRealmQuery, { family: currentRouteFamilySegment(), realm });
}

export function createLeaseAreaQuery(realm: string, area: string) {
  return createQuery(leaseAreaQuery, { area, family: currentRouteFamilySegment(), realm });
}

export function createLeaseResourceRowsQuery(
  request: {
    realm: string;
    area: string;
    resource: string;
    limit?: number;
  },
  options?: { skipInitialFetch?: boolean },
) {
  const limit = request.limit ?? 50;
  return createQuery(
    leaseResourceRowsQuery,
    {
      ...request,
      family: currentRouteFamilySegment(),
      limit,
    },
    options,
  );
}
