import { createQuery, queryScope } from "@askrjs/askr/data";
import { leaseService } from "./lease-service";
import { stableQueryFetch, type QueryFetch } from "@/shared/query-fetch";
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
const leaseRealmFetches = new Map<string, QueryFetch<LeaseRealmInventory>>();
const leaseAreaFetches = new Map<string, QueryFetch<LeaseAreaResourceRows>>();
const leaseResourceRowsFetches = new Map<string, QueryFetch<LeaseOwnershipSearchResult>>();

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

export function createLeaseOverviewQuery() {
  const key = leaseOverviewQueryKey();

  return createQuery<LeaseOverview>({
    key,
    fetch: leaseService.getOverview,
  });
}

export function createLeaseRealmQuery(realm: string) {
  const key = leaseRealmQueryKey(realm);

  return createQuery<LeaseRealmInventory>({
    key,
    fetch: stableQueryFetch(
      leaseRealmFetches,
      key,
      () =>
        ({ signal }) =>
          leaseService.listRealmResources(realm, { signal }),
    ),
  });
}

export function createLeaseAreaQuery(realm: string, area: string) {
  const key = leaseAreaQueryKey(realm, area);

  return createQuery<LeaseAreaResourceRows>({
    key,
    fetch: stableQueryFetch(
      leaseAreaFetches,
      key,
      () =>
        ({ signal }) =>
          leaseService.listAreaResources(realm, area, { signal }),
    ),
  });
}

export function createLeaseResourceRowsQuery(request: {
  realm: string;
  area: string;
  resource: string;
  limit?: number;
}) {
  const limit = request.limit ?? 50;
  const key = leaseResourceRowsQueryKey(request.realm, request.area, request.resource, limit);

  return createQuery<LeaseOwnershipSearchResult>({
    key,
    fetch: stableQueryFetch(
      leaseResourceRowsFetches,
      key,
      () =>
        ({ signal }) =>
          leaseService.searchRows(
            {
              area: request.area,
              limit,
              realm: request.realm,
              resource: request.resource,
            },
            { signal },
          ),
    ),
  });
}
