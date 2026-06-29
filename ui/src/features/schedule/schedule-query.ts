import { createQuery, queryScope } from "@askrjs/askr/data";
import { stableQueryFetch, type QueryFetch } from "@/shared/query-fetch";
import { scheduleService } from "./schedule-service";
import type { ScheduleExecutionObservationList, ScheduleMissedObservationList } from "@/adapters";
import type {
  ScheduleAreaInventory,
  ScheduleOverview,
  ScheduleRealmInventory,
  ScheduleResourceView,
} from "./schedule-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const scheduleQueries = queryScope("schedule");
const scheduleRealmFetches = new Map<string, QueryFetch<ScheduleRealmInventory>>();
const scheduleAreaFetches = new Map<string, QueryFetch<ScheduleAreaInventory>>();
const scheduleResourceFetches = new Map<string, QueryFetch<ScheduleResourceView>>();
const scheduleExecutionFetches = new Map<string, QueryFetch<ScheduleExecutionObservationList>>();
const scheduleMissedFetches = new Map<string, QueryFetch<ScheduleMissedObservationList>>();

export function scheduleRealmQueryKey(realm: string, family = currentRouteFamilySegment()) {
  return scheduleQueries.key("realm", family, realm);
}

export function scheduleAreaQueryKey(
  realm: string,
  area: string,
  family = currentRouteFamilySegment(),
) {
  return scheduleQueries.key("area", family, realm, area);
}

export function scheduleResourceQueryKey(
  request: { area: string; limit?: number; realm: string; resource: string },
  family = currentRouteFamilySegment(),
) {
  return scheduleQueries.key(
    "resource",
    family,
    request.realm,
    request.area,
    request.resource,
    String(request.limit ?? 20),
  );
}

export function scheduleExecutionObservationsQueryKey(
  request: { area: string; limit?: number; realm: string; resource: string },
  family = currentRouteFamilySegment(),
) {
  return scheduleQueries.key(
    "execution-observations",
    family,
    request.realm,
    request.area,
    request.resource,
    String(request.limit ?? 20),
  );
}

export function scheduleMissedHandoffsQueryKey(
  request: { area?: string; limit?: number; realm?: string; resource?: string },
  family = currentRouteFamilySegment(),
) {
  return scheduleQueries.key(
    "missed-handoffs",
    family,
    request.realm ?? "",
    request.area ?? "",
    request.resource ?? "",
    String(request.limit ?? 20),
  );
}

export function createScheduleOverviewQuery() {
  const key = scheduleQueries.key("overview", currentRouteFamilySegment());

  return createQuery<ScheduleOverview>({
    key,
    fetch: scheduleService.getOverview,
  });
}

export function createScheduleRealmQuery(realm: string) {
  const key = scheduleRealmQueryKey(realm);

  return createQuery<ScheduleRealmInventory>({
    key,
    fetch: stableQueryFetch(
      scheduleRealmFetches,
      key,
      () =>
        ({ signal }) =>
          scheduleService.listScheduleAreas(realm, { signal }),
    ),
  });
}

export function createScheduleAreaQuery(realm: string, area: string) {
  const key = scheduleAreaQueryKey(realm, area);

  return createQuery<ScheduleAreaInventory>({
    key,
    fetch: stableQueryFetch(
      scheduleAreaFetches,
      key,
      () =>
        ({ signal }) =>
          scheduleService.listScheduleResources(realm, area, { signal }),
    ),
  });
}

export function createScheduleResourceQuery(request: {
  area: string;
  limit?: number;
  realm: string;
  resource: string;
}) {
  const limit = request.limit ?? 20;
  const key = scheduleResourceQueryKey({ ...request, limit });

  return createQuery<ScheduleResourceView>({
    key,
    fetch: stableQueryFetch(
      scheduleResourceFetches,
      key,
      () =>
        ({ signal }) =>
          scheduleService.getScheduleResource(
            { ...request, limit, routeFamily: currentRouteFamilySegment() },
            { signal },
          ),
    ),
  });
}

export function createScheduleExecutionObservationsQuery(request: {
  area: string;
  limit?: number;
  realm: string;
  resource: string;
}) {
  const limit = request.limit ?? 20;
  const key = scheduleExecutionObservationsQueryKey({ ...request, limit });

  return createQuery<ScheduleExecutionObservationList>({
    key,
    fetch: stableQueryFetch(
      scheduleExecutionFetches,
      key,
      () =>
        ({ signal }) =>
          scheduleService.listExecutionObservations(
            { ...request, limit, routeFamily: currentRouteFamilySegment() },
            { signal },
          ),
    ),
  });
}

export function createScheduleMissedHandoffsQuery(request: {
  area?: string;
  limit?: number;
  realm?: string;
  resource?: string;
}) {
  const limit = request.limit ?? 20;
  const key = scheduleMissedHandoffsQueryKey({ ...request, limit });

  return createQuery<ScheduleMissedObservationList>({
    key,
    fetch: stableQueryFetch(
      scheduleMissedFetches,
      key,
      () =>
        ({ signal }) =>
          scheduleService.searchMissedHandoffs(
            { ...request, limit, routeFamily: currentRouteFamilySegment() },
            { signal },
          ),
    ),
  });
}
