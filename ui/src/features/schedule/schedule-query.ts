import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
import { scheduleService } from "./schedule-service";
import type { ScheduleExecutionObservationList, ScheduleMissedObservationList } from "@/adapters";
import type {
  ScheduleAreaInventory,
  ScheduleOverview,
  ScheduleOperationView,
  ScheduleRealmInventory,
  ScheduleResourceView,
} from "./schedule-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const scheduleQueries = queryScope("schedule");

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
  request: {
    area: string;
    limit?: number;
    offset?: number;
    realm: string;
    resource: string;
  },
  family = currentRouteFamilySegment(),
) {
  return scheduleQueries.key(
    "resource",
    family,
    request.realm,
    request.area,
    request.resource,
    String(request.offset ?? 0),
    String(request.limit ?? 20),
  );
}

export function scheduleOperationQueryKey(
  request: {
    area: string;
    limit?: number;
    operation: string;
    realm: string;
    resource: string;
  },
  family = currentRouteFamilySegment(),
) {
  return scheduleQueries.key(
    "operation",
    family,
    request.realm,
    request.area,
    request.resource,
    request.operation,
    String(request.limit ?? 20),
  );
}

export function scheduleExecutionObservationsQueryKey(
  request: {
    area: string;
    limit?: number;
    offset?: number;
    operation?: string;
    realm: string;
    resource: string;
  },
  family = currentRouteFamilySegment(),
) {
  return scheduleQueries.key(
    "execution-observations",
    family,
    request.realm,
    request.area,
    request.resource,
    request.operation ?? "",
    String(request.offset ?? 0),
    String(request.limit ?? 20),
  );
}

export function scheduleMissedHandoffsQueryKey(
  request: {
    area?: string;
    limit?: number;
    operation?: string;
    realm?: string;
    resource?: string;
  },
  family = currentRouteFamilySegment(),
) {
  return scheduleQueries.key(
    "missed-handoffs",
    family,
    request.realm ?? "",
    request.area ?? "",
    request.resource ?? "",
    request.operation ?? "",
    String(request.limit ?? 20),
  );
}

const scheduleOverviewQuery = defineQuery<{ family: string }, ScheduleOverview>({
  key: ({ family }) => scheduleQueries.key("overview", family),
  fetch: ({ signal }) => scheduleService.getOverview({ signal }),
});

const scheduleRealmQuery = defineQuery<{ family: string; realm: string }, ScheduleRealmInventory>({
  key: ({ family, realm }) => scheduleRealmQueryKey(realm, family),
  fetch: ({ realm, signal }) => scheduleService.listScheduleAreas(realm, { signal }),
});

const scheduleAreaQuery = defineQuery<
  { area: string; family: string; realm: string },
  ScheduleAreaInventory
>({
  key: ({ area, family, realm }) => scheduleAreaQueryKey(realm, area, family),
  fetch: ({ area, realm, signal }) =>
    scheduleService.listScheduleResources(realm, area, { signal }),
});

interface ScheduleResourceQueryInput {
  area: string;
  family: string;
  limit: number;
  offset: number;
  realm: string;
  resource: string;
}

const scheduleResourceQuery = defineQuery<ScheduleResourceQueryInput, ScheduleResourceView>({
  key: ({ family, ...request }) => scheduleResourceQueryKey(request, family),
  fetch: ({ family, signal, ...request }) =>
    scheduleService.getScheduleResource({ ...request, routeFamily: family }, { signal }),
});

interface ScheduleOperationQueryInput {
  area: string;
  family: string;
  limit: number;
  operation: string;
  realm: string;
  resource: string;
}

const scheduleOperationQuery = defineQuery<ScheduleOperationQueryInput, ScheduleOperationView>({
  key: ({ family, ...request }) => scheduleOperationQueryKey(request, family),
  fetch: ({ family, signal, ...request }) =>
    scheduleService.getScheduleOperation({ ...request, routeFamily: family }, { signal }),
});

interface ScheduleExecutionQueryInput extends ScheduleResourceQueryInput {
  operation?: string;
}

const scheduleExecutionQuery = defineQuery<
  ScheduleExecutionQueryInput,
  ScheduleExecutionObservationList
>({
  key: ({ family, ...request }) => scheduleExecutionObservationsQueryKey(request, family),
  fetch: ({ family, signal, ...request }) =>
    scheduleService.listExecutionObservations({ ...request, routeFamily: family }, { signal }),
});

interface ScheduleMissedQueryInput {
  area?: string;
  family: string;
  limit: number;
  operation?: string;
  realm?: string;
  resource?: string;
}

const scheduleMissedQuery = defineQuery<ScheduleMissedQueryInput, ScheduleMissedObservationList>({
  key: ({ family, ...request }) => scheduleMissedHandoffsQueryKey(request, family),
  fetch: ({ family, signal, ...request }) =>
    scheduleService.searchMissedHandoffs({ ...request, routeFamily: family }, { signal }),
});

export function createScheduleOverviewQuery() {
  return createQuery(scheduleOverviewQuery, { family: currentRouteFamilySegment() });
}

export function createScheduleRealmQuery(realm: string) {
  return createQuery(scheduleRealmQuery, { family: currentRouteFamilySegment(), realm });
}

export function createScheduleAreaQuery(realm: string, area: string) {
  return createQuery(scheduleAreaQuery, { area, family: currentRouteFamilySegment(), realm });
}

export function createScheduleResourceQuery(request: {
  area: string;
  limit?: number;
  offset?: number;
  realm: string;
  resource: string;
}) {
  const limit = request.limit ?? 20;
  const offset = request.offset ?? 0;
  return createQuery(scheduleResourceQuery, {
    ...request,
    family: currentRouteFamilySegment(),
    limit,
    offset,
  });
}

export function createScheduleOperationQuery(request: {
  area: string;
  limit?: number;
  operation: string;
  realm: string;
  resource: string;
}) {
  const limit = request.limit ?? 20;
  return createQuery(scheduleOperationQuery, {
    ...request,
    family: currentRouteFamilySegment(),
    limit,
  });
}

export function createScheduleExecutionObservationsQuery(request: {
  area: string;
  limit?: number;
  offset?: number;
  operation?: string;
  realm: string;
  resource: string;
}) {
  const limit = request.limit ?? 20;
  const offset = request.offset ?? 0;
  return createQuery(scheduleExecutionQuery, {
    ...request,
    family: currentRouteFamilySegment(),
    limit,
    offset,
  });
}

export function createScheduleMissedHandoffsQuery(request: {
  area?: string;
  limit?: number;
  operation?: string;
  realm?: string;
  resource?: string;
}) {
  const limit = request.limit ?? 20;
  return createQuery(scheduleMissedQuery, {
    ...request,
    family: currentRouteFamilySegment(),
    limit,
  });
}
