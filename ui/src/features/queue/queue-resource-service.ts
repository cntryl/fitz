import { apiParams, apiParamsQuery, apiv1 } from "@/adapters";
import { unwrapResponse } from "@/shared/errors/api";
import {
  routeFamilyRequest,
  type RouteFamilyRequestOptions,
} from "@/shared/navigation/route-family-request";
import {
  mapQueueResourceComparison,
  mapQueueInflight,
  mapQueueResourceDetail,
  mapQueueResourceTimeline,
} from "./queue-resource-mappers";
import type {
  QueueResourceComparison,
  QueueInflightMessage,
  QueueResourceDetail,
  QueueResourceRef,
  QueueResourceTimeline,
} from "./queue-resource-models";

async function getResource(
  resourceRef: QueueResourceRef,
  options: RouteFamilyRequestOptions = {},
): Promise<QueueResourceDetail> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const response = await apiv1.getQueueResource(
    apiParams(
      { area: resourceRef.area, family, realm: resourceRef.realm, resource: resourceRef.resource },
      requestOptions,
    ),
  );

  return mapQueueResourceDetail(unwrapResponse(response, "Unable to load queue resource"));
}

async function getInflight(
  resourceRef: QueueResourceRef,
  options: RouteFamilyRequestOptions = {},
): Promise<QueueInflightMessage[]> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const response = await apiv1.listQueueInflightEntries(
    apiParams(
      { area: resourceRef.area, family, realm: resourceRef.realm, resource: resourceRef.resource },
      requestOptions,
    ),
  );

  return unwrapResponse(response, "Unable to load queue inflight entries").inflight.map(
    mapQueueInflight,
  );
}

async function getTimeline(
  resourceRef: QueueResourceRef,
  options: RouteFamilyRequestOptions = {},
): Promise<QueueResourceTimeline> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const response = await apiv1.listQueueResourceEvents(
    apiParamsQuery(
      { area: resourceRef.area, family, realm: resourceRef.realm, resource: resourceRef.resource },
      { limit: 8 },
      requestOptions,
    ),
  );

  return mapQueueResourceTimeline(
    unwrapResponse(response, "Unable to load queue resource timeline"),
  );
}

async function compareResource(
  resourceRef: QueueResourceRef,
  againstResourceRef: QueueResourceRef & { family?: number | null },
  options: RouteFamilyRequestOptions = {},
): Promise<QueueResourceComparison> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const response = await apiv1.compareQueueResourceSnapshots(
    apiParamsQuery(
      {
        area: resourceRef.area,
        family,
        realm: resourceRef.realm,
        resource: resourceRef.resource,
      },
      {
        against_area: againstResourceRef.area,
        against_family: againstResourceRef.family ?? undefined,
        against_realm: againstResourceRef.realm,
        against_resource: againstResourceRef.resource,
      },
      requestOptions,
    ),
  );

  return mapQueueResourceComparison(
    unwrapResponse(response, "Unable to compare queue resource snapshots"),
  );
}

// Services are the app contract boundary: no Askr resources and no FetchResult leaks.
export const queueResourceService = {
  compareResource,
  getInflight,
  getResource,
  getTimeline,
};
