import { apiParams, apiParamsQuery, apiv1 } from "@/adapters";
import { unwrapResponse } from "@/shared/errors/api";
import {
  routeFamilyRequest,
  type RouteFamilyRequestOptions,
} from "@/shared/navigation/route-family-request";
import {
  mapQueueResourceComparison,
  mapQueueResourceOverview,
  mapQueueResourceTimeline,
} from "./queue-resource-mappers";
import type {
  QueueResourceComparison,
  QueueResourceOverview,
  QueueResourceRef,
  QueueResourceTimeline,
} from "./queue-resource-models";

async function getResource(
  resourceRef: QueueResourceRef,
  options: RouteFamilyRequestOptions = {},
): Promise<QueueResourceOverview> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const [detailResponse, inflightResponse, deadLettersResponse, timelineResponse] =
    await Promise.all([
      apiv1.getQueueResource(
        apiParams(
          {
            area: resourceRef.area,
            family,
            realm: resourceRef.realm,
            resource: resourceRef.resource,
          },
          requestOptions,
        ),
      ),
      apiv1.listQueueInflightEntries(
        apiParams(
          {
            area: resourceRef.area,
            family,
            realm: resourceRef.realm,
            resource: resourceRef.resource,
          },
          requestOptions,
        ),
      ),
      apiv1.listQueueDeadLetters(
        apiParams(
          {
            area: resourceRef.area,
            family,
            realm: resourceRef.realm,
            resource: resourceRef.resource,
          },
          requestOptions,
        ),
      ),
      apiv1.listQueueResourceEvents(
        apiParamsQuery(
          {
            area: resourceRef.area,
            family,
            realm: resourceRef.realm,
            resource: resourceRef.resource,
          },
          { limit: 8 },
          requestOptions,
        ),
      ),
    ]);

  return mapQueueResourceOverview(
    unwrapResponse(detailResponse, "Unable to load queue resource"),
    unwrapResponse(inflightResponse, "Unable to load queue inflight entries").inflight,
    unwrapResponse(deadLettersResponse, "Unable to load queue dead-letter messages").messages,
    unwrapResponse(timelineResponse, "Unable to load queue resource timeline"),
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
  getResource,
  getTimeline,
};
