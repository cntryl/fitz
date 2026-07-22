import { apiParams, apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapMessagingTopology } from "./topology-mappers";
import type { MessagingTopologyOverview } from "./topology-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

async function getOverview(
  family = currentRouteFamilySegment(),
  options: ServiceRequestOptions = {},
): Promise<MessagingTopologyOverview> {
  const response = await apiv1.getFamilyTopology(apiParams({ family }, options));

  return mapMessagingTopology(unwrapResponse(response, "Unable to load messaging topology"));
}

export const topologyService = {
  getOverview,
};
