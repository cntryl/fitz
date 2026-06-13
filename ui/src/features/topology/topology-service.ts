import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapMessagingTopology } from "./topology-mappers";
import type { MessagingTopologyOverview } from "./topology-models";

async function getOverview(
  options: ServiceRequestOptions = {},
): Promise<MessagingTopologyOverview> {
  const response = await apiv1.getMessagingTopology(options);

  return mapMessagingTopology(unwrapResponse(response, "Unable to load messaging topology"));
}

export const topologyService = {
  getOverview,
};
