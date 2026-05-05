import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapStreamOverview } from "./stream-mappers";
import type { StreamOverview } from "./stream-models";

async function getOverview(options: ServiceRequestOptions = {}): Promise<StreamOverview> {
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listStreamRealms(options),
    apiv1.getStreamStats(options),
  ]);

  return mapStreamOverview(
    unwrapResponse(realmsResponse, "Unable to load stream realms").realms,
    unwrapResponse(statsResponse, "Unable to load stream statistics"),
  );
}

export const streamService = {
  getOverview,
};
