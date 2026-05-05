import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapLeaseOverview } from "./lease-mappers";
import type { LeaseOverview } from "./lease-models";

async function getOverview(options: ServiceRequestOptions = {}): Promise<LeaseOverview> {
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listLeaseRealms(options),
    apiv1.getLeaseStats(options),
  ]);

  return mapLeaseOverview(
    unwrapResponse(realmsResponse, "Unable to load lease realms").realms,
    unwrapResponse(statsResponse, "Unable to load lease statistics"),
  );
}

export const leaseService = {
  getOverview,
};
