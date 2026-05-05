import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapKvOverview } from "./kv-mappers";
import type { KvOverview } from "./kv-models";

async function getOverview(options: ServiceRequestOptions = {}): Promise<KvOverview> {
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listKvRealms(options),
    apiv1.getKvStats(options),
  ]);

  return mapKvOverview(
    unwrapResponse(realmsResponse, "Unable to load KV realms").realms,
    unwrapResponse(statsResponse, "Unable to load KV statistics"),
  );
}

export const kvService = {
  getOverview,
};
