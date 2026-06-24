import { apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { mapKvCommittedValue, mapKvOverview, mapKvPrefixScan } from "./kv-mappers";
import type {
  KvCommittedResourceScope,
  KvCommittedValueResult,
  KvKeyEncoding,
  KvOverview,
  KvPrefixScanResult,
} from "./kv-models";

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

async function getCommittedValue(
  scope: KvCommittedResourceScope,
  key: string,
  keyEncoding: KvKeyEncoding,
  options: ServiceRequestOptions = {},
): Promise<KvCommittedValueResult> {
  return mapKvCommittedValue(
    unwrapResponse(
      await apiv1.getKvCommittedValue(
        scope.realm,
        scope.area,
        scope.resource,
        {
          key,
          key_encoding: keyEncoding,
          route_family: scope.routeFamily,
        },
        options,
      ),
      "Unable to load committed KV value",
    ),
  );
}

async function scanCommittedPrefix(
  scope: KvCommittedResourceScope,
  prefix: string,
  keyEncoding: KvKeyEncoding,
  limit = 50,
  options: ServiceRequestOptions = {},
): Promise<KvPrefixScanResult> {
  return mapKvPrefixScan(
    unwrapResponse(
      await apiv1.scanKvCommittedPrefix(
        scope.realm,
        scope.area,
        scope.resource,
        {
          key_encoding: keyEncoding,
          limit,
          prefix,
          route_family: scope.routeFamily,
        },
        options,
      ),
      "Unable to scan committed KV prefix",
    ),
  );
}

export const kvService = {
  getCommittedValue,
  getOverview,
  scanCommittedPrefix,
};
