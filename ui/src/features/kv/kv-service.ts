import { apiParams, apiParamsQuery, apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "@/shared/navigation/domains";
import { mapKvCommittedValue, mapKvOverview, mapKvPrefixScan, mapKvRows } from "./kv-mappers";
import type {
  KvCommittedResourceScope,
  KvCommittedValueResult,
  KvKeyEncoding,
  KvOverview,
  KvPrefixScanResult,
  KvResourceScope,
  KvRowsResult,
} from "./kv-models";

async function getOverview(options: ServiceRequestOptions = {}): Promise<KvOverview> {
  const family = apiRouteFamilySegment();
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listKvRealms(apiParams({ family }, options)),
    apiv1.getKvStats(apiParams({ family }, options)),
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
        apiParamsQuery(
          {
            area: scope.area,
            family: apiRouteFamilySegment(scope.routeFamily),
            realm: scope.realm,
            resource: scope.resource,
          },
          {
            key,
            key_encoding: keyEncoding,
          },
          options,
        ),
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
        apiParamsQuery(
          {
            area: scope.area,
            family: apiRouteFamilySegment(scope.routeFamily),
            realm: scope.realm,
            resource: scope.resource,
          },
          {
            key_encoding: keyEncoding,
            limit,
            prefix,
          },
          options,
        ),
      ),
      "Unable to scan committed KV prefix",
    ),
  );
}

async function browseCommittedRows(
  scope: KvResourceScope,
  request: {
    cursor?: string | null;
    limit?: number;
    startsWith?: string;
  },
  options: ServiceRequestOptions = {},
): Promise<KvRowsResult> {
  return mapKvRows(
    unwrapResponse(
      await apiv1.browseKvCommittedRows(
        apiParamsQuery(
          {
            area: scope.area,
            family: apiRouteFamilySegment(),
            realm: scope.realm,
            resource: scope.resource,
          },
          {
            cursor: request.cursor ?? undefined,
            key_encoding: "utf8",
            limit: request.limit,
            starts_with: request.startsWith,
          },
          options,
        ),
      ),
      "Unable to browse committed KV rows",
    ),
  );
}

export const kvService = {
  browseCommittedRows,
  getCommittedValue,
  getOverview,
  scanCommittedPrefix,
};
