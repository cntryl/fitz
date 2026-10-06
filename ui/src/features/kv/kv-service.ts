import { apiParams, apiParamsQuery, apiv1 } from "@/adapters";
import { unwrapResponse, type ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "@/shared/navigation/domains";
import {
  routeFamilyRequest,
  type RouteFamilyRequestOptions,
} from "@/shared/navigation/route-family-request";
import {
  mapKvCommittedValue,
  mapKvOverview,
  mapKvPrefixScan,
  mapKvResourceDetail,
  mapKvRows,
  mapKvTransactions,
} from "./kv-mappers";
import type {
  KvCommittedResourceScope,
  KvCommittedValueResult,
  KvKeyEncoding,
  KvOverview,
  KvPrefixScanResult,
  KvResourceScope,
  KvRowsResult,
  KvResourceDetail,
  KvTransaction,
} from "./kv-models";

async function getOverview(options: RouteFamilyRequestOptions = {}): Promise<KvOverview> {
  const { family, requestOptions } = routeFamilyRequest(options);
  const [realmsResponse, statsResponse] = await Promise.all([
    apiv1.listKvRealms(apiParams({ family }, requestOptions)),
    apiv1.getKvStats(apiParams({ family }, requestOptions)),
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
  options: RouteFamilyRequestOptions = {},
): Promise<KvRowsResult> {
  const { family, requestOptions } = routeFamilyRequest(options);
  return mapKvRows(
    unwrapResponse(
      await apiv1.browseKvCommittedRows(
        apiParamsQuery(
          {
            area: scope.area,
            family,
            realm: scope.realm,
            resource: scope.resource,
          },
          {
            cursor: request.cursor ?? undefined,
            key_encoding: "utf8",
            limit: request.limit,
            starts_with: request.startsWith,
          },
          requestOptions,
        ),
      ),
      "Unable to browse committed KV rows",
    ),
  );
}

async function getResource(
  scope: KvResourceScope,
  options: RouteFamilyRequestOptions = {},
): Promise<KvResourceDetail> {
  const { family, requestOptions } = routeFamilyRequest(options);
  return mapKvResourceDetail(
    unwrapResponse(
      await apiv1.getKvResource(
        apiParams(
          { area: scope.area, family, realm: scope.realm, resource: scope.resource },
          requestOptions,
        ),
      ),
      "Unable to load KV resource detail",
    ),
  );
}

async function listTransactions(
  scope: KvResourceScope,
  options: RouteFamilyRequestOptions = {},
): Promise<KvTransaction[]> {
  const { family, requestOptions } = routeFamilyRequest(options);
  return mapKvTransactions(
    unwrapResponse(
      await apiv1.listKvTransactions(
        apiParams(
          { area: scope.area, family, realm: scope.realm, resource: scope.resource },
          requestOptions,
        ),
      ),
      "Unable to load KV transactions",
    ),
  );
}

export const kvService = {
  browseCommittedRows,
  getCommittedValue,
  getOverview,
  getResource,
  listTransactions,
  scanCommittedPrefix,
};
