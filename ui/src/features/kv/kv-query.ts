import { createQuery, queryScope } from "@askrjs/askr/data";
import { kvService } from "./kv-service";
import type { KvOverview } from "./kv-models";

const kvQueries = queryScope("kv");

const KV_OVERVIEW_KEY = kvQueries.key("overview");

export function createKvOverviewQuery() {
  return createQuery<KvOverview>({
    key: KV_OVERVIEW_KEY,
    fetch: ({ signal }) => kvService.getOverview({ signal }),
  });
}
