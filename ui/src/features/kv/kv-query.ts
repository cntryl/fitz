import { createQuery } from "@askrjs/askr/data";
import { kvService } from "./kv-service";
import type { KvOverview } from "./kv-models";

const KV_OVERVIEW_KEY = "kv:overview";

export function createKvOverviewQuery() {
  return createQuery<KvOverview>({
    key: KV_OVERVIEW_KEY,
    fetch: ({ signal }) => kvService.getOverview({ signal }),
  });
}
