import { createQuery, queryScope } from "@askrjs/askr/data";
import { kvService } from "./kv-service";
import type { KvOverview } from "./kv-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const kvQueries = queryScope("kv");

export function createKvOverviewQuery() {
  const key = kvQueries.key("overview", currentRouteFamilySegment());

  return createQuery<KvOverview>({
    key,
    fetch: kvService.getOverview,
  });
}
