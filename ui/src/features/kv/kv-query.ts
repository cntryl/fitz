import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
import { kvService } from "./kv-service";
import type { KvOverview } from "./kv-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const kvQueries = queryScope("kv");
const kvOverviewQuery = defineQuery<{ family: string }, KvOverview>({
  key: ({ family }) => kvQueries.key("overview", family),
  fetch: ({ signal }) => kvService.getOverview({ signal }),
});

export function createKvOverviewQuery() {
  return createQuery(kvOverviewQuery, { family: currentRouteFamilySegment() });
}
