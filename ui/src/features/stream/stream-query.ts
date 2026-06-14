import { createQuery, queryScope } from "@askrjs/askr/data";
import { streamService } from "./stream-service";
import type { StreamOverview } from "./stream-models";

const streamQueries = queryScope("stream");

const STREAM_OVERVIEW_KEY = streamQueries.key("overview");

export function createStreamOverviewQuery() {
  return createQuery<StreamOverview>({
    key: STREAM_OVERVIEW_KEY,
    fetch: ({ signal }) => streamService.getOverview({ signal }),
  });
}
