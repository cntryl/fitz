import { createQuery } from "@askrjs/askr/data";
import { streamService } from "./stream-service";
import type { StreamOverview } from "./stream-models";

const STREAM_OVERVIEW_KEY = "stream:overview";

export function createStreamOverviewQuery() {
  return createQuery<StreamOverview>({
    key: STREAM_OVERVIEW_KEY,
    fetch: ({ signal }) => streamService.getOverview({ signal }),
  });
}
