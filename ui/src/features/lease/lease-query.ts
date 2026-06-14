import { createQuery, queryScope } from "@askrjs/askr/data";
import { leaseService } from "./lease-service";
import type { LeaseOverview } from "./lease-models";

const leaseQueries = queryScope("lease");

const LEASE_OVERVIEW_KEY = leaseQueries.key("overview");

export function createLeaseOverviewQuery() {
  return createQuery<LeaseOverview>({
    key: LEASE_OVERVIEW_KEY,
    fetch: ({ signal }) => leaseService.getOverview({ signal }),
  });
}
