import { createQuery } from "@askrjs/askr/data";
import { leaseService } from "./lease-service";
import type { LeaseOverview } from "./lease-models";

const LEASE_OVERVIEW_KEY = "lease:overview";

export function createLeaseOverviewQuery() {
  return createQuery<LeaseOverview>({
    key: LEASE_OVERVIEW_KEY,
    fetch: ({ signal }) => leaseService.getOverview({ signal }),
  });
}
