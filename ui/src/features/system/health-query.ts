import { createQuery } from "@askrjs/askr/data";
import { apiv1 } from "@/adapters";
import { unwrapResponse } from "@/shared/errors/api";

export interface HealthSummary {
  liveness: string;
  readiness: string;
  startup: string;
}

export function createHealthSummaryQuery() {
  return createQuery<HealthSummary>({
    key: "system:health-summary",
    fetch: async ({ signal }) => {
      const [liveness, readiness, startup] = await Promise.all([
        apiv1.getLivenessProbe({ signal }),
        apiv1.getReadinessProbe({ signal }),
        apiv1.getStartupProbe({ signal }),
      ]);

      return {
        liveness: unwrapResponse(liveness, "Unable to load liveness").status,
        readiness: unwrapResponse(readiness, "Unable to load readiness").status,
        startup: unwrapResponse(startup, "Unable to load startup").status,
      };
    },
  });
}
