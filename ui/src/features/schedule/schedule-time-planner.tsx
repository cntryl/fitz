import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Button } from "@askrjs/themes/components";
import { Inline, Stack } from "@askrjs/themes/components";
import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/components";
import { Input, Label, VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import type {
  ScheduleExecutionObservation,
  ScheduleExecutionObservationList,
  ScheduleMissedObservation,
  ScheduleMissedObservationList,
} from "@/adapters";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
} from "@/components/shared/query-state";
import type { ResourceInventory } from "@/features/resource/resource-models";
import { domainResourceHref, formatFitzRoute } from "@/shared/navigation/domains";
import { parseConcreteRouteFamilyId, useOperatorContext } from "@/shared/operator-context";
import { scheduleService } from "./schedule-service";

type SchedulePlannerMode = "resource" | "configuration" | "execution" | "missed";

interface ScheduleResourceRow {
  area: string;
  realm: string;
  resource: string;
}

export interface ScheduleTimePlannerProps {
  error?: unknown;
  inventory?: ResourceInventory | null;
  loading?: boolean;
}

const plannerModes: Array<{
  description: string;
  label: string;
  value: SchedulePlannerMode;
}> = [
  {
    description: "Use existing schedule resource detail and bounded event timeline APIs.",
    label: "Timeline",
    value: "resource",
  },
  {
    description: "Use existing resource detail to review durable timing intent.",
    label: "Config",
    value: "configuration",
  },
  {
    description: "List schedule-owned handoff observations for one resource.",
    label: "Handoffs",
    value: "execution",
  },
  {
    description: "Search pending schedule handoff claims in one Route Family.",
    label: "Missed",
    value: "missed",
  },
];

const scheduleColumns: readonly VirtualTableColumn<ScheduleResourceRow>[] = [
  {
    id: "route",
    header: "Route",
    width: "78%",
    cellComponent: ({ row }) => {
      const route = formatFitzRoute("schedule", row);

      return (
        <span class="domain-table-cell-truncate" title={route}>
          {route}
        </span>
      );
    },
  },
  {
    id: "action",
    header: "Planner",
    width: "22%",
    cellComponent: ({ row }) => (
      <Link class="text-link" href={domainResourceHref("schedule", row)}>
        Open plan
      </Link>
    ),
  },
];

const executionColumns: readonly VirtualTableColumn<ScheduleExecutionObservation>[] = [
  {
    id: "route",
    header: "Route",
    width: "22%",
    cellComponent: ({ row }) => {
      const route = formatFitzRoute("schedule", row);

      return (
        <span class="domain-table-cell-truncate" title={route}>
          {route}
        </span>
      );
    },
  },
  {
    id: "status",
    header: "Status",
    width: "20%",
    cellComponent: ({ row }) => <Badge variant="outline">{row.status}</Badge>,
  },
  {
    id: "next",
    header: "Next run",
    width: "24%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={row.next_run}>
        {row.next_run}
      </span>
    ),
  },
  {
    id: "last",
    header: "Last handoff",
    width: "20%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={row.last_run ?? "None"}>
        {row.last_run ?? "None"}
      </span>
    ),
  },
  {
    id: "count",
    header: "Count",
    width: "14%",
    cellComponent: ({ row }) => <span>{row.executions_total}</span>,
  },
];

const missedColumns: readonly VirtualTableColumn<ScheduleMissedObservation>[] = [
  {
    id: "route",
    header: "Route",
    width: "48%",
    cellComponent: ({ row }) => {
      const route = formatFitzRoute("schedule", row);

      return (
        <span class="domain-table-cell-truncate" title={route}>
          {route}
        </span>
      );
    },
  },
  {
    id: "fire",
    header: "Fire at",
    width: "24%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={row.fire_at}>
        {row.fire_at}
      </span>
    ),
  },
  {
    id: "age",
    header: "Age",
    width: "12%",
    cellComponent: ({ row }) => <span>{row.age_seconds}s</span>,
  },
  {
    id: "status",
    header: "Status",
    width: "16%",
    cellComponent: ({ row }) => <Badge variant="warning">{row.status}</Badge>,
  },
];

function flattenInventory(inventory?: ResourceInventory | null): ScheduleResourceRow[] {
  return (
    inventory?.realms.flatMap((realm) =>
      realm.areas.flatMap((area) =>
        area.resources.map((resource) => ({
          area: area.area,
          realm: realm.realm,
          resource,
        })),
      ),
    ) ?? []
  );
}

function includesQuery(value: string, query: string) {
  const normalized = query.trim().toLowerCase();

  return normalized.length === 0 || value.toLowerCase().includes(normalized);
}

function filterRows(
  rows: ScheduleResourceRow[],
  filters: {
    area: string;
    realm: string;
    resource: string;
  },
) {
  return rows.filter(
    (row) =>
      includesQuery(row.realm, filters.realm) &&
      includesQuery(row.area, filters.area) &&
      includesQuery(row.resource, filters.resource),
  );
}

function trimToUndefined(value: string) {
  const trimmed = value.trim();

  return trimmed.length > 0 ? trimmed : undefined;
}

function isObservationMode(mode: SchedulePlannerMode) {
  return mode === "execution" || mode === "missed";
}

function modeQueryLabel(mode: SchedulePlannerMode) {
  if (mode === "execution") return "Resource scope";
  if (mode === "missed") return "Claim filter";
  if (mode === "configuration") return "Policy";

  return "Window";
}

function modeQueryPlaceholder(mode: SchedulePlannerMode) {
  if (mode === "execution") return "Use realm, area, resource";
  if (mode === "missed") return "Optional";
  if (mode === "configuration") return "cron";

  return "next 24h";
}

function ScheduleExecutionPanel({ result }: { result: ScheduleExecutionObservationList }) {
  return (
    <div class="schedule-observation-result" aria-live="polite">
      <Inline justify="between" align="center" gap="3" wrap="wrap">
        <p class="domain-muted">
          {result.observations.length} handoff observation
          {result.observations.length === 1 ? "" : "s"} for {result.realm}/{result.area}/
          {result.resource}
        </p>
      </Inline>

      {result.observations.length === 0 ? (
        <QueryEmptyState
          title="No schedule handoffs"
          description="No schedule-owned handoff observations matched this resource."
        />
      ) : (
        <VirtualTable<ScheduleExecutionObservation>
          aria-label="Schedule handoff observations"
          class="schedule-resource-virtual-table"
          columns={executionColumns}
          getKey={(row) =>
            `${row.route_family}:${row.realm}:${row.area}:${row.resource}:${row.operation}`
          }
          headerHeight={44}
          overscan={6}
          rowHeight={48}
          rows={result.observations}
          style={{ height: "280px" }}
        />
      )}
    </div>
  );
}

function ScheduleMissedPanel({ result }: { result: ScheduleMissedObservationList }) {
  return (
    <div class="schedule-observation-result" aria-live="polite">
      <Inline justify="between" align="center" gap="3" wrap="wrap">
        <p class="domain-muted">
          {result.observations.length} pending handoff claim
          {result.observations.length === 1 ? "" : "s"} in route family {result.route_family}
        </p>
      </Inline>

      {result.observations.length === 0 ? (
        <QueryEmptyState
          title="No pending handoff claims"
          description="No pending schedule handoff claims matched the selected Route Family and scope."
        />
      ) : (
        <VirtualTable<ScheduleMissedObservation>
          aria-label="Pending schedule handoff claims"
          class="schedule-resource-virtual-table"
          columns={missedColumns}
          getKey={(row) =>
            `${row.route_family}:${row.realm}:${row.area}:${row.resource}:${row.operation}:${row.fire_ms}`
          }
          headerHeight={44}
          overscan={6}
          rowHeight={48}
          rows={result.observations}
          style={{ height: "280px" }}
        />
      )}
    </div>
  );
}

export default function ScheduleTimePlanner({
  error,
  inventory,
  loading = false,
}: ScheduleTimePlannerProps) {
  const operatorContext = useOperatorContext();
  const [mode, setMode] = state<SchedulePlannerMode>("resource");
  const [realm, setRealm] = state("");
  const [area, setArea] = state("");
  const [resource, setResource] = state("");
  const [plannerQuery, setPlannerQuery] = state("");
  const [observationLoading, setObservationLoading] = state(false);
  const [observationError, setObservationError] = state<unknown>(null);
  const [executionResult, setExecutionResult] = state<ScheduleExecutionObservationList | null>(
    null,
  );
  const [missedResult, setMissedResult] = state<ScheduleMissedObservationList | null>(null);
  const modeValue = mode();
  const realmValue = realm();
  const areaValue = area();
  const resourceValue = resource();
  const plannerQueryValue = plannerQuery();
  const observationLoadingValue = observationLoading();
  const observationErrorValue = observationError();
  const executionResultValue = executionResult();
  const missedResultValue = missedResult();
  const rows = flattenInventory(inventory);
  const filteredRows = filterRows(rows, {
    area: areaValue,
    realm: realmValue,
    resource: resourceValue,
  });
  const routeFamily = parseConcreteRouteFamilyId(operatorContext.selectedRouteFamilyId);
  const routeFamilyReady = routeFamily !== null;
  const observationMode = isObservationMode(modeValue);
  const trimmedRealm = trimToUndefined(realmValue);
  const trimmedArea = trimToUndefined(areaValue);
  const trimmedResource = trimToUndefined(resourceValue);
  const canRunObservationQuery =
    observationMode &&
    routeFamilyReady &&
    !observationLoadingValue &&
    (modeValue !== "execution" || Boolean(trimmedRealm && trimmedArea && trimmedResource));
  const canOpenExactResource = filteredRows.some(
    (row) => row.realm === realmValue && row.area === areaValue && row.resource === resourceValue,
  );
  const badgeLabel = observationMode
    ? routeFamilyReady
      ? "Schedule observation data"
      : "Route Family required"
    : "Live admin data";
  const badgeVariant = observationMode ? (routeFamilyReady ? "success" : "warning") : "outline";

  async function runObservationQuery() {
    if (!canRunObservationQuery || routeFamily === null) {
      return;
    }

    setObservationLoading(true);
    setObservationError(null);
    setExecutionResult(null);
    setMissedResult(null);

    try {
      if (modeValue === "execution" && trimmedRealm && trimmedArea && trimmedResource) {
        setExecutionResult(
          await scheduleService.listExecutionObservations({
            area: trimmedArea,
            limit: 50,
            realm: trimmedRealm,
            resource: trimmedResource,
            routeFamily,
          }),
        );
      } else if (modeValue === "missed") {
        setMissedResult(
          await scheduleService.searchMissedHandoffs({
            area: trimmedArea,
            limit: 50,
            realm: trimmedRealm,
            resource: trimmedResource,
            routeFamily,
          }),
        );
      }
    } catch (caughtError) {
      setObservationError(caughtError);
    } finally {
      setObservationLoading(false);
    }
  }

  function onSubmit(event: Event) {
    event.preventDefault();
    void runObservationQuery();
  }

  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <Inline justify="between" align="start" gap="3" wrap="wrap">
          <Stack gap="1">
            <CardTitle>Time planner</CardTitle>
            <CardDescription>
              Locate schedule resources by realm, area, and resource, then inspect durable timing
              intent, bounded resource observations, and handoff pressure.
            </CardDescription>
          </Stack>
          <Badge variant={badgeVariant}>{badgeLabel}</Badge>
        </Inline>
      </CardHeader>

      <CardContent>
        <Stack gap="3">
          <div class="domain-query-mode-grid" role="group" aria-label="Schedule planner mode">
            <For each={plannerModes} by={(plannerMode) => plannerMode.value}>
              {(plannerMode) => (
                <Button
                  type="button"
                  variant={modeValue === plannerMode.value ? "primary" : "outline"}
                  onPress={() => {
                    setMode(plannerMode.value);
                    setObservationError(null);
                    setExecutionResult(null);
                    setMissedResult(null);
                  }}
                  aria-pressed={modeValue === plannerMode.value}
                  title={plannerMode.description}
                >
                  <span>{plannerMode.label}</span>
                </Button>
              )}
            </For>
          </div>

          <form class="schedule-planner-form" onSubmit={onSubmit}>
            <div class="form-grid">
              <div class="auth-field">
                <Label for="schedule-planner-realm">Realm</Label>
                <Input
                  id="schedule-planner-realm"
                  value={realmValue}
                  onInput={(event: Event) => setRealm((event.target as HTMLInputElement).value)}
                  placeholder="billing"
                />
              </div>
              <div class="auth-field">
                <Label for="schedule-planner-area">Area</Label>
                <Input
                  id="schedule-planner-area"
                  value={areaValue}
                  onInput={(event: Event) => setArea((event.target as HTMLInputElement).value)}
                  placeholder="payments"
                />
              </div>
              <div class="auth-field">
                <Label for="schedule-planner-resource">Resource</Label>
                <Input
                  id="schedule-planner-resource"
                  value={resourceValue}
                  onInput={(event: Event) => setResource((event.target as HTMLInputElement).value)}
                  placeholder="settlement-run"
                />
              </div>
              <div class="auth-field">
                <Label for="schedule-planner-query">{modeQueryLabel(modeValue)}</Label>
                <Input
                  id="schedule-planner-query"
                  value={plannerQueryValue}
                  disabled={!observationMode && modeValue !== "configuration"}
                  onInput={(event: Event) =>
                    setPlannerQuery((event.target as HTMLInputElement).value)
                  }
                  placeholder={modeQueryPlaceholder(modeValue)}
                />
              </div>
            </div>
            {observationMode ? (
              <Inline
                class="schedule-query-actions"
                justify="between"
                align="center"
                gap="3"
                wrap="wrap"
              >
                <p class="domain-muted">
                  Querying {operatorContext.selectedRouteFamily.label}. Schedule observation reads
                  require a concrete numeric Route Family.
                </p>
                <Button type="submit" disabled={!canRunObservationQuery}>
                  {observationLoadingValue ? "Running" : "Run query"}
                </Button>
              </Inline>
            ) : null}
          </form>

          {observationMode && !routeFamilyReady ? (
            <QueryEmptyState
              title="Concrete Route Family required"
              description="Choose a numeric Route Family from the global selector before reading schedule observations."
            />
          ) : null}

          {modeValue === "execution" &&
          routeFamilyReady &&
          !(trimmedRealm && trimmedArea && trimmedResource) ? (
            <QueryEmptyState
              title="Resource scope required"
              description="Enter realm, area, and resource to list schedule handoff observations."
            />
          ) : null}

          {observationMode && observationLoadingValue ? (
            <QueryLoadingState description="Loading schedule observations..." />
          ) : null}
          {observationMode && observationErrorValue ? (
            <QueryErrorState
              title="Unable to load schedule observations"
              error={observationErrorValue}
              onRetry={() => void runObservationQuery()}
            />
          ) : null}
          {modeValue === "execution" && executionResultValue && !observationLoadingValue ? (
            <ScheduleExecutionPanel result={executionResultValue} />
          ) : null}
          {modeValue === "missed" && missedResultValue && !observationLoadingValue ? (
            <ScheduleMissedPanel result={missedResultValue} />
          ) : null}

          {modeValue === "configuration" ? (
            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>Configuration review lives on the resource page</CardTitle>
                <CardDescription>
                  Select a schedule resource to inspect enabled state, cron policy, next run, and
                  the exact diagnostics payload from the existing admin API.
                </CardDescription>
              </CardHeader>
            </Card>
          ) : null}

          {loading ? <QueryLoadingState description="Loading schedule resources..." /> : null}
          {error ? (
            <QueryErrorState title="Unable to load schedule resources" error={error} />
          ) : null}

          {!loading && !error ? (
            filteredRows.length === 0 ? (
              <QueryEmptyState
                title="No matching schedules"
                description="Clear filters, check the selected Route Family, or broaden scope to find visible schedule resources."
              />
            ) : (
              <Stack gap="3">
                <Inline justify="between" align="center" gap="3" wrap="wrap">
                  <p class="domain-muted">
                    {filteredRows.length} matching schedule
                    {filteredRows.length === 1 ? "" : "s"}
                  </p>
                  {canOpenExactResource ? (
                    <Link
                      class="text-link"
                      href={domainResourceHref("schedule", {
                        area: areaValue,
                        realm: realmValue,
                        resource: resourceValue,
                      })}
                    >
                      Open exact schedule
                    </Link>
                  ) : null}
                </Inline>

                <VirtualTable<ScheduleResourceRow>
                  aria-label="Matching schedule resources"
                  class="schedule-resource-virtual-table"
                  columns={scheduleColumns}
                  getKey={(row) => `${row.realm}:${row.area}:${row.resource}`}
                  headerHeight={44}
                  overscan={6}
                  rowHeight={48}
                  rows={filteredRows}
                  style={{ height: "384px" }}
                />
              </Stack>
            )
          ) : null}
        </Stack>
      </CardContent>
    </Card>
  );
}
