import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import {
  Badge,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Inline,
  Label,
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectPortal,
  SelectTrigger,
  SelectValue,
  Stack,
} from "@askrjs/themes/components";
import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
} from "@/components/shared/query-state";
import type { ResourceInventory } from "@/features/resource/resource-models";
import {
  communicationModeAdapters,
  type CommunicationDomain,
  type CommunicationMode,
  type CommunicationSearchResult,
  type NoticeCommunicationStats,
  type RpcCommunicationStats,
} from "@/features/communication/communication-mode-adapters";
import { domainResourceHref, formatFitzRoute } from "@/shared/navigation/domains";
import { parseConcreteRouteFamilyId, useOperatorContext } from "@/shared/operator-context";

interface CommunicationResourceRow {
  area: string;
  realm: string;
  resource: string;
}

export interface CommunicationFlowWorkspaceProps {
  domain: CommunicationDomain;
  error?: unknown;
  inventory?: ResourceInventory | null;
  loading?: boolean;
  stats: NoticeCommunicationStats | RpcCommunicationStats;
}

function flattenInventory(inventory?: ResourceInventory | null): CommunicationResourceRow[] {
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

function uniqueSorted(values: string[]) {
  return Array.from(new Set(values)).sort((first, second) => first.localeCompare(second));
}

function filterRows(
  rows: CommunicationResourceRow[],
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

export default function CommunicationFlowWorkspace({
  domain,
  error,
  inventory,
  loading = false,
  stats,
}: CommunicationFlowWorkspaceProps) {
  const adapter = communicationModeAdapters[domain];
  const operatorContext = useOperatorContext();
  const [mode, setMode] = state<CommunicationMode>("flow");
  const [realm, setRealm] = state("");
  const [area, setArea] = state("");
  const [resource, setResource] = state("");
  const [searchLoading, setSearchLoading] = state(false);
  const [searchError, setSearchError] = state<unknown>(null);
  const [searchResult, setSearchResult] = state<CommunicationSearchResult | null>(null);
  const modeValue = mode();
  const realmValue = realm();
  const areaValue = area();
  const resourceValue = resource();
  const searchLoadingValue = searchLoading();
  const searchErrorValue = searchError();
  const searchResultValue = searchResult();
  const rows = flattenInventory(inventory);
  const selectedRealmRows = realmValue ? rows.filter((row) => row.realm === realmValue) : rows;
  const selectedAreaRows = areaValue
    ? selectedRealmRows.filter((row) => row.area === areaValue)
    : selectedRealmRows;
  const realmOptions = uniqueSorted(rows.map((row) => row.realm));
  const areaOptions = uniqueSorted(selectedRealmRows.map((row) => row.area));
  const resourceOptions = uniqueSorted(selectedAreaRows.map((row) => row.resource));
  const filteredRows = filterRows(rows, {
    area: areaValue,
    realm: realmValue,
    resource: resourceValue,
  });
  const flowStages = adapter.flowStages(stats);
  const routeFamily = parseConcreteRouteFamilyId(operatorContext.selectedRouteFamilyId);
  const routeFamilyReady = routeFamily !== null;
  const searchMode = modeValue === "search";
  const trimmedRealm = trimToUndefined(realmValue);
  const trimmedArea = trimToUndefined(areaValue);
  const trimmedResource = trimToUndefined(resourceValue);
  const canRunSearch = searchMode && routeFamilyReady && !searchLoadingValue;
  const canOpenExactResource = filteredRows.some(
    (row) => row.realm === realmValue && row.area === areaValue && row.resource === resourceValue,
  );
  const badgeLabel = searchMode
    ? routeFamilyReady
      ? adapter.searchReadyLabel
      : adapter.routeFamilyRequiredLabel
    : adapter.liveDataLabel;
  const badgeVariant = searchMode ? (routeFamilyReady ? "success" : "warning") : "success";
  const columns: readonly VirtualTableColumn<CommunicationResourceRow>[] = [
    {
      id: "route",
      header: "Route",
      width: "76%",
      cellComponent: ({ row }) => {
        const route = formatFitzRoute(domain, row);

        return (
          <span class="domain-table-cell-truncate" title={route}>
            {route}
          </span>
        );
      },
    },
    {
      id: "action",
      header: "Inspect",
      width: "24%",
      cellComponent: ({ row }) => (
        <Link class="text-link" href={domainResourceHref(domain, row)}>
          {adapter.actionLabel}
        </Link>
      ),
    },
  ];

  function resetSearchResults() {
    setSearchError(null);
    setSearchResult(null);
  }

  function selectRealm(nextRealm: string) {
    setRealm(nextRealm);
    setArea("");
    setResource("");
    resetSearchResults();
  }

  function selectArea(nextArea: string) {
    setArea(nextArea);
    setResource("");
    resetSearchResults();
  }

  function selectResource(nextResource: string) {
    setResource(nextResource);
    resetSearchResults();
  }

  async function runSearch() {
    if (!canRunSearch || routeFamily === null) {
      return;
    }

    setSearchLoading(true);
    setSearchError(null);
    setSearchResult(null);

    try {
      setSearchResult(
        await adapter.search({
          area: trimmedArea,
          limit: 50,
          realm: trimmedRealm,
          resource: trimmedResource,
          routeFamily,
        }),
      );
    } catch (caughtError) {
      setSearchError(caughtError);
    } finally {
      setSearchLoading(false);
    }
  }

  function onSubmit(event: Event) {
    event.preventDefault();
    void runSearch();
  }

  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <Inline justify="between" align="start" gap="3" wrap="wrap">
          <Stack gap="1">
            <CardTitle>Communication flow</CardTitle>
            <CardDescription>
              Follow live communication from ingress through route, participant, failure, and
              performance signals without treating ephemeral state as durable history.
            </CardDescription>
          </Stack>
          <Badge variant={badgeVariant}>{badgeLabel}</Badge>
        </Inline>
      </CardHeader>

      <CardContent>
        <Stack gap="3">
          <div class="communication-flow-grid" aria-label={`${domain} flow graph`}>
            <For each={flowStages} by={(stage) => stage.label}>
              {(stage) => (
                <div class="communication-flow-card" data-tone={stage.tone ?? "default"}>
                  <span class="domain-header-kicker">{stage.label}</span>
                  <strong class="communication-flow-value">{stage.value}</strong>
                  <span class="domain-muted">{stage.caption}</span>
                </div>
              )}
            </For>
          </div>

          <div class="domain-query-mode-grid" role="group" aria-label={`${domain} flow mode`}>
            <For each={adapter.modeOptions} by={(modeOption) => modeOption.value}>
              {(modeOption) => (
                <Button
                  type="button"
                  variant={modeValue === modeOption.value ? "primary" : "outline"}
                  onPress={() => {
                    setMode(modeOption.value);
                    resetSearchResults();
                  }}
                  aria-pressed={modeValue === modeOption.value}
                  title={modeOption.description}
                >
                  <span>{modeOption.label}</span>
                </Button>
              )}
            </For>
          </div>

          <form class="communication-flow-form" onSubmit={onSubmit}>
            <div class="form-grid">
              <div class="auth-field">
                <Label for={`${domain}-flow-realm`}>Realm</Label>
                <Select
                  value={realmValue}
                  onValueChange={selectRealm}
                  disabled={realmOptions.length === 0}
                >
                  <SelectTrigger id={`${domain}-flow-realm`}>
                    <SelectValue placeholder="All realms" />
                  </SelectTrigger>
                  <SelectPortal>
                    <SelectContent align="start" sideOffset={6}>
                      <SelectGroup>
                        <SelectLabel>Realm scope</SelectLabel>
                        <SelectItem value="">All realms</SelectItem>
                        <For each={realmOptions} by={(option) => option}>
                          {(option) => <SelectItem value={option}>{option}</SelectItem>}
                        </For>
                      </SelectGroup>
                    </SelectContent>
                  </SelectPortal>
                </Select>
              </div>
              <div class="auth-field">
                <Label for={`${domain}-flow-area`}>Area</Label>
                <Select
                  value={areaValue}
                  onValueChange={selectArea}
                  disabled={areaOptions.length === 0}
                >
                  <SelectTrigger id={`${domain}-flow-area`}>
                    <SelectValue placeholder="All areas" />
                  </SelectTrigger>
                  <SelectPortal>
                    <SelectContent align="start" sideOffset={6}>
                      <SelectGroup>
                        <SelectLabel>Area scope</SelectLabel>
                        <SelectItem value="">All areas</SelectItem>
                        <For each={areaOptions} by={(option) => option}>
                          {(option) => <SelectItem value={option}>{option}</SelectItem>}
                        </For>
                      </SelectGroup>
                    </SelectContent>
                  </SelectPortal>
                </Select>
              </div>
              <div class="auth-field">
                <Label for={`${domain}-flow-resource`}>{adapter.resourceLabel}</Label>
                <Select
                  value={resourceValue}
                  onValueChange={selectResource}
                  disabled={resourceOptions.length === 0}
                >
                  <SelectTrigger id={`${domain}-flow-resource`}>
                    <SelectValue placeholder={adapter.allResourcesLabel} />
                  </SelectTrigger>
                  <SelectPortal>
                    <SelectContent align="start" sideOffset={6}>
                      <SelectGroup>
                        <SelectLabel>{adapter.resourceScopeLabel}</SelectLabel>
                        <SelectItem value="">{adapter.allResourcesLabel}</SelectItem>
                        <For each={resourceOptions} by={(option) => option}>
                          {(option) => <SelectItem value={option}>{option}</SelectItem>}
                        </For>
                      </SelectGroup>
                    </SelectContent>
                  </SelectPortal>
                </Select>
              </div>
            </div>
            {searchMode ? (
              <Inline
                class="communication-query-actions"
                justify="between"
                align="center"
                gap="3"
                wrap="wrap"
              >
                <p class="domain-muted">
                  Querying {operatorContext.selectedRouteFamily.label} with the selected route
                  scope. Leave selectors on All to broaden the evidence read.
                </p>
                <Button type="submit" disabled={!canRunSearch}>
                  {searchLoadingValue ? "Running" : "Run search"}
                </Button>
              </Inline>
            ) : null}
          </form>

          {modeValue !== "flow" ? (
            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>{adapter.modeDetailTitle(modeValue)}</CardTitle>
                <CardDescription>{adapter.modeDetailDescription(modeValue)}</CardDescription>
              </CardHeader>
            </Card>
          ) : null}

          {searchMode && !routeFamilyReady ? (
            <QueryEmptyState
              title="Concrete Route Family required"
              description="Choose a numeric Route Family from the global selector before searching communication evidence."
            />
          ) : null}

          {searchMode && searchLoadingValue ? (
            <QueryLoadingState description={`Searching ${domain.toUpperCase()} evidence...`} />
          ) : null}
          {searchMode && searchErrorValue ? (
            <QueryErrorState
              title={adapter.searchErrorTitle}
              error={searchErrorValue}
              onRetry={() => void runSearch()}
            />
          ) : null}
          {searchMode && searchResultValue && !searchLoadingValue
            ? adapter.renderSearchResult(searchResultValue)
            : null}

          {loading ? (
            <QueryLoadingState description={`Loading ${domain.toUpperCase()} flow resources...`} />
          ) : null}
          {error ? <QueryErrorState title={adapter.loadErrorTitle} error={error} /> : null}

          {!loading && !error ? (
            filteredRows.length === 0 ? (
              <QueryEmptyState
                title={adapter.emptyResourceTitle}
                description="Clear filters, check the selected Route Family, or broaden scope to find visible communication resources."
              />
            ) : (
              <Stack gap="3">
                <Inline justify="between" align="center" gap="3" wrap="wrap">
                  <p class="domain-muted">
                    {filteredRows.length} matching {adapter.resourceNoun(filteredRows.length)}
                  </p>
                  {canOpenExactResource ? (
                    <Link
                      class="text-link"
                      href={domainResourceHref(domain, {
                        area: areaValue,
                        realm: realmValue,
                        resource: resourceValue,
                      })}
                    >
                      {adapter.exactActionLabel}
                    </Link>
                  ) : null}
                </Inline>

                <VirtualTable<CommunicationResourceRow>
                  aria-label={`${domain} communication resources`}
                  class="communication-resource-virtual-table"
                  columns={columns}
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
