import { Show } from "@askrjs/askr/control";
import { currentRoute, Link } from "@askrjs/askr/router";
import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import { Stack } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainWorkflowPanel from "@/components/shared/domain-workflow-panel";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createKvOverviewQuery } from "@/features/kv/kv-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import type {
  ResourceInventory,
  ResourceInventoryArea,
  ResourceInventoryRealm,
  ResourceInventoryResource,
} from "@/features/resource/resource-models";
import { formatNumber } from "@/shared/format";
import { domainHref, domainResourceHref, domainScopeHref } from "@/shared/navigation/domains";

interface KvResourceRow {
  area: string;
  estimateComplete?: boolean;
  estimatedRecordCount?: number;
  estimatedStorageBytes?: number;
  realm: string;
  readLatencyAvgMs?: number;
  readLatencyP95Ms?: number;
  resource: string;
  transactionsActive?: number;
  writeLatencyAvgMs?: number;
  writeLatencyP95Ms?: number;
}

function decodeParam(value: string | undefined) {
  if (!value) return null;

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function resourcesInRealm(realm: ResourceInventoryRealm) {
  return realm.areas.flatMap((area) =>
    (area.resourceEntries ?? area.resources.map((resource) => ({ resource }))).map((resource) => ({
      area: area.area,
      realm: realm.realm,
      ...resource,
    })),
  );
}

function rowFromResourceEntry(
  realm: string,
  area: string,
  resource: ResourceInventoryResource,
): KvResourceRow {
  return {
    area,
    realm,
    ...resource,
  };
}

function resourceRows(
  inventory: ResourceInventory | null | undefined,
  realm: string,
  area?: string,
) {
  const inventoryRealm = inventory?.realms.find((entry) => entry.realm === realm);
  if (!inventoryRealm) return [];

  if (!area) {
    return resourcesInRealm(inventoryRealm);
  }

  const inventoryArea = inventoryRealm.areas.find((entry) => entry.area === area);
  const resourceEntries =
    inventoryArea?.resourceEntries ??
    inventoryArea?.resources.map((resource) => ({ resource })) ??
    [];

  return resourceEntries.map((resource) => rowFromResourceEntry(realm, area, resource));
}

function formatMaybeNumber(value: number | undefined) {
  return value === undefined ? "--" : formatNumber(value);
}

function formatStorageBytes(value: number | undefined) {
  if (value === undefined) return "--";
  if (value < 1024) return `${formatNumber(value)} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MiB`;
}

function formatLatency(avg: number | undefined, p95: number | undefined) {
  if (avg === undefined || p95 === undefined) return "--";
  return `${avg.toFixed(1)} / ${p95.toFixed(1)} ms`;
}

function KvRealmRows(props: { realms: ResourceInventoryRealm[] }) {
  const columns: readonly VirtualTableColumn<ResourceInventoryRealm>[] = [
    {
      id: "realm",
      header: "Realm",
      width: "48%",
      cellComponent: ({ row }) => (
        <Link class="domain-link-cell" href={domainScopeHref("kv", { realm: row.realm })}>
          {row.realm}
        </Link>
      ),
    },
    {
      id: "areas",
      header: "Areas",
      width: "24%",
      cellComponent: ({ row }) => <span>{formatNumber(row.areas.length)}</span>,
    },
    {
      id: "resources",
      header: "Resources",
      width: "28%",
      cellComponent: ({ row }) => <span>{formatNumber(resourcesInRealm(row).length)}</span>,
    },
  ];

  if (props.realms.length === 0) {
    return <QueryEmptyState description="No visible KV resources at the current level." />;
  }

  return (
    <VirtualTable<ResourceInventoryRealm>
      aria-label="KV realms"
      class="domain-resource-virtual-table"
      columns={columns}
      getKey={(row) => row.realm}
      headerHeight={44}
      overscan={4}
      rowHeight={48}
      rows={props.realms}
      style={{ height: "320px" }}
    />
  );
}

function KvAreaRows(props: { areas: ResourceInventoryArea[]; realm: string }) {
  const columns: readonly VirtualTableColumn<ResourceInventoryArea>[] = [
    {
      id: "area",
      header: "Area",
      width: "60%",
      cellComponent: ({ row }) => (
        <Link
          class="domain-link-cell"
          href={domainScopeHref("kv", { area: row.area, realm: props.realm })}
        >
          {row.area}
        </Link>
      ),
    },
    {
      id: "resources",
      header: "Resources",
      width: "40%",
      cellComponent: ({ row }) => <span>{formatNumber(row.resources.length)}</span>,
    },
  ];

  if (props.areas.length === 0) {
    return <QueryEmptyState description="No visible KV resources at the current level." />;
  }

  return (
    <VirtualTable<ResourceInventoryArea>
      aria-label="KV areas"
      class="domain-resource-virtual-table"
      columns={columns}
      getKey={(row) => `${props.realm}:${row.area}`}
      headerHeight={44}
      overscan={4}
      rowHeight={48}
      rows={props.areas}
      style={{ height: "280px" }}
    />
  );
}

function KvResourceRows(props: { resources: KvResourceRow[] }) {
  const columns: readonly VirtualTableColumn<KvResourceRow>[] = [
    {
      id: "resource",
      header: "Resource",
      width: "28%",
      cellComponent: ({ row }) => (
        <Link class="domain-link-cell" href={domainResourceHref("kv", row)}>
          {row.resource}
        </Link>
      ),
    },
    {
      id: "area",
      header: "Area",
      width: "16%",
      cellComponent: ({ row }) => <span class="domain-table-cell-truncate">{row.area}</span>,
    },
    {
      id: "records",
      header: "Estimated records",
      width: "14%",
      cellComponent: ({ row }) => (
        <span title={row.estimateComplete === false ? "Estimate incomplete" : undefined}>
          {formatMaybeNumber(row.estimatedRecordCount)}
          {row.estimateComplete === false ? " +" : ""}
        </span>
      ),
    },
    {
      id: "storage",
      header: "Logical storage",
      width: "14%",
      cellComponent: ({ row }) => <span>{formatStorageBytes(row.estimatedStorageBytes)}</span>,
    },
    {
      id: "read-latency",
      header: "Read latency",
      width: "14%",
      cellComponent: ({ row }) => (
        <span>{formatLatency(row.readLatencyAvgMs, row.readLatencyP95Ms)}</span>
      ),
    },
    {
      id: "write-latency",
      header: "Write latency",
      width: "14%",
      cellComponent: ({ row }) => (
        <span>{formatLatency(row.writeLatencyAvgMs, row.writeLatencyP95Ms)}</span>
      ),
    },
  ];

  if (props.resources.length === 0) {
    return <QueryEmptyState description="No visible KV resources at the current level." />;
  }

  return (
    <VirtualTable<KvResourceRow>
      aria-label="KV resources"
      class="domain-resource-virtual-table"
      columns={columns}
      getKey={(row) => `${row.realm}:${row.area}:${row.resource}`}
      headerHeight={44}
      overscan={6}
      rowHeight={48}
      rows={props.resources}
      style={{ height: "360px" }}
    />
  );
}

function summarizeKvHealth(stats: {
  commitsFailedTotal: number;
  invalidTransactionRejectsTotal: number;
  keysTotal: number;
  transactionsActive: number;
}) {
  const pressureSignals = [
    stats.commitsFailedTotal > 0 ? `${stats.commitsFailedTotal} commit failure(s)` : null,
    stats.invalidTransactionRejectsTotal > 0
      ? `${stats.invalidTransactionRejectsTotal} invalid reject(s)`
      : null,
  ].filter((signal): signal is string => signal !== null);

  if (pressureSignals.length > 0) {
    return {
      detail: `${stats.keysTotal} keys are currently authoritative. ${stats.transactionsActive} active transaction session(s) are broker-local. ${pressureSignals.join(", ")} indicate transactional pressure.`,
      label: "Attention" as const,
      tone: "danger" as const,
    };
  }

  return {
    detail: `${stats.keysTotal} keys are currently authoritative with ${stats.transactionsActive} active transaction session(s).`,
    label: "Live" as const,
    tone: "success" as const,
  };
}

export default function KvPage() {
  const route = currentRoute();
  const realm = decodeParam(route.params.realm);
  const area = decodeParam(route.params.area);
  const overview = createKvOverviewQuery();
  const inventory = createResourceInventoryQuery("kv");
  const data = overview.data;
  const health = summarizeKvHealth(
    data?.stats ?? {
      commitsFailedTotal: 0,
      invalidTransactionRejectsTotal: 0,
      keysTotal: 0,
      transactionsActive: 0,
    },
  );
  const selectedRealm = realm
    ? inventory.data?.realms.find((entry) => entry.realm === realm)
    : undefined;
  const selectedArea = area ? selectedRealm?.areas.find((entry) => entry.area === area) : undefined;
  const rows = realm ? resourceRows(inventory.data, realm, area ?? undefined) : [];

  const snapshot = createDomainSidebar({
    data: inventory.data,
    title: "KV scope",
    description: realm ? [realm, area].filter(Boolean).join(" / ") : "Visible KV realms",
    stats: (current) => [
      { label: "Realms", value: current.realms.length },
      {
        label: "Areas",
        value: current.realms.reduce((sum, entry) => sum + entry.areas.length, 0),
      },
      {
        label: "Resources",
        value: current.realms.reduce((sum, entry) => sum + resourcesInRealm(entry).length, 0),
      },
    ],
  });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Authoritative state"
          title={area ? `KV area ${area}` : realm ? `KV realm ${realm}` : "KV overview"}
          description={
            area
              ? "KV resources in this area."
              : realm
                ? "KV areas and resources in this realm."
                : "KV realms for the active route family."
          }
          primaryAction={{
            label: "Refresh KV",
            onPress: () => {
              void overview.refresh();
              void inventory.refresh();
            },
          }}
          status={{
            detail: health.detail,
            label: overview.refreshing || inventory.refreshing ? "Refreshing" : health.label,
            tone: overview.refreshing || inventory.refreshing ? "info" : health.tone,
          }}
        />

        {snapshot}

        <Show when={!data && overview.loading}>
          <QueryLoadingState description="Loading KV overview..." />
        </Show>

        <Show when={!inventory.data && inventory.loading}>
          <QueryLoadingState description="Loading KV inventory..." />
        </Show>

        <Show when={!data && overview.error}>
          <QueryErrorState
            title="Unable to load KV overview"
            error={overview.error}
            onRetry={() => overview.refresh()}
          />
        </Show>

        <Show when={!inventory.data && inventory.error}>
          <QueryErrorState
            title="Unable to load KV inventory"
            error={inventory.error}
            onRetry={() => inventory.refresh()}
          />
        </Show>

        <Show when={data && inventory.data ? data : null}>
          {(currentData) => (
            <Stack gap="3">
              <Show when={overview.refreshing || inventory.refreshing}>
                <QueryRefreshingState description="Refreshing KV data..." />
              </Show>

              <DomainMetricTable
                title="KV metrics"
                description="Current key count, transaction pressure, and throughput."
                metrics={[
                  { label: "Keys total", value: currentData.stats.keysTotal },
                  { label: "Active transactions", value: currentData.stats.transactionsActive },
                  { label: "Ops / sec", value: currentData.stats.operationsPerSecond.toFixed(2) },
                  { label: "Commit failures", value: currentData.stats.commitsFailedTotal },
                  {
                    label: "Invalid transaction rejects",
                    value: currentData.stats.invalidTransactionRejectsTotal,
                  },
                ]}
              />

              <Show when={!realm}>
                <KvRealmRows realms={inventory.data!.realms} />
              </Show>

              <Show when={realm && !area}>
                <Stack gap="3">
                  <KvAreaRows areas={selectedRealm?.areas ?? []} realm={realm!} />
                  <KvResourceRows resources={rows} />
                </Stack>
              </Show>

              <Show when={realm && area}>
                <KvResourceRows resources={selectedArea ? rows : []} />
              </Show>

              <Show when={!realm}>
                <DomainWorkflowPanel
                  archetype="KV Resources"
                  workflows={["Drill down", "Browse rows", "Inspect values"]}
                  questions={["Which resources exist?", "What committed rows are visible?"]}
                  diagnostics={["Transaction pressure", "Committed data", "Resource inventory"]}
                />
              </Show>
            </Stack>
          )}
        </Show>

        <Show when={realm && !selectedRealm && inventory.data}>
          <QueryEmptyState description="No visible KV resources at the current level." />
        </Show>

        <Show when={realm && area && !selectedArea && inventory.data}>
          <QueryEmptyState description="No visible KV resources at the current level." />
        </Show>

        <Show when={realm}>
          <Link
            class="text-link"
            href={area ? domainScopeHref("kv", { realm: realm! }) : domainHref("kv")}
          >
            Back
          </Link>
        </Show>
      </Stack>
    </DomainPageFrame>
  );
}
