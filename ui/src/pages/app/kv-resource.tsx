import { state } from "@askrjs/askr";
import { Show } from "@askrjs/askr/control";
import { currentRoute, Link, navigate } from "@askrjs/askr/router";
import { Form, Input, Label, VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import {
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Inline,
  Stack,
} from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import CopyTextButton from "@/components/shared/copy-text-button";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import { queryFreshness, queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryCompactEmptyState,
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import type {
  KvByteValue,
  KvCommittedPair,
  KvKeyEncoding,
  KvResourceScope,
} from "@/features/kv/kv-models";
import { createKvRowsQuery } from "@/features/kv/kv-rows-query";
import { createKvValueQuery } from "@/features/kv/kv-value-query";
import { formatNumber } from "@/shared/format";
import { currentRouteFamilySegment, domainResourceHref } from "@/shared/navigation/domains";
import { parseConcreteRouteFamilyId, useOperatorScope } from "@/shared/operator-scope";

const DEFAULT_LIMIT = 50;

function decodeParam(value: string | undefined) {
  if (!value) return "";

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function parseLimit(value: string | null) {
  if (!value) return DEFAULT_LIMIT;
  const parsed = Number(value);

  return Number.isFinite(parsed) && parsed > 0 ? Math.min(Math.floor(parsed), 200) : DEFAULT_LIMIT;
}

function bytePreview(value: KvByteValue) {
  return value.utf8 ?? value.base64;
}

function bytePreviewKind(value: KvByteValue) {
  return value.utf8 ? "utf8" : "base64";
}

function rowsHref(
  scope: KvResourceScope,
  params: {
    cursor?: string | null;
    cursorTrail?: readonly string[];
    limit: number;
    startsWith: string;
  },
) {
  const query = new URLSearchParams();

  if (params.startsWith) query.set("startsWith", params.startsWith);
  if (params.cursor) query.set("cursor", params.cursor);
  for (const trailCursor of params.cursorTrail ?? []) {
    query.append("cursorTrail", trailCursor);
  }
  if (params.limit !== DEFAULT_LIMIT) query.set("limit", params.limit.toString());

  const queryString = query.toString();
  const href = domainResourceHref("kv", scope);

  return queryString ? `${href}?${queryString}` : href;
}

export default function KvResourcePage() {
  const route = currentRoute();
  const operator = useOperatorScope();
  const scope = {
    area: decodeParam(route.params.area),
    realm: decodeParam(route.params.realm),
    resource: decodeParam(route.params.resource),
  };
  const startsWith = route.query.get("startsWith") ?? "";
  const cursor = route.query.get("cursor");
  const cursorTrail = route.query.getAll("cursorTrail");
  const limit = parseLimit(route.query.get("limit"));
  const [startsWithDraft, setStartsWithDraft] = state(startsWith);
  const [limitDraft, setLimitDraft] = state(limit.toString());
  const [lookupKeyDraft, setLookupKeyDraft] = state("");
  const [lookupEncoding, setLookupEncoding] = state<KvKeyEncoding>("utf8");
  const [activeLookup, setActiveLookup] = state<{
    key: string;
    keyEncoding: KvKeyEncoding;
  } | null>(null);
  const selectedFamily = currentRouteFamilySegment() ?? operator.selectedRouteFamilyId;
  const concreteFamily = parseConcreteRouteFamilyId(selectedFamily);
  const rowsQuery =
    concreteFamily === null
      ? null
      : createKvRowsQuery(scope, {
          cursor,
          limit,
          startsWith,
        });
  const rows = rowsQuery?.data?.items ?? [];
  const lookup = activeLookup();
  const valueQuery =
    concreteFamily !== null && lookup
      ? createKvValueQuery(
          { ...scope, routeFamily: concreteFamily },
          lookup.key,
          lookup.keyEncoding,
        )
      : null;
  const valueResult = valueQuery?.data;
  const exactLookupMetrics =
    valueResult?.found && valueResult.value
      ? [
          { label: "Key", value: bytePreview(valueResult.key) },
          { label: "Key bytes", value: valueResult.key.lenBytes },
          { label: "Value", value: bytePreview(valueResult.value) },
          { label: "Value bytes", value: valueResult.value.lenBytes },
        ]
      : [];
  const rowColumns: readonly VirtualTableColumn<KvCommittedPair>[] = [
    {
      id: "key-bytes",
      header: "Key bytes",
      width: "12%",
      cellComponent: ({ row }) => <span>{formatNumber(row.key.lenBytes)}</span>,
    },
    {
      id: "key-preview",
      header: "Key preview",
      width: "26%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.key.base64}>
          {bytePreview(row.key)} ({bytePreviewKind(row.key)})
        </span>
      ),
    },
    {
      id: "value-bytes",
      header: "Value bytes",
      width: "12%",
      cellComponent: ({ row }) => <span>{formatNumber(row.value.lenBytes)}</span>,
    },
    {
      id: "value-preview",
      header: "Value preview",
      width: "28%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.value.base64}>
          {bytePreview(row.value)} ({bytePreviewKind(row.value)})
        </span>
      ),
    },
    {
      id: "actions",
      header: "Copy",
      width: "22%",
      cellComponent: ({ row }) => (
        <Inline gap="1" wrap="nowrap">
          <CopyTextButton label="Copy key" text={bytePreview(row.key)} />
          <CopyTextButton label="Copy value" text={bytePreview(row.value)} />
        </Inline>
      ),
    },
  ];
  const nextCursor = rowsQuery?.data?.nextCursor ?? null;
  const previousCursor = cursorTrail[cursorTrail.length - 1] ?? null;
  const previousCursorTrail = cursorTrail.slice(0, -1);

  function applyFilters(event: Event) {
    event.preventDefault();
    const nextLimit = parseLimit(limitDraft());
    navigate(
      rowsHref(scope, {
        limit: nextLimit,
        startsWith: startsWithDraft(),
      }),
    );
  }

  function lookUpKey(event: Event) {
    event.preventDefault();
    const key = lookupKeyDraft().trim();

    if (key.length > 0) {
      setActiveLookup({ key, keyEncoding: lookupEncoding() });
    }
  }

  const rowsStatus =
    concreteFamily === null
      ? {
          detail: "Committed row browsing requires a concrete Route Family.",
          label: "Select Route Family",
          tone: "warning" as const,
        }
      : queryHeaderStatus(rowsQuery ?? {}, {
          loading: "Loading committed KV rows.",
          ready: `${formatNumber(rows.length)} committed row${rows.length === 1 ? "" : "s"} visible for this resource.`,
          unavailable: "Committed KV rows are unavailable.",
        });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="KV resource"
          title={scope.resource}
          description={`${scope.realm} / ${scope.area}`}
          primaryAction={
            rowsQuery
              ? {
                  label: "Refresh rows",
                  onPress: () => rowsQuery.refresh(),
                }
              : undefined
          }
          status={rowsStatus}
        />
        <OperatorScopeStrip
          realm={scope.realm}
          area={scope.area}
          resource={scope.resource}
          freshness={
            concreteFamily === null ? "Route Family required" : queryFreshness(rowsQuery ?? {})
          }
        />

        <Card padding="sm" variant="default">
          <CardHeader>
            <CardTitle titleAs="h2">Exact key lookup</CardTitle>
            <CardDescription>
              Read the current committed value for one UTF-8 or base64-encoded key.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Form onSubmit={lookUpKey}>
              <Stack gap="3">
                <Inline align="end" gap="3" wrap="wrap">
                  <Stack gap="1">
                    <Label for="kv-exact-key">Key</Label>
                    <Input
                      id="kv-exact-key"
                      required
                      value={lookupKeyDraft()}
                      onInput={(event: Event) =>
                        setLookupKeyDraft((event.target as HTMLInputElement).value)
                      }
                    />
                  </Stack>
                  <div class="kv-encoding-controls" role="group" aria-label="Key encoding">
                    <Button
                      type="button"
                      variant={lookupEncoding() === "utf8" ? "secondary" : "outline"}
                      aria-pressed={lookupEncoding() === "utf8"}
                      onPress={() => setLookupEncoding("utf8")}
                    >
                      UTF-8
                    </Button>
                    <Button
                      type="button"
                      variant={lookupEncoding() === "base64" ? "secondary" : "outline"}
                      aria-pressed={lookupEncoding() === "base64"}
                      onPress={() => setLookupEncoding("base64")}
                    >
                      Base64
                    </Button>
                  </div>
                  <Button type="submit" disabled={concreteFamily === null}>
                    Look up key
                  </Button>
                </Inline>
              </Stack>
            </Form>
          </CardContent>
        </Card>

        <Show when={valueQuery?.loading}>
          <QueryLoadingState description="Looking up the committed KV value..." />
        </Show>
        <Show when={valueQuery?.error}>
          <QueryErrorState
            title="Unable to look up committed KV value"
            error={valueQuery?.error}
            onRetry={() => valueQuery?.refresh()}
          />
        </Show>
        <Show when={valueResult && !valueResult.found}>
          <QueryCompactEmptyState
            title="Key not found"
            description="No current committed value exists for this exact key."
          />
        </Show>
        <Show when={valueResult?.found && valueResult.value}>
          <Stack gap="2">
            <DomainMetricTable
              title="Exact key result"
              description={`Current committed value for the submitted ${lookup?.keyEncoding ?? "UTF-8"} key.`}
              metrics={exactLookupMetrics}
            />
            <Inline gap="2" wrap="wrap">
              <CopyTextButton
                label="Copy exact key"
                text={valueResult ? bytePreview(valueResult.key) : ""}
              />
              <CopyTextButton
                label="Copy exact value"
                text={valueResult?.value ? bytePreview(valueResult.value) : ""}
              />
            </Inline>
          </Stack>
        </Show>

        <Card padding="sm" variant="default">
          <CardHeader>
            <CardTitle titleAs="h2">Row filters</CardTitle>
            <CardDescription>Filter committed rows by key prefix and page size.</CardDescription>
          </CardHeader>
          <CardContent>
            <Form onSubmit={applyFilters}>
              <Inline align="end" gap="3" wrap="wrap">
                <Stack gap="1">
                  <Label for="kv-starts-with">Key starts with</Label>
                  <Input
                    id="kv-starts-with"
                    value={startsWithDraft()}
                    onInput={(event: Event) =>
                      setStartsWithDraft((event.target as HTMLInputElement).value)
                    }
                  />
                </Stack>
                <Stack gap="1">
                  <Label for="kv-limit">Limit</Label>
                  <Input
                    id="kv-limit"
                    {...({ min: 1 } as Record<string, unknown>)}
                    type="number"
                    value={limitDraft()}
                    onInput={(event: Event) =>
                      setLimitDraft((event.target as HTMLInputElement).value)
                    }
                  />
                </Stack>
                <Button type="submit">Apply filters</Button>
              </Inline>
            </Form>
          </CardContent>
        </Card>

        <Show when={concreteFamily === null}>
          <QueryEmptyState description="Select a concrete Route Family to browse committed KV rows." />
        </Show>

        <Show when={rowsQuery?.loading}>
          <QueryLoadingState description="Loading committed KV rows..." />
        </Show>

        <Show when={rowsQuery?.refreshing}>
          <QueryRefreshingState description="Refreshing committed KV rows..." />
        </Show>

        <Show when={rowsQuery?.error}>
          <QueryErrorState
            title="Unable to load committed KV rows"
            error={rowsQuery?.error}
            onRetry={() => rowsQuery?.refresh()}
          />
        </Show>

        <Show when={rowsQuery?.data && rows.length === 0}>
          <QueryEmptyState description="No committed KV rows match this resource and key prefix." />
        </Show>

        <Show when={rows.length > 0}>
          <Card padding="sm" variant="default">
            <CardHeader>
              <CardTitle titleAs="h2">Current authoritative KV rows</CardTitle>
              <CardDescription>
                Committed rows returned by the selected scope and filters.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <VirtualTable<KvCommittedPair>
                aria-label="Committed KV rows"
                class="domain-resource-virtual-table"
                columns={rowColumns}
                getKey={(row) => row.key.base64}
                headerHeight={44}
                overscan={6}
                rowHeight={52}
                rows={rows}
                style={{ height: `${Math.min(420, Math.max(144, 44 + rows.length * 52))}px` }}
              />
            </CardContent>
          </Card>
        </Show>

        <Inline gap="2" wrap="wrap">
          <Show when={cursor !== null}>
            <Link class="page-action-link" href={rowsHref(scope, { limit, startsWith })}>
              First page
            </Link>
            <Link
              class="page-action-link"
              href={rowsHref(scope, {
                cursor: previousCursor,
                cursorTrail: previousCursorTrail,
                limit,
                startsWith,
              })}
            >
              Previous page
            </Link>
          </Show>
          <Show when={rowsQuery?.data?.hasMore && nextCursor}>
            <Button asChild>
              <Link
                href={rowsHref(scope, {
                  cursor: nextCursor,
                  cursorTrail: [...cursorTrail, cursor ?? ""],
                  limit,
                  startsWith,
                })}
              >
                Next page
              </Link>
            </Button>
          </Show>
        </Inline>
      </Stack>
    </DomainPageFrame>
  );
}
