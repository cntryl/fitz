import { state } from "@askrjs/askr";
import { Show } from "@askrjs/askr/control";
import { currentRoute, Link, navigate } from "@askrjs/askr/router";
import { Form, Input, Label } from "@askrjs/ui";
import {
  Button,
  Block,
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemGroup,
  ItemTitle,
  Text,
} from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import CopyTextButton from "@/components/shared/copy-text-button";
import DataTable, { type DataTableColumn } from "@/components/shared/data-table";
import DomainDataSection from "@/components/shared/domain-data-section";
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
  const rowsQueryCell = createKvRowsQuery(
    scope,
    {
      cursor,
      limit,
      startsWith,
    },
    { skipInitialFetch: concreteFamily === null },
  );
  const rowsQuery = concreteFamily === null ? null : rowsQueryCell;
  const rows = rowsQuery?.data?.items ?? [];
  const lookup = activeLookup();
  const valueQueryCell = createKvValueQuery(
    { ...scope, routeFamily: concreteFamily ?? 0 },
    lookup?.key ?? "",
    lookup?.keyEncoding ?? "utf8",
    { skipInitialFetch: concreteFamily === null || lookup === null },
  );
  const valueQuery = concreteFamily !== null && lookup ? valueQueryCell : null;
  const valueResult = valueQuery?.data;
  const rowColumns: readonly DataTableColumn<KvCommittedPair>[] = [
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
      width: "36%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.value.base64}>
          {bytePreview(row.value)} ({bytePreviewKind(row.value)})
        </span>
      ),
    },
    {
      id: "actions",
      header: "Action",
      width: "14%",
      cellComponent: ({ row }) => (
        <CopyTextButton label="Copy value" text={bytePreview(row.value)} />
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
      <Block direction="column" gap="sm">
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

        <DomainDataSection
          id="kv-exact-key-lookup"
          title="Exact key lookup"
          description="Read the current committed value for one UTF-8 or base64-encoded key."
        >
          <Block borderTop borderBottom paddingY="sm">
            <Form onSubmit={lookUpKey}>
              <Block direction="column" gap="sm">
                <Block
                  direction={{ base: "column", sm: "row" }}
                  align={{ base: "stretch", sm: "end" }}
                  gap="sm"
                  wrap={true}
                >
                  <Block direction="column" gap="xs" width={{ base: "full", sm: "auto" }}>
                    <Label for="kv-exact-key">Key</Label>
                    <Input
                      id="kv-exact-key"
                      required
                      value={lookupKeyDraft()}
                      onInput={(event: Event) =>
                        setLookupKeyDraft((event.target as HTMLInputElement).value)
                      }
                    />
                  </Block>
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
                </Block>
              </Block>
            </Form>
          </Block>
        </DomainDataSection>

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
          <DomainDataSection
            id="kv-exact-key-result"
            title="Exact key result"
            description={`Current committed value for the submitted ${lookup?.keyEncoding ?? "UTF-8"} key.`}
          >
            <ItemGroup role="list" aria-label="Exact key result">
              <Item role="listitem" variant="outline">
                <ItemContent>
                  <ItemTitle>Key</ItemTitle>
                  <ItemDescription>
                    <Text as="span" font="mono" wrap="anywhere">
                      {valueResult ? bytePreview(valueResult.key) : ""}
                    </Text>
                  </ItemDescription>
                  <ItemDescription>
                    {valueResult ? formatNumber(valueResult.key.lenBytes) : "0"} bytes ·{" "}
                    {valueResult ? bytePreviewKind(valueResult.key) : "utf8"}
                  </ItemDescription>
                </ItemContent>
                <ItemActions>
                  <CopyTextButton
                    label="Copy exact key"
                    text={valueResult ? bytePreview(valueResult.key) : ""}
                  />
                </ItemActions>
              </Item>
              <Item role="listitem" variant="outline">
                <ItemContent>
                  <ItemTitle>Value</ItemTitle>
                  <ItemDescription>
                    <Text as="span" font="mono" wrap="anywhere">
                      {valueResult?.value ? bytePreview(valueResult.value) : ""}
                    </Text>
                  </ItemDescription>
                  <ItemDescription>
                    {valueResult?.value ? formatNumber(valueResult.value.lenBytes) : "0"} bytes ·{" "}
                    {valueResult?.value ? bytePreviewKind(valueResult.value) : "utf8"}
                  </ItemDescription>
                </ItemContent>
                <ItemActions>
                  <CopyTextButton
                    label="Copy exact value"
                    text={valueResult?.value ? bytePreview(valueResult.value) : ""}
                  />
                </ItemActions>
              </Item>
            </ItemGroup>
          </DomainDataSection>
        </Show>

        <DomainDataSection
          id="kv-row-filters"
          title="Row filters"
          description="Filter committed rows by key prefix and page size."
        >
          <Block borderTop borderBottom paddingY="sm">
            <Form onSubmit={applyFilters}>
              <Block
                direction={{ base: "column", sm: "row" }}
                align={{ base: "stretch", sm: "end" }}
                gap="sm"
                wrap={true}
              >
                <Block direction="column" gap="xs" width={{ base: "full", sm: "auto" }}>
                  <Label for="kv-starts-with">Key starts with</Label>
                  <Input
                    id="kv-starts-with"
                    value={startsWithDraft()}
                    onInput={(event: Event) =>
                      setStartsWithDraft((event.target as HTMLInputElement).value)
                    }
                  />
                </Block>
                <Block direction="column" gap="xs" width={{ base: "full", sm: "auto" }}>
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
                </Block>
                <Button type="submit">Apply filters</Button>
              </Block>
            </Form>
          </Block>
        </DomainDataSection>

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
          <DomainDataSection
            id="kv-committed-rows"
            title="Current authoritative KV rows"
            description="Committed rows returned by the selected scope and filters."
          >
            <Block direction="column" gap="xs">
              <p class="domain-inventory-scroll-hint">
                Scroll horizontally to inspect every row field and action.
              </p>
              <DataTable<KvCommittedPair>
                ariaLabel="Committed KV rows"
                class="domain-resource-data-table"
                columns={rowColumns}
                getKey={(row) => row.key.base64}
                rows={rows}
              />
            </Block>
          </DomainDataSection>
        </Show>

        <Block direction="row" gap="xs" wrap={true}>
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
        </Block>
      </Block>
    </DomainPageFrame>
  );
}
