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
import DomainPageFrame from "@/components/shared/domain-page-frame";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import type { KvByteValue, KvCommittedPair, KvResourceScope } from "@/features/kv/kv-models";
import { createKvRowsQuery } from "@/features/kv/kv-rows-query";
import { formatNumber } from "@/shared/format";
import {
  currentRouteFamilySegment,
  domainResourceHref,
  domainScopeHref,
} from "@/shared/navigation/domains";
import { parseConcreteRouteFamilyId, useOperatorContext } from "@/shared/operator-context";

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
  params: { cursor?: string | null; limit: number; startsWith: string },
) {
  const query = new URLSearchParams();

  if (params.startsWith) query.set("startsWith", params.startsWith);
  if (params.cursor) query.set("cursor", params.cursor);
  if (params.limit !== DEFAULT_LIMIT) query.set("limit", params.limit.toString());

  const queryString = query.toString();
  const href = domainResourceHref("kv", scope);

  return queryString ? `${href}?${queryString}` : href;
}

export default function KvResourcePage() {
  const route = currentRoute();
  const operator = useOperatorContext();
  const scope = {
    area: decodeParam(route.params.area),
    realm: decodeParam(route.params.realm),
    resource: decodeParam(route.params.resource),
  };
  const startsWith = route.query.get("startsWith") ?? "";
  const cursor = route.query.get("cursor");
  const limit = parseLimit(route.query.get("limit"));
  const [startsWithDraft, setStartsWithDraft] = state(startsWith);
  const [limitDraft, setLimitDraft] = state(limit.toString());
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
  const rowColumns: readonly VirtualTableColumn<KvCommittedPair>[] = [
    {
      id: "key-bytes",
      header: "Key bytes",
      width: "14%",
      cellComponent: ({ row }) => <span>{formatNumber(row.key.lenBytes)}</span>,
    },
    {
      id: "key-preview",
      header: "Key preview",
      width: "36%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.key.base64}>
          {bytePreview(row.key)} ({bytePreviewKind(row.key)})
        </span>
      ),
    },
    {
      id: "value-bytes",
      header: "Value bytes",
      width: "14%",
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
  ];
  const nextCursor = rowsQuery?.data?.nextCursor ?? null;

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
          status={{
            detail:
              concreteFamily === null
                ? "Committed row browsing requires a concrete route family."
                : rowsQuery?.data
                  ? `${formatNumber(rows.length)} committed row${rows.length === 1 ? "" : "s"} visible for this resource.`
                  : "Loading committed KV rows.",
            label:
              concreteFamily === null
                ? "Select Route Family"
                : rowsQuery?.refreshing
                  ? "Refreshing"
                  : rowsQuery?.stale
                    ? "Stale"
                    : "Live",
            tone:
              concreteFamily === null
                ? "warning"
                : rowsQuery?.refreshing
                  ? "info"
                  : rowsQuery?.stale
                    ? "warning"
                    : "success",
          }}
        />
        <OperatorScopeStrip
          realm={scope.realm}
          area={scope.area}
          resource={scope.resource}
          freshness={
            concreteFamily === null
              ? "Route Family required"
              : rowsQuery?.refreshing
                ? "Refreshing"
                : rowsQuery?.stale
                  ? "Stale"
                  : rowsQuery?.data
                    ? "Live"
                    : rowsQuery?.loading
                      ? "Loading"
                      : undefined
          }
        />

        <Card padding="sm" variant="default">
          <CardHeader>
            <CardTitle>Row filters</CardTitle>
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
                    min="1"
                    type="number"
                    value={limitDraft()}
                    onInput={(event: Event) =>
                      setLimitDraft((event.target as HTMLInputElement).value)
                    }
                  />
                </Stack>
                <Button type="submit">Apply</Button>
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
            title="Unable to browse committed KV rows"
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
              <CardTitle>Current authoritative KV rows</CardTitle>
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
                style={{ height: "420px" }}
              />
            </CardContent>
          </Card>
        </Show>

        <Inline gap="2" wrap="wrap">
          <Button asChild variant="outline">
            <Link href={domainScopeHref("kv", { area: scope.area, realm: scope.realm })}>
              Back to area
            </Link>
          </Button>
          <Show when={rowsQuery?.data?.hasMore && nextCursor}>
            <Button asChild>
              <Link
                href={rowsHref(scope, {
                  cursor: nextCursor,
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
