import { state } from "@askrjs/askr";
import { Show } from "@askrjs/askr/control";
import { currentRoute, Link, navigate } from "@askrjs/askr/router";
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
import { Form, Input, Label, VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import type { StreamAdminRecord } from "@/adapters";
import CopyTextButton from "@/components/shared/copy-text-button";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import { queryFreshness, queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createStreamResourceQuery } from "@/features/stream/stream-query";
import { formatCount, formatNumber, formatTimestampMs } from "@/shared/format";
import { domainResourceHref, formatFitzRoute } from "@/shared/navigation/domains";

const DEFAULT_LIMIT = 50;

function decodeParam(value: string | undefined) {
  if (!value) return "";

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function parsePositiveInt(value: string | null, fallback: number) {
  const parsed = Number(value ?? fallback);
  return Number.isFinite(parsed) ? Math.max(0, Math.floor(parsed)) : fallback;
}

function recordsHref(
  scope: { area: string; realm: string; resource: string },
  params: {
    discriminator?: string;
    fromOffset: number;
    limit: number;
  },
) {
  const query = new URLSearchParams();

  if (params.fromOffset > 0) query.set("fromOffset", params.fromOffset.toString());
  if (params.discriminator) query.set("discriminator", params.discriminator);
  if (params.limit !== DEFAULT_LIMIT) query.set("limit", params.limit.toString());

  const queryString = query.toString();
  const href = domainResourceHref("stream", scope);

  return queryString ? `${href}?${queryString}` : href;
}

const recordColumns: readonly VirtualTableColumn<StreamAdminRecord>[] = [
  {
    id: "route",
    header: "Route",
    width: "28%",
    cellComponent: ({ row }) => {
      const route = formatFitzRoute("stream", row);

      return (
        <span class="domain-table-cell-truncate" title={route}>
          {route}
        </span>
      );
    },
  },
  {
    id: "offset",
    header: "Offset",
    width: "9%",
    cellComponent: ({ row }) => <span>{formatNumber(row.resource_offset)}</span>,
  },
  {
    id: "family",
    header: "Family",
    width: "8%",
    cellComponent: ({ row }) => <span>{formatNumber(row.route_family)}</span>,
  },
  {
    id: "created",
    header: "Created",
    width: "18%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={formatTimestampMs(row.created_at_ms)}>
        {formatTimestampMs(row.created_at_ms)}
      </span>
    ),
  },
  {
    id: "body",
    header: "Body",
    width: "25%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={row.body.base64}>
        {row.body.utf8 ?? row.body.base64}
      </span>
    ),
  },
  {
    id: "actions",
    header: "Copy",
    width: "12%",
    cellComponent: ({ row }) => (
      <CopyTextButton
        label={`Copy body at offset ${row.resource_offset}`}
        text={row.body.utf8 ?? row.body.base64}
      />
    ),
  },
];

function recordTableHeight(rowCount: number) {
  return `${Math.min(432, Math.max(144, 44 + rowCount * 48))}px`;
}

export default function StreamResourcePage() {
  const route = currentRoute();
  const scope = {
    area: decodeParam(route.params.area),
    realm: decodeParam(route.params.realm),
    resource: decodeParam(route.params.resource),
  };
  const fromOffset = parsePositiveInt(route.query.get("fromOffset"), 0);
  const limit = Math.min(parsePositiveInt(route.query.get("limit"), DEFAULT_LIMIT), 200);
  const discriminator = route.query.get("discriminator") ?? "";
  const [fromOffsetDraft, setFromOffsetDraft] = state(fromOffset.toString());
  const [limitDraft, setLimitDraft] = state(limit.toString());
  const [discriminatorDraft, setDiscriminatorDraft] = state(discriminator);
  const query = createStreamResourceQuery({ ...scope, discriminator, fromOffset, limit });
  const data = query.data;
  const records = data?.records.records ?? [];
  const headerStatus = queryHeaderStatus(
    query,
    {
      loading: "Loading stream resource.",
      ready: `${formatCount(records.length, "committed record")} visible from offset ${formatNumber(data?.records.from_offset ?? fromOffset)}.`,
      unavailable: "Committed stream records are unavailable.",
    },
    { label: "Committed", tone: "success" },
  );

  function applyFilters(event: Event) {
    event.preventDefault();
    navigate(
      recordsHref(scope, {
        discriminator: discriminatorDraft(),
        fromOffset: parsePositiveInt(fromOffsetDraft(), 0),
        limit: Math.min(parsePositiveInt(limitDraft(), DEFAULT_LIMIT), 200),
      }),
    );
  }

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Stream resource"
          title={scope.resource}
          description={`${scope.realm} / ${scope.area}`}
          primaryAction={{
            busy: query.refreshing,
            disabled: query.refreshing,
            label: "Refresh records",
            onPress: () => query.refresh(),
          }}
          status={headerStatus}
        />
        <OperatorScopeStrip
          realm={scope.realm}
          area={scope.area}
          resource={scope.resource}
          freshness={queryFreshness(query)}
        />
        {data ? (
          <DomainMetricTable
            title="Stream resource metrics"
            description="Durable committed metadata. Append sessions are live and separate from replay history."
            metrics={[
              {
                label: "Latest committed offset",
                value: data.detail.offset,
                caption: "Resource high-water metadata, not the read cursor",
              },
              {
                label: "Watermark",
                value: data.detail.watermark,
                caption: "Durable committed metadata",
              },
              {
                label: "Size bytes",
                value: data.detail.size_bytes,
                caption: "Durable committed metadata",
              },
              {
                label: "Append sessions",
                value: data.detail.sessions_active,
                caption: "Live append sessions",
              },
            ]}
          />
        ) : null}
        <Card padding="sm" variant="default">
          <CardHeader>
            <CardTitle titleAs="h2">Record filters</CardTitle>
            <CardDescription>
              Read committed records by from offset, optional discriminator, and limit.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Form onSubmit={applyFilters}>
              <Inline align="end" gap="3" wrap="wrap">
                <Stack gap="1">
                  <Label for="stream-from-offset">From offset</Label>
                  <Input
                    id="stream-from-offset"
                    min="0"
                    type="number"
                    value={fromOffsetDraft()}
                    onInput={(event: Event) =>
                      setFromOffsetDraft((event.target as HTMLInputElement).value)
                    }
                  />
                </Stack>
                <Stack gap="1">
                  <Label for="stream-discriminator">Discriminator</Label>
                  <Input
                    id="stream-discriminator"
                    value={discriminatorDraft()}
                    onInput={(event: Event) =>
                      setDiscriminatorDraft((event.target as HTMLInputElement).value)
                    }
                  />
                </Stack>
                <Stack gap="1">
                  <Label for="stream-limit">Limit</Label>
                  <Input
                    id="stream-limit"
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
        <Show when={query.loading}>
          <QueryLoadingState description="Loading committed stream records..." />
        </Show>
        <Show when={query.refreshing}>
          <QueryRefreshingState description="Refreshing committed stream records..." />
        </Show>
        <Show when={query.error}>
          <QueryErrorState
            title="Unable to read stream records"
            error={query.error}
            onRetry={() => query.refresh()}
          />
        </Show>
        <Show when={data && records.length === 0}>
          <QueryEmptyState description="No committed stream records matched this offset and discriminator." />
        </Show>
        <Show when={records.length > 0}>
          <Card padding="sm" variant="default">
            <CardHeader>
              <CardTitle titleAs="h2">Committed records</CardTitle>
              <CardDescription>
                Durable stream records returned by the current read window.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <VirtualTable<StreamAdminRecord>
                aria-label="Stream records"
                class="stream-resource-virtual-table"
                columns={recordColumns}
                getKey={(record) => `${record.route_family}:${record.resource_offset}`}
                headerHeight={44}
                overscan={8}
                rowHeight={48}
                rows={records}
                style={{ height: recordTableHeight(records.length) }}
              />
            </CardContent>
          </Card>
        </Show>
        <Inline gap="2" wrap="wrap">
          <Show when={fromOffset > 0}>
            <Link
              class="page-action-link"
              href={recordsHref(scope, { discriminator, fromOffset: 0, limit })}
            >
              First page
            </Link>
            <Link
              class="page-action-link"
              href={recordsHref(scope, {
                discriminator,
                fromOffset: Math.max(0, fromOffset - limit),
                limit,
              })}
            >
              Previous page
            </Link>
          </Show>
          <Show when={data?.records.has_more}>
            <Button asChild>
              <Link
                href={recordsHref(scope, {
                  discriminator,
                  fromOffset: (records[records.length - 1]?.resource_offset ?? fromOffset) + 1,
                  limit,
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
