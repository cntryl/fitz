import { state } from "@askrjs/askr";
import { For, Show } from "@askrjs/askr/control";
import { currentRoute, Link, navigate } from "@askrjs/askr/router";
import { Button, Inline, Stack } from "@askrjs/themes/components";
import {
  Form,
  Input,
  Label,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeaderCell,
  TableRow,
} from "@askrjs/ui";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createStreamResourceQuery } from "@/features/stream/stream-query";
import { formatNumber } from "@/shared/format";
import { domainResourceHref, domainScopeHref } from "@/shared/navigation/domains";

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
          primaryAction={{ label: "Refresh records", onPress: () => query.refresh() }}
          status={{
            detail: data
              ? `${formatNumber(records.length)} committed record(s) visible from offset ${formatNumber(data.records.from_offset)}.`
              : "Loading stream resource.",
            label: query.refreshing ? "Refreshing" : query.stale ? "Stale" : "Live",
            tone: query.refreshing ? "info" : query.stale ? "warning" : "success",
          }}
        />
        {data ? (
          <DomainMetricTable
            title="Stream resource metrics"
            description="Durable committed metadata and live append session count."
            metrics={[
              { label: "Offset", value: data.detail.offset },
              { label: "Watermark", value: data.detail.watermark },
              { label: "Size bytes", value: data.detail.size_bytes },
              { label: "Append sessions", value: data.detail.sessions_active },
            ]}
          />
        ) : null}
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
                onInput={(event: Event) => setLimitDraft((event.target as HTMLInputElement).value)}
              />
            </Stack>
            <Button type="submit">Apply</Button>
          </Inline>
        </Form>
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
          <QueryEmptyState description="No committed stream records matched this offset." />
        </Show>
        <Show when={records.length > 0}>
          <Table>
            <TableHead>
              <TableRow>
                <TableHeaderCell>Offset</TableHeaderCell>
                <TableHeaderCell>Family</TableHeaderCell>
                <TableHeaderCell>Created</TableHeaderCell>
                <TableHeaderCell>Body</TableHeaderCell>
              </TableRow>
            </TableHead>
            <TableBody>
              <For
                each={records}
                by={(record) => `${record.route_family}:${record.resource_offset}`}
              >
                {(record) => (
                  <TableRow>
                    <TableCell>{formatNumber(record.resource_offset)}</TableCell>
                    <TableCell>{formatNumber(record.route_family)}</TableCell>
                    <TableCell>{formatNumber(record.created_at_ms)}</TableCell>
                    <TableCell>
                      <span class="domain-table-cell-truncate" title={record.body.base64}>
                        {record.body.utf8 ?? record.body.base64}
                      </span>
                    </TableCell>
                  </TableRow>
                )}
              </For>
            </TableBody>
          </Table>
        </Show>
        <Inline gap="2" wrap="wrap">
          <Button asChild variant="outline">
            <Link href={domainScopeHref("stream", { area: scope.area, realm: scope.realm })}>
              Back to area
            </Link>
          </Button>
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
