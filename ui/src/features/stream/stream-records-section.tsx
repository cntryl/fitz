import { Show } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { RefreshCwIcon } from "@askrjs/lucide";
import { Block, Button } from "@askrjs/themes/components";
import type { StreamAdminRecord } from "@/adapters";
import CopyTextButton from "@/components/shared/copy-text-button";
import DataTable, { type DataTableColumn } from "@/components/shared/data-table";
import DomainDataSection from "@/components/shared/domain-data-section";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import TitledCell from "@/components/shared/titled-cell";
import { createStreamRecordsQuery } from "@/features/stream/stream-query";
import { formatNumber, formatTimestampMs } from "@/shared/format";
import { domainResourceHref } from "@/shared/navigation/domains";
import { rowsRequestQuery } from "@/shared/navigation/rows-request";

export const DEFAULT_RECORDS_LIMIT = 50;

export interface StreamRecordsScope {
  area: string;
  realm: string;
  resource: string;
}

export function recordsHref(
  scope: StreamRecordsScope,
  params: {
    discriminator?: string;
    fromOffset: number;
    limit: number;
  },
) {
  const query = rowsRequestQuery();

  if (params.fromOffset > 0) query.set("fromOffset", params.fromOffset.toString());
  if (params.discriminator) query.set("discriminator", params.discriminator);
  if (params.limit !== DEFAULT_RECORDS_LIMIT) query.set("limit", params.limit.toString());

  return `${domainResourceHref("stream", scope)}?${query.toString()}`;
}

const recordColumns: readonly DataTableColumn<StreamAdminRecord>[] = [
  {
    id: "offset",
    header: "Offset",
    width: "16%",
    cellComponent: ({ row }) => <span>{formatNumber(row.resource_offset)}</span>,
  },
  {
    id: "body",
    header: "Body",
    width: "64%",
    cellComponent: ({ row }) => (
      <TitledCell title={row.body.base64} subtitle={formatTimestampMs(row.created_at_ms)}>
        {row.body.utf8 ?? row.body.base64}
      </TitledCell>
    ),
  },
  {
    id: "actions",
    header: "Action",
    width: "20%",
    cellComponent: ({ row }) => (
      <CopyTextButton
        label={`Copy body at offset ${row.resource_offset}`}
        text={row.body.utf8 ?? row.body.base64}
      />
    ),
  },
];

export interface StreamRecordsSectionProps {
  discriminator: string;
  fromOffset: number;
  limit: number;
  scope: StreamRecordsScope;
}

/**
 * Committed records are data rows, so the page renders this section only after
 * the operator asks; creating it is what starts the read.
 */
export default function StreamRecordsSection({
  discriminator,
  fromOffset,
  limit,
  scope,
}: StreamRecordsSectionProps) {
  const recordsQuery = createStreamRecordsQuery({ ...scope, discriminator, fromOffset, limit });
  const recordsWindow = recordsQuery.data;
  const records = recordsWindow?.records ?? [];

  return (
    <Block direction="column" gap="sm">
      <Block direction="row" justify="end">
        <Button
          variant="outline"
          aria-busy={recordsQuery.refreshing ? "true" : undefined}
          disabled={recordsQuery.refreshing}
          onPress={() => void recordsQuery.refresh()}
        >
          <RefreshCwIcon size={16} />
          Refresh records
        </Button>
      </Block>
      <Show when={recordsQuery.loading}>
        <QueryLoadingState description="Loading committed stream records..." />
      </Show>
      <Show when={recordsQuery.refreshing}>
        <QueryRefreshingState description="Refreshing committed stream records..." />
      </Show>
      <Show when={recordsQuery.error}>
        <QueryErrorState
          title="Unable to read stream records"
          error={recordsQuery.error}
          onRetry={() => recordsQuery.refresh()}
        />
      </Show>
      <Show when={recordsWindow && records.length === 0}>
        <QueryEmptyState description="No committed stream records matched this offset and discriminator." />
      </Show>
      <Show when={records.length > 0}>
        <DomainDataSection
          id="stream-committed-records"
          title="Committed records"
          description="Durable stream records returned by the current read window."
        >
          <Block direction="column" gap="xs">
            <DataTable<StreamAdminRecord>
              ariaLabel="Stream records"
              class="stream-record-table"
              columns={recordColumns}
              getKey={(record) => record.resource_offset}
              rows={records}
            />
          </Block>
        </DomainDataSection>
      </Show>
      <Block as="nav" aria-label="Record pages" direction="row" gap="xs" wrap={true}>
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
        <Show when={recordsWindow?.has_more}>
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
      </Block>
    </Block>
  );
}
