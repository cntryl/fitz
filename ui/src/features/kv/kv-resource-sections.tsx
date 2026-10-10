import { Show } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { RefreshCwIcon } from "@askrjs/lucide";
import {
  Block,
  Button,
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemGroup,
  ItemTitle,
  Text,
} from "@askrjs/themes/components";
import CopyTextButton from "@/components/shared/copy-text-button";
import DataTable, { type DataTableColumn } from "@/components/shared/data-table";
import DomainDataSection from "@/components/shared/domain-data-section";
import {
  QueryCompactEmptyState,
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import TitledCell from "@/components/shared/titled-cell";
import type {
  KvByteValue,
  KvCommittedPair,
  KvKeyEncoding,
  KvResourceScope,
} from "@/features/kv/kv-models";
import { createKvRowsQuery } from "@/features/kv/kv-rows-query";
import { createKvTransactionsQuery } from "@/features/kv/kv-query";
import { createKvValueQuery } from "@/features/kv/kv-value-query";
import { formatNumber, formatTimestamp } from "@/shared/format";
import { domainResourceHref } from "@/shared/navigation/domains";
import { rowsRequestQuery } from "@/shared/navigation/rows-request";

export const DEFAULT_ROWS_LIMIT = 50;

function bytePreview(value: KvByteValue) {
  return value.utf8 ?? value.base64;
}

function bytePreviewKind(value: KvByteValue) {
  return value.utf8 ? "utf8" : "base64";
}

export function rowsHref(
  scope: KvResourceScope,
  params: {
    cursor?: string | null;
    cursorTrail?: readonly string[];
    limit: number;
    startsWith: string;
    transactions?: boolean;
  },
) {
  const query = rowsRequestQuery();

  if (params.startsWith) query.set("startsWith", params.startsWith);
  if (params.cursor) query.set("cursor", params.cursor);
  for (const trailCursor of params.cursorTrail ?? []) {
    query.append("cursorTrail", trailCursor);
  }
  if (params.limit !== DEFAULT_ROWS_LIMIT) query.set("limit", params.limit.toString());
  if (params.transactions) query.set("transactions", "1");

  return `${domainResourceHref("kv", scope)}?${query.toString()}`;
}

export interface KvRowsSectionProps {
  cursor: string | null;
  cursorTrail: readonly string[];
  limit: number;
  scope: KvResourceScope;
  startsWith: string;
  transactions: boolean;
}

/**
 * Committed rows are data rows, so the page renders this section only after the
 * operator asks; creating it is what starts the read.
 */
export function KvRowsSection({
  cursor,
  cursorTrail,
  limit,
  scope,
  startsWith,
  transactions,
}: KvRowsSectionProps) {
  const rowsQuery = createKvRowsQuery(scope, { cursor, limit, startsWith });
  const rows = rowsQuery.data?.items ?? [];
  const rowColumns: readonly DataTableColumn<KvCommittedPair>[] = [
    {
      id: "key",
      header: "Key",
      width: "80%",
      cellComponent: ({ row }) => (
        <TitledCell
          title={row.key.base64}
          subtitle={`${bytePreview(row.value)} (${bytePreviewKind(row.value)}) · key ${formatNumber(
            row.key.lenBytes,
          )} B · value ${formatNumber(row.value.lenBytes)} B`}
        >
          {bytePreview(row.key)} ({bytePreviewKind(row.key)})
        </TitledCell>
      ),
    },
    {
      id: "actions",
      header: "Action",
      width: "20%",
      cellComponent: ({ row }) => (
        <CopyTextButton label="Copy value" text={bytePreview(row.value)} />
      ),
    },
  ];
  const nextCursor = rowsQuery.data?.nextCursor ?? null;
  const previousCursor = cursorTrail[cursorTrail.length - 1] ?? null;
  const previousCursorTrail = cursorTrail.slice(0, -1);

  return (
    <Block direction="column" gap="sm">
      <Block direction="row" justify="end">
        <Button
          variant="outline"
          aria-busy={rowsQuery.refreshing ? "true" : undefined}
          disabled={rowsQuery.refreshing}
          onPress={() => void rowsQuery.refresh()}
        >
          <RefreshCwIcon size={16} />
          Refresh rows
        </Button>
      </Block>
      <Show when={rowsQuery.loading}>
        <QueryLoadingState description="Loading committed KV rows..." />
      </Show>

      <Show when={rowsQuery.refreshing}>
        <QueryRefreshingState description="Refreshing committed KV rows..." />
      </Show>

      <Show when={rowsQuery.error}>
        <QueryErrorState
          title="Unable to load committed KV rows"
          error={rowsQuery.error}
          onRetry={() => rowsQuery.refresh()}
        />
      </Show>

      <Show when={rowsQuery.data && rows.length === 0}>
        <QueryEmptyState description="No committed KV rows match this resource and key prefix." />
      </Show>

      <Show when={rows.length > 0}>
        <DomainDataSection
          id="kv-committed-rows"
          title="Current authoritative KV rows"
          description="Committed rows returned by the selected scope and filters."
        >
          <Block direction="column" gap="xs">
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
          <Link
            class="page-action-link"
            href={rowsHref(scope, { limit, startsWith, transactions })}
          >
            First page
          </Link>
          <Link
            class="page-action-link"
            href={rowsHref(scope, {
              cursor: previousCursor,
              cursorTrail: previousCursorTrail,
              limit,
              startsWith,
              transactions,
            })}
          >
            Previous page
          </Link>
        </Show>
        <Show when={rowsQuery.data?.hasMore && nextCursor}>
          <Button asChild>
            <Link
              href={rowsHref(scope, {
                cursor: nextCursor,
                cursorTrail: [...cursorTrail, cursor ?? ""],
                limit,
                startsWith,
                transactions,
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

export function KvTransactionsSection({ scope }: { scope: KvResourceScope }) {
  const query = createKvTransactionsQuery(scope);
  const transactions = query.data ?? [];

  return (
    <DomainDataSection id="kv-active-transactions" title="Active transactions">
      <Block direction="column" gap="sm">
        <Show when={query.loading && !query.data}>
          <QueryLoadingState description="Loading active KV transactions..." />
        </Show>
        <Show when={query.error}>
          <QueryErrorState
            title="Unable to load active KV transactions"
            error={query.error}
            onRetry={() => query.refresh()}
          />
        </Show>
        <Show when={query.data && transactions.length === 0}>
          <QueryCompactEmptyState
            title="No active transactions"
            description="No active transaction rows are currently reported."
          />
        </Show>
        <Show when={transactions.length > 0}>
          <div class="domain-table-wrap">
            <Table>
              <TableHead>
                <TableRow>
                  <TableHeaderCell>Transaction</TableHeaderCell>
                  <TableHeaderCell>Mode</TableHeaderCell>
                  <TableHeaderCell>Started</TableHeaderCell>
                  <TableHeaderCell>Idle</TableHeaderCell>
                  <TableHeaderCell>Operations</TableHeaderCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {transactions.map((transaction) => (
                  <TableRow>
                    <TableCell>{transaction.txId ?? "--"}</TableCell>
                    <TableCell>{transaction.mode ?? "--"}</TableCell>
                    <TableCell>
                      {transaction.startedAt ? formatTimestamp(transaction.startedAt) : "--"}
                    </TableCell>
                    <TableCell>
                      {transaction.idleSeconds === undefined
                        ? "--"
                        : `${formatNumber(transaction.idleSeconds)}s`}
                    </TableCell>
                    <TableCell>
                      {transaction.operationsCount === undefined
                        ? "--"
                        : formatNumber(transaction.operationsCount)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </Show>
      </Block>
    </DomainDataSection>
  );
}

export interface KvValueLookupResultProps {
  lookup: { key: string; keyEncoding: KvKeyEncoding };
  routeFamily: number;
  scope: KvResourceScope;
}

/** Reads one exact key; rendered only once a key has been submitted. */
export function KvValueLookupResult({ lookup, routeFamily, scope }: KvValueLookupResultProps) {
  const valueQuery = createKvValueQuery({ ...scope, routeFamily }, lookup.key, lookup.keyEncoding);
  const valueResult = valueQuery.data;

  return (
    <Block direction="column" gap="sm">
      <Show when={valueQuery.loading}>
        <QueryLoadingState description="Looking up the committed KV value..." />
      </Show>
      <Show when={valueQuery.error}>
        <QueryErrorState
          title="Unable to look up committed KV value"
          error={valueQuery.error}
          onRetry={() => valueQuery.refresh()}
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
          description={`Current committed value for the submitted ${lookup.keyEncoding} key.`}
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
    </Block>
  );
}
