import { state } from "@askrjs/askr";
import { Show } from "@askrjs/askr/control";
import { currentRoute, navigate } from "@askrjs/askr/router";
import { Form, Input, Label } from "@askrjs/ui";
import { Button, Block } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainFacts from "@/components/shared/domain-facts";
import RowsRequestPrompt from "@/components/shared/rows-request-prompt";
import { hasRowsRequest, revealSectionHref } from "@/shared/navigation/rows-request";
import DomainDataSection from "@/components/shared/domain-data-section";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
} from "@/components/shared/query-state";
import type { KvKeyEncoding } from "@/features/kv/kv-models";
import {
  DEFAULT_ROWS_LIMIT,
  KvRowsSection,
  KvTransactionsSection,
  KvValueLookupResult,
  rowsHref,
} from "@/features/kv/kv-resource-sections";
import { createKvResourceDetailQuery } from "@/features/kv/kv-query";
import { formatBytes, formatNumber } from "@/shared/format";
import { currentRouteFamilySegment, domainResourceHref } from "@/shared/navigation/domains";
import { parseConcreteRouteFamilyId, useOperatorScope } from "@/shared/operator-scope";

function decodeParam(value: string | undefined) {
  if (!value) return "";

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function parseLimit(value: string | null) {
  if (!value) return DEFAULT_ROWS_LIMIT;
  const parsed = Number(value);

  return Number.isFinite(parsed) && parsed > 0
    ? Math.min(Math.floor(parsed), 200)
    : DEFAULT_ROWS_LIMIT;
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
  const rowsRequested = hasRowsRequest(route.query, ["cursor", "startsWith"]);
  const transactionsRequested = route.query.get("transactions") === "1";
  const detailQuery = createKvResourceDetailQuery(scope);
  const detail = detailQuery.data;
  const lookup = activeLookup();

  function applyFilters(event: Event) {
    event.preventDefault();
    const nextLimit = parseLimit(limitDraft());
    navigate(
      rowsHref(scope, {
        limit: nextLimit,
        startsWith: startsWithDraft(),
        transactions: transactionsRequested,
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
      : undefined;

  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          compact={true}
          eyebrow="KV resource"
          title={scope.resource}
          description={`${scope.realm} / ${scope.area}`}
          status={rowsStatus}
        />

        <Show when={detailQuery.loading && !detail}>
          <QueryLoadingState description="Loading KV resource measurements..." />
        </Show>
        <Show when={detailQuery.error}>
          <QueryErrorState
            title="Unable to load KV resource measurements"
            error={detailQuery.error}
            onRetry={() => detailQuery.refresh()}
          />
        </Show>
        <Show when={detail}>
          {(resourceDetail) => (
            <DomainFacts
              id="kv-resource-facts"
              title="Current state"
              items={[
                {
                  label: resourceDetail.estimateComplete
                    ? "Estimated records"
                    : "Estimated records (incomplete)",
                  value: resourceDetail.measurementsAvailable
                    ? formatNumber(resourceDetail.estimatedRecordCount)
                    : "--",
                  title: resourceDetail.measurementsAvailable
                    ? resourceDetail.estimateComplete
                      ? "Current KV inventory estimate."
                      : "KV inventory estimate is incomplete."
                    : "KV inventory measurements are unavailable for this resource.",
                },
                {
                  label: "Estimated storage",
                  value: resourceDetail.measurementsAvailable
                    ? formatBytes(resourceDetail.estimatedStorageBytes)
                    : "--",
                },
                {
                  label: "Active transactions",
                  value: formatNumber(resourceDetail.transactionsActive),
                },
                {
                  label: "Read p95",
                  value: resourceDetail.measurementsAvailable
                    ? `${resourceDetail.readLatencyP95Ms.toFixed(1)} ms`
                    : "--",
                  title:
                    "Percentile from up to 256 retained resource samples; the sample is not time-based.",
                },
                {
                  label: "Write p95",
                  value: resourceDetail.measurementsAvailable
                    ? `${resourceDetail.writeLatencyP95Ms.toFixed(1)} ms`
                    : "--",
                  title:
                    "Percentile from up to 256 retained resource samples; the sample is not time-based.",
                },
              ]}
            />
          )}
        </Show>

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

        <Show when={concreteFamily !== null && lookup}>
          <KvValueLookupResult lookup={lookup!} routeFamily={concreteFamily ?? 0} scope={scope} />
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

        <Show when={concreteFamily !== null && !rowsRequested}>
          <RowsRequestPrompt
            description="Committed rows load only when requested."
            href={rowsHref(scope, { limit, startsWith, transactions: transactionsRequested })}
            label="Load rows"
          />
        </Show>

        <Show when={concreteFamily !== null && rowsRequested}>
          <KvRowsSection
            cursor={cursor}
            cursorTrail={cursorTrail}
            limit={limit}
            scope={scope}
            startsWith={startsWith}
            transactions={transactionsRequested}
          />
        </Show>

        <Show when={concreteFamily !== null && detail && detail.transactionsActive > 0}>
          {transactionsRequested ? (
            <KvTransactionsSection scope={scope} />
          ) : (
            <RowsRequestPrompt
              description="Inspect transaction mode, age, and queued operations."
              href={revealSectionHref(domainResourceHref("kv", scope), route.query, "transactions")}
              label="Inspect active transactions"
            />
          )}
        </Show>
      </Block>
    </DomainPageFrame>
  );
}
