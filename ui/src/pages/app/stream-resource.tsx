import { state } from "@askrjs/askr";
import { Show } from "@askrjs/askr/control";
import { currentRoute, navigate } from "@askrjs/askr/router";
import { Button, Block } from "@askrjs/themes/components";
import { Form, Input, Label } from "@askrjs/ui";
import RowsRequestPrompt from "@/components/shared/rows-request-prompt";
import { hasRowsRequest } from "@/shared/navigation/rows-request";
import DomainDataSection from "@/components/shared/domain-data-section";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import DomainSummaryStrip from "@/components/shared/domain-summary-strip";
import { queryHeaderStatus } from "@/components/shared/query-header-status";
import { createStreamResourceQuery } from "@/features/stream/stream-query";
import StreamRecordsSection, {
  DEFAULT_RECORDS_LIMIT,
  recordsHref,
} from "@/features/stream/stream-records-section";

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

export default function StreamResourcePage() {
  const route = currentRoute();
  const scope = {
    area: decodeParam(route.params.area),
    realm: decodeParam(route.params.realm),
    resource: decodeParam(route.params.resource),
  };
  const fromOffset = parsePositiveInt(route.query.get("fromOffset"), 0);
  const limit = Math.min(parsePositiveInt(route.query.get("limit"), DEFAULT_RECORDS_LIMIT), 200);
  const discriminator = route.query.get("discriminator") ?? "";
  const [fromOffsetDraft, setFromOffsetDraft] = state(fromOffset.toString());
  const [limitDraft, setLimitDraft] = state(limit.toString());
  const [discriminatorDraft, setDiscriminatorDraft] = state(discriminator);
  const recordsRequested = hasRowsRequest(route.query, ["fromOffset", "discriminator"]);
  const detailQuery = createStreamResourceQuery(scope);
  const detail = detailQuery.data;
  const headerStatus = queryHeaderStatus(detailQuery, {
    loading: "Loading stream resource.",
    ready: "Stream resource metadata is current.",
    unavailable: "Stream resource metadata is unavailable.",
  });

  function applyFilters(event: Event) {
    event.preventDefault();
    navigate(
      recordsHref(scope, {
        discriminator: discriminatorDraft(),
        fromOffset: parsePositiveInt(fromOffsetDraft(), 0),
        limit: Math.min(parsePositiveInt(limitDraft(), DEFAULT_RECORDS_LIMIT), 200),
      }),
    );
  }

  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          eyebrow="Stream resource"
          title={scope.resource}
          description={`${scope.realm} / ${scope.area}`}
          primaryAction={{
            busy: detailQuery.refreshing,
            disabled: detailQuery.refreshing,
            label: "Refresh stream",
            onPress: () => void detailQuery.refresh(),
          }}
          status={headerStatus}
        />
        {detail ? (
          <DomainSummaryStrip
            id="stream-committed-metadata"
            class="domain-detail-summary"
            title="Committed metadata"
            description="Durable committed metadata. Append sessions are live and separate from replay history."
            items={[
              {
                label: "Latest committed offset",
                value: detail.offset,
                caption: "Resource high-water metadata, not the read cursor",
              },
              {
                label: "Watermark",
                value: detail.watermark,
                caption: "Durable committed metadata",
              },
              {
                label: "Size bytes",
                value: detail.size_bytes,
                caption: "Durable committed metadata",
              },
              {
                label: "Append sessions",
                value: detail.sessions_active,
                caption: "Live append sessions",
              },
              {
                label: "Active subscriptions",
                value: detail.subscriptions_active,
                caption: "Live subscriptions; resets on disconnect cleanup or broker restart",
              },
            ]}
          />
        ) : null}
        <DomainDataSection
          id="stream-record-filters"
          title="Record filters"
          description="Read committed records by from offset, optional discriminator, and limit."
        >
          <Form onSubmit={applyFilters}>
            <Block
              direction={{ base: "column", sm: "row" }}
              align={{ base: "stretch", sm: "end" }}
              gap="sm"
              wrap={true}
            >
              <Block direction="column" gap="xs" width={{ base: "full", sm: "auto" }}>
                <Label for="stream-from-offset">From offset</Label>
                <Input
                  id="stream-from-offset"
                  {...({ min: 0 } as Record<string, unknown>)}
                  type="number"
                  value={fromOffsetDraft()}
                  onInput={(event: Event) =>
                    setFromOffsetDraft((event.target as HTMLInputElement).value)
                  }
                />
              </Block>
              <Block direction="column" gap="xs" width={{ base: "full", sm: "auto" }}>
                <Label for="stream-discriminator">Discriminator</Label>
                <Input
                  id="stream-discriminator"
                  value={discriminatorDraft()}
                  onInput={(event: Event) =>
                    setDiscriminatorDraft((event.target as HTMLInputElement).value)
                  }
                />
              </Block>
              <Block direction="column" gap="xs" width={{ base: "full", sm: "auto" }}>
                <Label for="stream-limit">Limit</Label>
                <Input
                  id="stream-limit"
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
        </DomainDataSection>
        <Show when={!recordsRequested}>
          <RowsRequestPrompt
            description="Committed records load only when requested."
            href={recordsHref(scope, { discriminator, fromOffset, limit })}
            label="Load records"
          />
        </Show>
        <Show when={recordsRequested}>
          <StreamRecordsSection
            discriminator={discriminator}
            fromOffset={fromOffset}
            limit={limit}
            scope={scope}
          />
        </Show>
      </Block>
    </DomainPageFrame>
  );
}
