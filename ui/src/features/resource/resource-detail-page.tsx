import { state } from "@askrjs/askr";
import { Show } from "@askrjs/askr/control";
import { currentRoute, navigate } from "@askrjs/askr/router";
import { Button } from "@askrjs/themes/controls";
import { Stack } from "@askrjs/themes/layouts";
import { Input, Label } from "@askrjs/ui";
import DomainHeader from "@/components/shared/domain-header";
import ResourceWorkbench, { describeResourceDetail } from "@/components/shared/resource-workbench";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { QueryErrorState, QueryLoadingState } from "@/components/shared/query-state";
import {
  createResourceQuery,
  type DomainId,
  type ResourceRef,
} from "@/features/resource/resource-query";

const domainLabels: Record<DomainId, string> = {
  kv: "KV",
  lease: "Lease",
  notice: "Notice",
  rpc: "RPC",
  schedule: "Schedule",
  stream: "Stream",
};

function trimmedOrNull(value: string) {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function currentCompareScope() {
  if (typeof window === "undefined") {
    return { area: "", realm: "", resource: "" };
  }

  const query = currentRoute().query;
  return {
    area: query.get("againstArea") ?? "",
    realm: query.get("againstRealm") ?? "",
    resource: query.get("againstResource") ?? "",
  };
}

function parseDomain(value: string | undefined): DomainId {
  if (
    value === "kv" ||
    value === "stream" ||
    value === "lease" ||
    value === "schedule" ||
    value === "notice" ||
    value === "rpc"
  ) {
    return value;
  }

  return "kv";
}

function currentDomain() {
  if (typeof window !== "undefined") {
    return parseDomain(window.location.pathname.split("/").filter(Boolean)[0]);
  }

  return "kv";
}

export default function ResourceDetailPage() {
  const route = currentRoute();
  const domain = currentDomain();
  const ref: ResourceRef = {
    area: route.params.area,
    realm: route.params.realm,
    resource: route.params.resource,
  };
  const compareScope = currentCompareScope();
  const [compareRealm, setCompareRealm] = state(compareScope.realm);
  const [compareArea, setCompareArea] = state(compareScope.area);
  const [compareResource, setCompareResource] = state(compareScope.resource);
  const compareRealmValue = compareRealm();
  const compareAreaValue = compareArea();
  const compareResourceValue = compareResource();
  const compareRealmTrimmed = trimmedOrNull(compareRealmValue);
  const compareAreaTrimmed = trimmedOrNull(compareAreaValue);
  const compareResourceTrimmed = trimmedOrNull(compareResourceValue);
  const against =
    compareRealmTrimmed && compareAreaTrimmed && compareResourceTrimmed
      ? {
          area: compareAreaTrimmed,
          realm: compareRealmTrimmed,
          resource: compareResourceTrimmed,
        }
      : null;
  const query = createResourceQuery(domain, ref, against);
  const data = query.data;
  const resourceSummary = data ? describeResourceDetail(data) : null;
  const headerStatus = {
    detail:
      resourceSummary?.detail ?? "Inspect the current snapshot, timeline, and related records.",
    label: query.refreshing
      ? "Refreshing"
      : query.stale
        ? "Stale"
        : (resourceSummary?.label ?? (data ? "Live" : "Loading")),
    tone: query.refreshing
      ? "info"
      : query.stale
        ? "warning"
        : (resourceSummary?.tone ?? (data ? "success" : "info")),
  } as const;
  const sidebar = createDomainSidebar({
    data,
    title: `${domainLabels[domain]} resource`,
    description: `${ref.realm} / ${ref.area} / ${ref.resource}`,
    stats: (current) => current.detailMetrics.slice(0, 6),
    footer: (
      <Stack gap="3">
        <form onSubmit={onCompareSubmit}>
          <div class="auth-field">
            <Label for="compare-realm">Target realm</Label>
            <Input
              id="compare-realm"
              value={compareRealmValue}
              onInput={(event: Event) => setCompareRealm((event.target as HTMLInputElement).value)}
            />
          </div>
          <div class="auth-field">
            <Label for="compare-area">Target area</Label>
            <Input
              id="compare-area"
              value={compareAreaValue}
              onInput={(event: Event) => setCompareArea((event.target as HTMLInputElement).value)}
            />
          </div>
          <div class="auth-field">
            <Label for="compare-resource">Target resource</Label>
            <Input
              id="compare-resource"
              value={compareResourceValue}
              onInput={(event: Event) =>
                setCompareResource((event.target as HTMLInputElement).value)
              }
            />
          </div>
          <Button type="submit">Compare scope</Button>
        </form>
        <p class="domain-muted">
          All three fields are required to compare the current snapshot. Leave them blank if you
          only need the live resource view.
        </p>
      </Stack>
    ),
  });

  function onCompareSubmit(event: Event) {
    event.preventDefault();

    if (typeof window === "undefined") return;

    const nextQuery = new URLSearchParams();
    const nextRealm = trimmedOrNull(compareRealm());
    const nextArea = trimmedOrNull(compareArea());
    const nextResource = trimmedOrNull(compareResource());

    if (nextRealm) nextQuery.set("againstRealm", nextRealm);
    if (nextArea) nextQuery.set("againstArea", nextArea);
    if (nextResource) nextQuery.set("againstResource", nextResource);

    const search = nextQuery.toString();
    navigate(`/${domain}/${ref.realm}/${ref.area}/${ref.resource}${search ? `?${search}` : ""}`);
  }

  return (
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          eyebrow={`${domainLabels[domain]} detail`}
          title={`${domainLabels[domain]} resource inspection`}
          description={`${ref.realm} / ${ref.area} / ${ref.resource}`}
          primaryAction={{
            label: "Refresh resource",
            onPress: () => query.refresh(),
          }}
          status={headerStatus}
        />

        <Show when={query.loading && !data}>
          <QueryLoadingState description={`Loading ${domainLabels[domain]} resource...`} />
        </Show>

        <Show when={query.error && !data}>
          <QueryErrorState error={query.error} onRetry={() => query.refresh()} />
        </Show>

        {data ? <ResourceWorkbench detail={data} /> : null}
      </Stack>
    </DomainPageFrame>
  );
}
