import { state } from "@askrjs/askr";
import { Show } from "@askrjs/askr/control";
import { currentRoute, navigate } from "@askrjs/askr/router";
import { Button } from "@askrjs/themes/controls";
import { Flex, Stack } from "@askrjs/themes/layouts";
import { Input, Label } from "@askrjs/ui";
import DomainHeader from "@/components/shared/domain-header";
import ResourceWorkbench, { describeResourceDetail } from "@/components/shared/resource-workbench";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import {
  createResourceQuery,
  type DomainId,
  type ResourceRef,
} from "@/features/resource/resource-query";
import {
  domainResourceHref,
  isGenericResourceDomainSegment,
} from "@/shared/navigation/domains";

type DomainPresentation = {
  description: string;
  eyebrow: string;
  title: string;
};

const domainMeta: Record<DomainId, DomainPresentation> = {
  kv: {
    description:
      "Inspect live KV transaction activity and broker-local contention signals for this scope.",
    eyebrow: "KV state",
    title: "KV resource inspection",
  },
  lease: {
    description:
      "Inspect ephemeral lease ownership, waiter pressure, and coordination signals for this scope.",
    eyebrow: "Lease coordination",
    title: "Lease resource inspection",
  },
  notice: {
    description: "Inspect live Notice session fanout and subscription state for this scope.",
    eyebrow: "Notice fanout",
    title: "Notice resource inspection",
  },
  rpc: {
    description: "Inspect live request/response workers, pending calls, and active routing state.",
    eyebrow: "RPC request/response",
    title: "RPC resource inspection",
  },
  schedule: {
    description: "Inspect durable timing intent and live handoff state for this scope.",
    eyebrow: "Timing intent",
    title: "Schedule resource inspection",
  },
  stream: {
    description:
      "Inspect durable stream history indicators and live append activity for this scope.",
    eyebrow: "Stream durable replay",
    title: "Stream resource inspection",
  },
};

function trimmedOrNull(value: string) {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function currentDomain() {
  if (typeof window !== "undefined") {
    const route = currentRoute();
    const path = route.path.split("/").filter(Boolean)[0];
    return isGenericResourceDomainSegment(path) ? path : "kv";
  }

  return "kv";
}

function formatScopeLabel(ref: ResourceRef) {
  return `${ref.realm} / ${ref.area} / ${ref.resource}`;
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

export default function ResourceDetailPage() {
  const route = currentRoute();
  const domain = currentDomain();
  const domainConfig = domainMeta[domain];
  const ref: ResourceRef = {
    area: route.params.area,
    realm: route.params.realm,
    resource: route.params.resource,
  };
  const scopeLabel = formatScopeLabel(ref);
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
  const compareReady = Boolean(compareRealmTrimmed && compareAreaTrimmed && compareResourceTrimmed);
  const compareHasInput =
    compareRealmTrimmed != null || compareAreaTrimmed != null || compareResourceTrimmed != null;
  const compareHint =
    compareHasInput && !compareReady
      ? "Provide realm, area, and resource to compare against another scope."
      : null;

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
    detail: query.refreshing
      ? "Refreshing resource context and timeline."
      : (resourceSummary?.detail ?? "Inspecting domain resource state."),
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
  };

  const sidebar = createDomainSidebar({
    data,
    title: `${domainConfig.title} scope`,
    description: scopeLabel,
    stats: (current) => [
      { label: "Realm", value: current.ref.realm },
      { label: "Area", value: current.ref.area },
      { label: "Resource", value: current.ref.resource },
      ...current.detailMetrics.slice(0, 3).map((metric) => ({
        label: metric.label,
        value: metric.value,
        note: metric.caption,
      })),
    ],
    footer: (
      <Stack gap="3">
        <form class="resource-compare-form" onSubmit={onCompareSubmit}>
          <div class="form-grid">
            <div class="auth-field">
              <Label for="compare-realm">Target realm</Label>
              <Input
                id="compare-realm"
                value={compareRealmValue}
                onInput={(event: Event) =>
                  setCompareRealm((event.target as HTMLInputElement).value)
                }
                placeholder="default"
              />
            </div>
            <div class="auth-field">
              <Label for="compare-area">Target area</Label>
              <Input
                id="compare-area"
                value={compareAreaValue}
                onInput={(event: Event) => setCompareArea((event.target as HTMLInputElement).value)}
                placeholder="ops"
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
                placeholder="primary"
              />
            </div>
          </div>

          {compareHint ? <p class="domain-muted">{compareHint}</p> : null}

          <Flex gap="2" wrap="wrap">
            <Button type="submit" disabled={!compareReady}>
              Compare scope
            </Button>
            <Button
              type="button"
              variant="outline"
              onPress={() => {
                setCompareRealm("");
                setCompareArea("");
                setCompareResource("");
                navigate(domainResourceHref(domain, ref));
              }}
            >
              Clear comparison
            </Button>
          </Flex>
        </form>
      </Stack>
    ),
  });

  return (
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          eyebrow={domainConfig.eyebrow}
          title={domainConfig.title}
          description={`${domainConfig.description} Scope: ${scopeLabel}.`}
          primaryAction={{
            label: "Refresh resource",
            onPress: () => query.refresh(),
          }}
          status={headerStatus}
        />

        <Show when={query.loading && !data}>
          <QueryLoadingState description={`Loading ${domain.toUpperCase()} resource...`} />
        </Show>

        <Show when={query.error && !data}>
          <QueryErrorState error={query.error} onRetry={() => query.refresh()} />
        </Show>

        {query.refreshing ? (
          <QueryRefreshingState description={`Updating ${domain.toUpperCase()} resource...`} />
        ) : null}

        <Show when={data !== null && data !== undefined}>
          <ResourceWorkbench detail={data!} />
        </Show>
      </Stack>
    </DomainPageFrame>
  );

  function onCompareSubmit(event: Event) {
    event.preventDefault();

    if (typeof window === "undefined" || !compareReady) {
      return;
    }

    const nextQuery = new URLSearchParams();

    if (compareRealmTrimmed) {
      nextQuery.set("againstRealm", compareRealmTrimmed);
    }

    if (compareAreaTrimmed) {
      nextQuery.set("againstArea", compareAreaTrimmed);
    }

    if (compareResourceTrimmed) {
      nextQuery.set("againstResource", compareResourceTrimmed);
    }

    navigate(`${domainResourceHref(domain, ref)}?${nextQuery.toString()}`);
  }
}
