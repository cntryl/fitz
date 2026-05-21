import { state } from "@askrjs/askr";
import { currentRoute } from "@askrjs/askr/router";
import { Button } from "@askrjs/themes/controls";
import { Input, Label } from "@askrjs/ui";
import { AlertTriangleIcon } from "@askrjs/lucide";
import { EmptyState, Spinner } from "@askrjs/themes/feedback";
import ResourceWorkbench from "@/components/shared/resource-workbench";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import SidebarLayout from "@/components/shared/sidebar-layout";
import { createResourceQuery, type DomainId, type ResourceRef } from "@/features/resource/resource-query";
import { formatUnknownError } from "@/shared/errors/format";

const domainLabels: Record<DomainId, string> = {
  kv: "KV",
  lease: "Lease",
  notice: "Notice",
  rpc: "RPC",
  schedule: "Schedule",
  stream: "Stream",
};

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
  const compareRealm = state(compareScope.realm);
  const compareArea = state(compareScope.area);
  const compareResource = state(compareScope.resource);
  const against =
    compareRealm().trim() && compareArea().trim() && compareResource().trim()
      ? {
          area: compareArea().trim(),
          realm: compareRealm().trim(),
          resource: compareResource().trim(),
        }
      : null;
  const query = createResourceQuery(domain, ref, against);
  const data = query.data;
  const sidebar = createDomainSidebar({
    data,
    title: `${domainLabels[domain]} resource`,
    description: `${ref.realm} / ${ref.area} / ${ref.resource}`,
    stats: (current) => current.detailMetrics.slice(0, 6),
    footer: (
      <form class="domain-stack" onSubmit={onCompareSubmit}>
        <div class="auth-field">
          <Label for="compare-realm">Against realm</Label>
          <Input
            id="compare-realm"
            value={compareRealm()}
            onInput={(event: Event) => compareRealm.set((event.target as HTMLInputElement).value)}
          />
        </div>
        <div class="auth-field">
          <Label for="compare-area">Against area</Label>
          <Input
            id="compare-area"
            value={compareArea()}
            onInput={(event: Event) => compareArea.set((event.target as HTMLInputElement).value)}
          />
        </div>
        <div class="auth-field">
          <Label for="compare-resource">Against resource</Label>
          <Input
            id="compare-resource"
            value={compareResource()}
            onInput={(event: Event) => compareResource.set((event.target as HTMLInputElement).value)}
          />
        </div>
        <Button type="submit" class="secondary-action">
          Compare
        </Button>
      </form>
    ),
  });

  function onCompareSubmit(event: Event) {
    event.preventDefault();

    if (typeof window === "undefined") return;

    const nextQuery = new URLSearchParams();
    if (compareRealm().trim()) nextQuery.set("againstRealm", compareRealm().trim());
    if (compareArea().trim()) nextQuery.set("againstArea", compareArea().trim());
    if (compareResource().trim()) nextQuery.set("againstResource", compareResource().trim());

    const search = nextQuery.toString();
    window.location.assign(
      `/${domain}/${ref.realm}/${ref.area}/${ref.resource}${search ? `?${search}` : ""}`,
    );
  }

  return (
    <SidebarLayout
      sidebar={sidebar}
      sidebarPosition="end"
      sidebarWidth="20rem"
      gap="1.5rem"
      collapseBelow="md"
    >
      <section class="domain-page">
        {query.loading ? (
          <EmptyState
            class="domain-state"
            icon={<Spinner label="Loading" />}
            description={`Loading ${domainLabels[domain]} resource...`}
          />
        ) : null}

        {query.error ? (
          <EmptyState
            class="domain-state"
            icon={<AlertTriangleIcon size={18} />}
            description={formatUnknownError(query.error)}
          />
        ) : null}

        {data && !query.loading && !query.error ? <ResourceWorkbench detail={data} /> : null}
      </section>
    </SidebarLayout>
  );
}
