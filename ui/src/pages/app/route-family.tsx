import { For } from "@askrjs/askr/control";
import { task } from "@askrjs/askr/resources";
import { Link } from "@askrjs/askr/router";
import { NetworkIcon } from "@askrjs/lucide";
import { Button, Main, PageHeader, Block } from "@askrjs/themes/components";
import { pathWithRouteFamily } from "@/shared/navigation/domains";
import { manageRoutePageContext } from "@/components/shared/domain-page-frame";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
} from "@/components/shared/query-state";
import { useOperatorScope, type RouteFamilyOption } from "@/shared/operator-scope";
import { routeFamilyIconSlot } from "@/shared/route-family-appearance";

function selectorTarget(routeFamilyId: string) {
  if (typeof window === "undefined") {
    return pathWithRouteFamily("/", routeFamilyId);
  }

  return `${pathWithRouteFamily(window.location.pathname, routeFamilyId)}${window.location.search}${window.location.hash}`;
}

function RouteFamilyList({ routeFamilies }: { routeFamilies: RouteFamilyOption[] }) {
  return (
    <ul class="route-family-list">
      <For each={routeFamilies} by={(family) => family.id}>
        {(family) => (
          <li>
            <Link
              class="route-family-list-link"
              href={selectorTarget(family.id)}
              aria-label={`Open workspace for Route Family ${family.id}`}
            >
              <NetworkIcon
                class="route-family-icon"
                data-route-family-identity={routeFamilyIconSlot(family.id)}
                size={18}
                aria-hidden="true"
              />
              <span>{family.label}</span>
            </Link>
          </li>
        )}
      </For>
    </ul>
  );
}

export function RouteFamilyNotFoundPage() {
  task(() => manageRoutePageContext("Route Family not found"));

  return (
    <Main
      id="main-content"
      class="domain-page-frame route-transition-surface"
      direction="column"
      paddingY="xl"
      tabIndex={-1}
    >
      <Block direction="column" gap="md">
        <PageHeader
          title="Route Family not found"
          description="404 · This Route Family is unavailable to the current session."
        />
        <Button asChild variant="outline">
          <Link href="/admin">Back to Route Families</Link>
        </Button>
      </Block>
    </Main>
  );
}

export default function RouteFamilySelectorPage() {
  const operator = useOperatorScope();

  task(() => manageRoutePageContext("Select Route Family"));

  return (
    <Main
      id="main-content"
      class="domain-page-frame route-transition-surface"
      direction="column"
      paddingY="xl"
      tabIndex={-1}
    >
      <Block direction="column" gap="sm">
        <PageHeader
          title="Select Route Family"
          description="Choose a concrete Route Family before opening the Fitz operator workspace."
        />

        {operator.routeFamilyState === "loading" ? (
          <QueryLoadingState description="Loading available Route Families..." />
        ) : null}
        {operator.routeFamilyState === "error" ? (
          <QueryErrorState
            title="Unable to load Route Families"
            error={operator.routeFamilyError}
            onRetry={operator.retryRouteFamilies}
          />
        ) : null}
        {operator.routeFamilyState === "empty" ? (
          <QueryEmptyState
            title="No Route Families available"
            description="This session does not currently expose a concrete Route Family."
          />
        ) : null}
        {operator.routeFamilyState === "ready" ? (
          <RouteFamilyList routeFamilies={operator.routeFamilies} />
        ) : null}
      </Block>
    </Main>
  );
}
