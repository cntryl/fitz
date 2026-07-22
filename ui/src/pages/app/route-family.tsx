import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { task } from "@askrjs/askr/resources";
import { Link, navigate } from "@askrjs/askr/router";
import { ArrowRightIcon, NetworkIcon } from "@askrjs/lucide";
import { Input, Label } from "@askrjs/ui";
import {
  Block,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Field,
  Grid,
  Inline,
  Main,
  PageHeader,
  Stack,
} from "@askrjs/themes/components";
import { pathWithRouteFamily } from "@/shared/navigation/domains";
import { manageRoutePageContext } from "@/components/shared/domain-page-frame";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
} from "@/components/shared/query-state";
import { useOperatorScope, type RouteFamilyOption } from "@/shared/operator-scope";

function selectorTarget(routeFamilyId: string) {
  if (typeof window === "undefined") {
    return pathWithRouteFamily("/", routeFamilyId);
  }

  return `${pathWithRouteFamily(window.location.pathname, routeFamilyId)}${window.location.search}${window.location.hash}`;
}

function RouteFamilyGrid({ routeFamilies }: { routeFamilies: RouteFamilyOption[] }) {
  return (
    <Grid columns={{ base: 1, md: 2, xl: 3 }} gap="md">
      <For each={routeFamilies} by={(family) => family.id}>
        {(family) => (
          <Card>
            <CardHeader>
              <CardTitle titleAs="h2">
                <Block as="span" direction="row" align="center" gap="xs">
                  <>
                    <NetworkIcon size={16} aria-hidden="true" />
                    <span>{family.label}</span>
                  </>
                </Block>
              </CardTitle>
              <CardDescription>{family.description}</CardDescription>
            </CardHeader>
            <CardContent>
              <Button asChild variant="outline">
                <Link
                  href={selectorTarget(family.id)}
                  aria-label={`Open workspace for Route family ${family.id}`}
                >
                  Open
                  <ArrowRightIcon size={16} aria-hidden="true" />
                </Link>
              </Button>
            </CardContent>
          </Card>
        )}
      </For>
    </Grid>
  );
}

function WildcardRouteFamilyForm() {
  const [routeFamilyId, setRouteFamilyId] = state("");

  function openRouteFamily(event: Event) {
    event.preventDefault();
    const value = routeFamilyId().trim();

    if (/^\d+$/.test(value)) {
      navigate(selectorTarget(value));
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle titleAs="h2">Open another Route Family</CardTitle>
        <CardDescription>
          This session has wildcard access. Enter the numeric Route Family identifier you want to
          operate.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <form onSubmit={openRouteFamily}>
          <Inline align="end" gap="3" wrap="wrap">
            <Field>
              <Label for="wildcard-route-family">Route Family</Label>
              <Input
                id="wildcard-route-family"
                name="routeFamily"
                inputmode="numeric"
                pattern="[0-9]+"
                required
                value={routeFamilyId()}
                onInput={(event: Event) =>
                  setRouteFamilyId((event.target as HTMLInputElement).value)
                }
                placeholder="42"
              />
            </Field>
            <Button type="submit">Open</Button>
          </Inline>
        </form>
      </CardContent>
    </Card>
  );
}

export default function RouteFamilySelectorPage() {
  const operator = useOperatorScope();

  task(() => manageRoutePageContext("Select Route Family"));

  return (
    <Main
      id="main-content"
      class="domain-page-frame route-transition-surface"
      paddingY="xl"
      tabIndex={-1}
    >
      <Stack gap="3">
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
          <Stack gap="3">
            {operator.routeFamilies.length > 0 ? (
              <RouteFamilyGrid routeFamilies={operator.routeFamilies} />
            ) : null}
            {operator.routeFamiliesWildcard ? <WildcardRouteFamilyForm /> : null}
          </Stack>
        ) : null}
      </Stack>
    </Main>
  );
}
