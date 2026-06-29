import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { ArrowRightIcon, NetworkIcon } from "@askrjs/lucide";
import {
  Badge,
  Block,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Grid,
  Main,
  PageHeader,
  Stack,
} from "@askrjs/themes/components";
import { pathWithRouteFamily } from "@/shared/navigation/domains";
import { useOperatorContext, type RouteFamilyOption } from "@/shared/operator-context";

function selectorTarget(routeFamilyId: string) {
  if (typeof window === "undefined") {
    return pathWithRouteFamily("/", routeFamilyId);
  }

  return `${pathWithRouteFamily(window.location.pathname, routeFamilyId)}${window.location.search}${window.location.hash}`;
}

function RouteFamilyGrid({
  routeFamilies,
  setRouteFamily,
}: {
  routeFamilies: RouteFamilyOption[];
  setRouteFamily: (routeFamilyId: string) => void;
}) {
  return (
    <Grid columns={{ base: 1, md: 2, xl: 3 }} gap="md">
      <For each={routeFamilies} by={(family) => family.id}>
        {(family) => (
          <Card>
            <CardHeader>
              <CardTitle>
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
              <Stack gap="3">
                <Badge variant="secondary">{family.id}</Badge>
                <Button asChild variant="outline">
                  <Link href={selectorTarget(family.id)} onClick={() => setRouteFamily(family.id)}>
                    Open workspace
                    <ArrowRightIcon size={16} aria-hidden="true" />
                  </Link>
                </Button>
              </Stack>
            </CardContent>
          </Card>
        )}
      </For>
    </Grid>
  );
}

export default function RouteFamilySelectorPage() {
  const operator = useOperatorContext();

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

        <RouteFamilyGrid
          routeFamilies={operator.routeFamilies}
          setRouteFamily={operator.setRouteFamily}
        />
      </Stack>
    </Main>
  );
}
