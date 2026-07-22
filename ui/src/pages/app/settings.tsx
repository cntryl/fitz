import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { LogOutIcon, NetworkIcon, UserIcon } from "@askrjs/lucide";
import {
  Badge,
  Alert,
  Block,
  Button,
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Grid,
  Stack,
  Text,
} from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { QueryErrorState, QueryLoadingState } from "@/components/shared/query-state";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { appConfig } from "@/shared/config";
import { formatUnknownError } from "@/shared/errors/format";
import { useOperatorScope } from "@/shared/operator-scope";

function SettingValue({ detail, label, value }: { detail?: string; label: string; value: string }) {
  return (
    <Block gap="xs">
      <Text as="span" tone="muted" size="sm">
        {label}
      </Text>
      <Text as="strong" weight="semibold">
        {value}
      </Text>
      {detail ? (
        <Text tone="muted" size="sm">
          {detail}
        </Text>
      ) : null}
    </Block>
  );
}

export default function SettingsPage() {
  const session = createCurrentSessionQuery();
  const operator = useOperatorScope();
  const openAccess = session.data?.authRequired === false;
  const authenticated = session.data?.authenticated === true;
  const signedIn = authenticated && !openAccess;
  const accountLabel =
    session.data?.username ||
    (openAccess ? "Open access" : authenticated ? "Authenticated session" : "Guest");
  const isInitialLoad = session.loading && !session.data;
  const isInitialError = session.error && !session.data;

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Admin workspace"
          title="Workspace & account"
          description="Review the current operator context, environment, Route Family, and session access."
          status={{
            detail: isInitialLoad
              ? "Loading browser session access."
              : isInitialError
                ? "Browser session access could not be loaded."
                : `Selected Route Family: ${operator.selectedRouteFamily.label}. Environment: ${appConfig.environmentLabel}.`,
            label: isInitialLoad
              ? "Loading"
              : isInitialError
                ? "Unavailable"
                : signedIn
                  ? "Signed in"
                  : openAccess
                    ? "Open access"
                    : "Guest",
            tone: isInitialError ? "danger" : signedIn ? "success" : "info",
          }}
        />

        {isInitialLoad ? (
          <QueryLoadingState description="Loading workspace session access..." />
        ) : null}
        {isInitialError ? (
          <QueryErrorState
            title="Unable to load session access"
            error={session.error}
            onRetry={() => session.refresh()}
          />
        ) : null}
        {session.data && session.error ? (
          <Alert
            variant="warning"
            title="Session refresh failed"
            description={`Showing the last available session context. ${formatUnknownError(session.error)}`}
          />
        ) : null}

        {session.data ? (
          <Block gap="lg">
            <Card id="operator-context" variant="raised">
              <CardHeader>
                <CardTitle titleAs="h2">Operator context</CardTitle>
                <CardDescription>
                  Current browser-local context used by navigation, diagnostics, and admin actions.
                </CardDescription>
                <CardAction>
                  <Badge variant={signedIn ? "success" : "info"}>
                    {signedIn ? "Signed in" : openAccess ? "Open access" : "Guest"}
                  </Badge>
                </CardAction>
              </CardHeader>
              <CardContent>
                <Grid columns={{ base: 1, md: 2 }} gap="md">
                  <SettingValue label={openAccess ? "Access" : "User"} value={accountLabel} />
                  <SettingValue label="Environment" value={appConfig.environmentLabel} />
                  <SettingValue label="Route Family" value={operator.selectedRouteFamily.label} />
                  <SettingValue label="Route Family id" value={operator.selectedRouteFamilyId} />
                </Grid>
              </CardContent>
            </Card>

            <Card id="route-family">
              <CardHeader>
                <CardTitle titleAs="h2">Route Family scope</CardTitle>
                <CardDescription>
                  Fitz keeps Route Family separate from realm and applies it only as the admin
                  routing scope.
                </CardDescription>
                <CardAction>
                  <NetworkIcon size={18} aria-hidden="true" />
                </CardAction>
              </CardHeader>
              <CardContent>
                <Block gap="md">
                  <Block
                    rowFrom="md"
                    align={{ base: "start", md: "center" }}
                    justify="between"
                    gap="md"
                  >
                    <Block gap="xs">
                      <Text weight="medium">{operator.selectedRouteFamily.label}</Text>
                      <Text tone="muted" size="sm">
                        {operator.selectedRouteFamily.description}
                      </Text>
                    </Block>
                    <Badge variant="secondary">{operator.selectedRouteFamilyId}</Badge>
                  </Block>
                  <Block gap="sm">
                    <Text weight="medium">Available scopes</Text>
                    <Grid columns={{ base: 1, md: 2 }} gap="sm">
                      <For each={operator.routeFamilies} by={(family) => family.id}>
                        {(family) => (
                          <Block gap="xs">
                            <Text as="span" weight="semibold" size="sm">
                              {family.label}
                            </Text>
                            <Text tone="muted" size="sm">
                              {family.description}
                            </Text>
                          </Block>
                        )}
                      </For>
                    </Grid>
                  </Block>
                </Block>
              </CardContent>
            </Card>

            <Card id="session-access">
              <CardHeader>
                <CardTitle titleAs="h2">Session access</CardTitle>
                <CardDescription>Browser session state and account actions.</CardDescription>
                <CardAction>
                  <UserIcon size={18} aria-hidden="true" />
                </CardAction>
              </CardHeader>
              <CardContent>
                <Block gap="md">
                  <Block direction="row" align="center" justify="between" gap="md">
                    <Block gap="xs">
                      <Text weight="medium">{accountLabel}</Text>
                      <Text tone="muted" size="sm">
                        {signedIn
                          ? "Authenticated admin session"
                          : openAccess
                            ? "Admin authentication is disabled; no browser account session exists"
                            : "No authenticated admin session"}
                      </Text>
                    </Block>
                    <Badge variant={signedIn ? "success" : "info"}>
                      {signedIn ? "Current" : openAccess ? "Open" : "Guest"}
                    </Badge>
                  </Block>
                  {signedIn ? (
                    <Button asChild variant="outline">
                      <Link href="/logout">
                        <LogOutIcon size={16} aria-hidden="true" />
                        Sign out
                      </Link>
                    </Button>
                  ) : null}
                </Block>
              </CardContent>
            </Card>
          </Block>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
