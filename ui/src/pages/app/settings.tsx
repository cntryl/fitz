import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import {
  GaugeIcon,
  LogOutIcon,
  MonitorCogIcon,
  NetworkIcon,
  SettingsIcon,
  ShieldIcon,
  UserIcon,
} from "@askrjs/lucide";
import {
  Badge,
  Block,
  Button,
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Grid,
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  Text,
} from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { appConfig } from "@/shared/config";
import { adminChildHref } from "@/shared/navigation/domains";
import { useOperatorContext } from "@/shared/operator-context";

const settingsNavigation: Array<{
  group: string;
  items: Array<{
    href: string;
    icon: typeof UserIcon;
    label: string;
  }>;
}> = [
  {
    group: "Account",
    items: [
      {
        href: "#operator-context",
        icon: UserIcon,
        label: "Operator",
      },
      {
        href: "#route-family",
        icon: NetworkIcon,
        label: "Route Family",
      },
    ],
  },
  {
    group: "Admin",
    items: [
      {
        href: "#admin-tools",
        icon: MonitorCogIcon,
        label: "Tools",
      },
      {
        href: "#session-access",
        icon: ShieldIcon,
        label: "Access",
      },
    ],
  },
];

function SettingsSidebar() {
  return (
    <Sidebar width="full" minHeight="auto" padding="0" borderRight={false} shrink={false}>
      <SidebarHeader>
        <Text as="strong" weight="semibold">
          Settings
        </Text>
      </SidebarHeader>
      <SidebarContent>
        <For each={settingsNavigation} by={(group) => group.group}>
          {(group) => (
            <SidebarGroup>
              <SidebarGroupLabel>{group.group}</SidebarGroupLabel>
              <SidebarGroupContent>
                <SidebarMenu>
                  <For each={group.items} by={(item) => item.href}>
                    {(item) => (
                      <SidebarMenuItem>
                        <SidebarMenuButton asChild>
                          <a href={item.href}>
                            <item.icon size={16} aria-hidden="true" />
                            <Text as="span" size="sm" weight="medium">
                              {item.label}
                            </Text>
                          </a>
                        </SidebarMenuButton>
                      </SidebarMenuItem>
                    )}
                  </For>
                </SidebarMenu>
              </SidebarGroupContent>
            </SidebarGroup>
          )}
        </For>
      </SidebarContent>
    </Sidebar>
  );
}

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
  const operator = useOperatorContext();
  const accountLabel = session.data?.username ?? "admin";
  const authenticated = session.data?.authenticated === true;
  const sessionsHref = adminChildHref("sessions", operator.selectedRouteFamilyId);
  const diagnosticsHref = adminChildHref("diagnostics", operator.selectedRouteFamilyId);
  const metricsHref = adminChildHref("metrics", operator.selectedRouteFamilyId);

  return (
    <DomainPageFrame>
      <DomainHeader
        eyebrow="Admin workspace"
        title="Settings"
        description="Operator context, environment, and account-level admin tools."
        status={{
          detail: `Selected Route Family: ${operator.selectedRouteFamily.label}. Environment: ${appConfig.environmentLabel}.`,
          label: authenticated ? "Signed in" : "Open",
          tone: authenticated ? "success" : "info",
        }}
      />

      <Grid columns={{ base: 1, lg: "14rem minmax(0, 1fr)" }} gap="lg">
        <SettingsSidebar />

        <Block gap="lg">
          <Card id="operator-context" variant="raised">
            <CardHeader>
              <CardTitle>Operator context</CardTitle>
              <CardDescription>
                Current browser-local context used by navigation, diagnostics, and admin actions.
              </CardDescription>
              <CardAction>
                <Badge variant={authenticated ? "success" : "info"}>
                  {authenticated ? "Signed in" : "Open"}
                </Badge>
              </CardAction>
            </CardHeader>
            <CardContent>
              <Grid columns={{ base: 1, md: 2 }} gap="md">
                <SettingValue label="User" value={accountLabel} />
                <SettingValue label="Environment" value={appConfig.environmentLabel} />
                <SettingValue label="Route Family" value={operator.selectedRouteFamily.label} />
                <SettingValue label="Route Family id" value={operator.selectedRouteFamilyId} />
              </Grid>
            </CardContent>
          </Card>

          <Card id="route-family">
            <CardHeader>
              <CardTitle>Route Family scope</CardTitle>
              <CardDescription>
                Fitz keeps Route Family separate from realm and applies it only as the admin routing
                scope.
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

          <Grid id="admin-tools" columns={{ base: 1, md: 2 }} gap="lg">
            <Card>
              <CardHeader>
                <CardTitle>Admin tools</CardTitle>
                <CardDescription>
                  Secondary operator surfaces outside domain navigation.
                </CardDescription>
                <CardAction>
                  <SettingsIcon size={18} aria-hidden="true" />
                </CardAction>
              </CardHeader>
              <CardContent>
                <Block gap="sm">
                  <Button asChild variant="outline">
                    <Link href={sessionsHref}>
                      <ShieldIcon size={16} aria-hidden="true" />
                      Active sessions
                    </Link>
                  </Button>
                  <Button asChild variant="outline">
                    <Link href={diagnosticsHref}>
                      <GaugeIcon size={16} aria-hidden="true" />
                      Diagnostics
                    </Link>
                  </Button>
                  <Button asChild variant="outline">
                    <Link href={metricsHref}>
                      <MonitorCogIcon size={16} aria-hidden="true" />
                      Metrics
                    </Link>
                  </Button>
                </Block>
              </CardContent>
            </Card>

            <Card id="session-access">
              <CardHeader>
                <CardTitle>Session access</CardTitle>
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
                        {authenticated
                          ? "Authenticated admin session"
                          : "Admin endpoint is currently open"}
                      </Text>
                    </Block>
                    <Badge variant={authenticated ? "success" : "info"}>
                      {authenticated ? "Current" : "Open"}
                    </Badge>
                  </Block>
                  <Button asChild variant="outline">
                    <Link href="/logout">
                      <LogOutIcon size={16} aria-hidden="true" />
                      Sign out
                    </Link>
                  </Button>
                </Block>
              </CardContent>
            </Card>
          </Grid>
        </Block>
      </Grid>
    </DomainPageFrame>
  );
}
