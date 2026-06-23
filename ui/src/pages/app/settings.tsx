import { Link } from "@askrjs/askr/router";
import { Stack } from "@askrjs/themes/layouts";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@askrjs/themes/surfaces";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { appConfig } from "@/shared/config";
import { useOperatorContext } from "@/shared/operator-context";

export default function SettingsPage() {
  const session = createCurrentSessionQuery();
  const operator = useOperatorContext();

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Admin workspace"
          title="Settings"
          description="Operator context, environment, and account-level admin tools."
          status={{
            detail: `Selected Route Family: ${operator.selectedRouteFamily.label}. Environment: ${appConfig.environmentLabel}.`,
            label: session.data?.authenticated ? "Signed in" : "Open",
            tone: session.data?.authenticated ? "success" : "info",
          }}
        />

        <DomainMetricTable
          title="Operator context"
          description="Current UI-local context used by navigation, search, diagnostics, and actions."
          metrics={[
            { label: "Route Family", value: operator.selectedRouteFamily.label },
            { label: "Route Family id", value: operator.selectedRouteFamilyId },
            { label: "Environment", value: appConfig.environmentLabel },
            { label: "User", value: session.data?.username ?? "admin" },
          ]}
        />

        <Card padding="sm" variant="default">
          <CardHeader>
            <CardTitle>Admin tools</CardTitle>
            <CardDescription>
              Secondary operator surfaces retained outside primary domain navigation.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div class="diagnostics-link-grid">
              <Link class="text-link" href="/sessions">
                Active sessions
              </Link>
              <Link class="text-link" href="/diagnostics">
                Diagnostics
              </Link>
              <Link class="text-link" href="/logout">
                Sign out
              </Link>
            </div>
          </CardContent>
        </Card>
      </Stack>
    </DomainPageFrame>
  );
}
