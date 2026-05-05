import { currentRoute, Link } from "@askrjs/askr/router";
import { Button } from "@askrjs/ui";
import { Badge, Card, CardContent, CardHeader, CardTitle } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainState from "@/components/shared/domain-state";
import DomainSidebar from "@/components/shared/domain-sidebar";
import PageShell from "@/components/shared/page-shell";
import QueueDeadLetterTable from "@/components/shared/queue-dead-letter-table";
import QueueInflightTable from "@/components/shared/queue-inflight-table";
import { createQueueResourceQuery } from "@/features/queue/queue-resource-query";

function humanizeSeconds(seconds: number) {
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h`;
}

export default function QueueResourcePage() {
  const { realm, area, resource } = currentRoute().params;
  const resourceQuery = createQueueResourceQuery({ realm, area, resource });
  const data = resourceQuery.data;

  const sidebar = data ? (
    <DomainSidebar
      title="Resource snapshot"
      description="Current queue actor state for this resource."
      stats={[
        { label: "Realm", value: data.detail.realm },
        { label: "Area", value: data.detail.area },
        { label: "Resource", value: data.detail.resource },
        { label: "Ready", value: data.detail.messagesReady },
        { label: "Inflight", value: data.detail.messagesInflight },
        { label: "Dead-lettered", value: data.detail.messagesDeadLettered },
        { label: "Delayed", value: data.detail.messagesDelayed },
        {
          label: "Oldest age",
          value: humanizeSeconds(data.detail.oldestMessageAgeSeconds),
          note: "Point-in-time broker snapshot",
        },
      ]}
      footer={
        <div class="admin-sidebar-actions">
          <Link href="/queue" class="admin-sidebar-link">
            Back to Queue
          </Link>
          <Button class="secondary-action" onPress={() => resourceQuery.refresh()}>
            Refresh
          </Button>
        </div>
      }
    />
  ) : undefined;

  return (
    <PageShell sidebar={sidebar}>
      <section class="domain-page">
        <DomainHeader
          domain="Queue"
          title="Resource drill-down"
          description={`${realm} / ${area} / ${resource}`}
          onRefresh={() => resourceQuery.refresh()}
        />

        {resourceQuery.loading ? (
          <DomainState kind="loading" message="Loading queue resource..." />
        ) : null}

        {resourceQuery.error ? (
          <DomainState
            kind="error"
            message="Queue resource could not be loaded."
            error={resourceQuery.error}
          />
        ) : null}

        {data && !resourceQuery.loading && !resourceQuery.error ? (
          <>
            <Card class="domain-resource-card" variant="raised">
              <CardHeader>
                <Badge>Queue Resource</Badge>
                <CardTitle>{data.detail.resource}</CardTitle>
              </CardHeader>
              <CardContent>
                <p>
                  Live in-memory view of the broker actor for this queue resource. Message counts
                  are point-in-time and reflect the current broker process.
                </p>
              </CardContent>
            </Card>

            <DomainMetricTable
              title="Resource metrics"
              metrics={[
                { label: "Total messages", value: data.detail.messagesTotal },
                { label: "Ready", value: data.detail.messagesReady },
                { label: "Inflight", value: data.detail.messagesInflight },
                { label: "Dead-lettered", value: data.detail.messagesDeadLettered },
                { label: "Delayed", value: data.detail.messagesDelayed },
                {
                  label: "Oldest age",
                  value: humanizeSeconds(data.detail.oldestMessageAgeSeconds),
                },
              ]}
            />

            <section class="domain-section">
              <div class="domain-section-header">
                <div>
                  <p class="eyebrow">Inflight</p>
                  <h2>{data.inflight.length} entries</h2>
                </div>
              </div>

              {data.inflight.length === 0 ? (
                <DomainState
                  kind="empty"
                  message="No inflight messages are visible for this resource."
                />
              ) : (
                <QueueInflightTable messages={data.inflight} />
              )}
            </section>

            <section class="domain-section">
              <div class="domain-section-header">
                <div>
                  <p class="eyebrow">Dead letters</p>
                  <h2>{data.deadLetters.length} messages</h2>
                </div>
              </div>

              {data.deadLetters.length === 0 ? (
                <DomainState
                  kind="empty"
                  message="No dead-letter messages are visible for this resource."
                />
              ) : (
                <QueueDeadLetterTable messages={data.deadLetters} />
              )}
            </section>
          </>
        ) : null}
      </section>
    </PageShell>
  );
}
