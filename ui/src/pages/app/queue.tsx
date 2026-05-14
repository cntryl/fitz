import { state } from "@askrjs/askr";
import { Button, Input, Label } from "@askrjs/ui";
import { SidebarLayout } from "@askrjs/themes/components";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import { AlertTriangleIcon } from "@askrjs/lucide";
import { EmptyState, Spinner } from "@askrjs/themes/components";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import { createQueueOverviewQuery } from "@/features/queue/queue-query";
import { formatUnknownError } from "@/shared/errors/format";

function summarizeQueuePressure(messagesReady: number, messagesPending: number, deadLetters: number) {
  if (deadLetters > 0) {
    return {
      label: "Dead-letter pressure",
      detail: `${deadLetters} messages are already in the dead-letter lane.`,
    };
  }

  if (messagesPending > messagesReady) {
    return {
      label: "Backlog growth",
      detail: `${messagesPending} pending messages are outpacing ${messagesReady} ready messages.`,
    };
  }

  return {
    label: "Stable",
    detail: "Queue pressure is not currently building faster than it drains.",
  };
}

function summarizeQueueExplanation(messagesReady: number, messagesPending: number, deadLetters: number) {
  if (deadLetters > 0) {
    return {
      title: "Dead-letter pressure",
      body: "The queue is already shedding failures into the dead-letter lane, so the next drilldown should focus on the hottest resource and its dead-letter list.",
      next: "Open a queue resource drilldown and inspect the dead-letter table.",
    };
  }

  if (messagesPending > messagesReady) {
    return {
      title: "Backlog growth",
      body: "Pending work is outpacing ready work, which usually means the queue is building pressure faster than it drains.",
      next: "Jump to a queue resource drilldown to inspect inflight and timeline events.",
    };
  }

  return {
    title: "No obvious stall",
    body: "The current counts do not show an immediate backlog or dead-letter spike, so the next check is usually a targeted resource scope.",
    next: "Use the jump bar to open a specific queue resource or compare another scope.",
  };
}

export default function QueuePage() {
  const overview = createQueueOverviewQuery();
  const data = overview.data;
  const domainInput = state("queue");
  const realmInput = state("");
  const areaInput = state("");
  const resourceInput = state("");

  const sidebar = createDomainSidebar({
    data,
    title: "Queue snapshot",
    description: "Current queue health across messages and broker activity.",
    stats: (current) => [
      { label: "Ready", value: current.stats.messagesReady },
      { label: "Inflight", value: current.stats.inflightActive },
      { label: "Pending", value: current.stats.messagesPending },
      { label: "Dead-lettered", value: current.stats.messagesDeadLettered },
      { label: "Delayed", value: current.stats.messagesDelayed },
      {
        label: "Ops / sec",
        value: current.stats.operationsPerSecond.toFixed(2),
        note: "Live broker snapshot",
      },
    ],
  });

  function onJumpSubmit(event: Event) {
    event.preventDefault();

    if (typeof window === "undefined") {
      return;
    }

    const nextDomain = domainInput().trim().toLowerCase() || "queue";
    const nextRealm = realmInput().trim();
    const nextArea = areaInput().trim();
    const nextResource = resourceInput().trim();

    if (nextDomain === "queue" && nextRealm && nextArea && nextResource) {
      window.location.assign(`/queue/${encodeURIComponent(nextRealm)}/${encodeURIComponent(nextArea)}/${encodeURIComponent(nextResource)}`);
      return;
    }

    if (nextDomain === "queue") {
      window.location.assign("/queue");
      return;
    }

    window.location.assign(`/${encodeURIComponent(nextDomain)}`);
  }

  return (
    <SidebarLayout
      sidebar={sidebar}
      sidebarPosition="end"
      sidebarWidth="18rem"
      gap="1.5rem"
      collapseBelow="md"
    >
      <section class="domain-page">
        <DomainHeader
          domain="Queue"
          title="Queue overview"
          description="Live queue statistics and realm inventory. Resource-level dead-letter drill-down comes next."
          onRefresh={() => overview.refresh()}
        />

        {overview.loading ? (
          <EmptyState
            class="domain-state"
            icon={<Spinner label="Loading" />}
            description="Loading queue overview..."
          />
        ) : null}

        {overview.error ? (
          <EmptyState
            class="domain-state"
            icon={<AlertTriangleIcon size={18} />}
            description={formatUnknownError(overview.error)}
          />
        ) : null}

        {data && !overview.loading && !overview.error ? (
          <>
            {(() => {
              const pressure = summarizeQueuePressure(
                data.stats.messagesReady,
                data.stats.messagesPending,
                data.stats.messagesDeadLettered,
              );

              return (
                <Card class="dashboard-status-card" variant="raised">
                  <CardHeader>
                    <CardTitle>Current pressure</CardTitle>
                    <CardDescription>{pressure.label}</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <p>{pressure.detail}</p>
                  </CardContent>
                </Card>
              );
            })()}

            <section class="domain-section">
              <div class="domain-section-header">
                <div>
                  <p class="eyebrow">Jump / filter</p>
                  <h2>Open a troubleshooting scope</h2>
                </div>
              </div>

              <form class="domain-stack" onSubmit={onJumpSubmit}>
                <div class="auth-field">
                  <Label for="domain-filter">Domain</Label>
                  <Input
                    id="domain-filter"
                    value={domainInput()}
                    onInput={(event: Event) => domainInput.set((event.target as HTMLInputElement).value)}
                    placeholder="queue, lease, rpc, notice, schedule, stream, kv"
                  />
                </div>

                <div class="auth-field">
                  <Label for="realm-filter">Realm</Label>
                  <Input
                    id="realm-filter"
                    value={realmInput()}
                    onInput={(event: Event) => realmInput.set((event.target as HTMLInputElement).value)}
                    placeholder="Optional realm"
                  />
                </div>

                <div class="auth-field">
                  <Label for="area-filter">Area</Label>
                  <Input
                    id="area-filter"
                    value={areaInput()}
                    onInput={(event: Event) => areaInput.set((event.target as HTMLInputElement).value)}
                    placeholder="Optional area"
                  />
                </div>

                <div class="auth-field">
                  <Label for="resource-filter">Resource</Label>
                  <Input
                    id="resource-filter"
                    value={resourceInput()}
                    onInput={(event: Event) => resourceInput.set((event.target as HTMLInputElement).value)}
                    placeholder="Optional resource"
                  />
                </div>

                <div class="session-filter-actions">
                  <Button type="submit" class="primary-action">
                    Open scope
                  </Button>
                  <Button
                    type="button"
                    class="secondary-action"
                    onPress={() => {
                      domainInput.set("queue");
                      realmInput.set("");
                      areaInput.set("");
                      resourceInput.set("");
                    }}
                  >
                    Reset
                  </Button>
                </div>
              </form>
            </section>

            {(() => {
              const explanation = summarizeQueueExplanation(
                data.stats.messagesReady,
                data.stats.messagesPending,
                data.stats.messagesDeadLettered,
              );

              return (
                <Card class="dashboard-status-card" variant="raised">
                  <CardHeader>
                    <CardTitle>Likely explanation</CardTitle>
                    <CardDescription>{explanation.title}</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <p>{explanation.body}</p>
                    <p>{explanation.next}</p>
                  </CardContent>
                </Card>
              );
            })()}

            <DomainMetricTable
              title="Queue metrics"
              metrics={[
                { label: "Inflight", value: data.stats.inflightActive },
                { label: "Ready", value: data.stats.messagesReady },
                { label: "Pending", value: data.stats.messagesPending },
                { label: "Dead-lettered", value: data.stats.messagesDeadLettered },
                { label: "Delayed", value: data.stats.messagesDelayed },
                {
                  label: "Ops / sec",
                  value: data.stats.operationsPerSecond.toFixed(2),
                },
              ]}
            />

            <DomainRealmTable
              title="Queue realms"
              realms={data.realms}
              emptyMessage="No queue realms are currently visible."
            />
          </>
        ) : null}
      </section>
    </SidebarLayout>
  );
}
