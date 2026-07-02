import { Input, Label } from "@askrjs/ui";
import {
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Inline,
  Stack,
} from "@askrjs/themes/components";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createQueueResourceComparisonQuery } from "@/features/queue/queue-resource-query";
import type { QueueResourceRef } from "@/features/queue/queue-resource-models";
import {
  formatComparisonValue,
  formatQueueScope,
  humanizeSeconds,
  type QueueComparisonTarget,
} from "./queue-resource-presenters";

interface QueueResourceComparisonResultsProps {
  compareTarget: QueueComparisonTarget;
  resourceRef: QueueResourceRef;
}

export function QueueResourceComparisonResults({
  compareTarget,
  resourceRef,
}: QueueResourceComparisonResultsProps) {
  const comparisonQuery = createQueueResourceComparisonQuery(resourceRef, compareTarget);
  const comparison = comparisonQuery.data;

  return (
    <>
      {comparisonQuery.loading && !comparison ? (
        <QueryLoadingState description="Loading queue resource comparison..." />
      ) : null}

      {comparisonQuery.error && !comparison ? (
        <QueryErrorState
          error={comparisonQuery.error}
          onRetry={() => comparisonQuery.refresh()}
          title="Unable to compare"
        />
      ) : null}

      {comparison ? (
        <Stack gap="3">
          <DomainMetricTable
            title="Comparison summary"
            description={`Durable backlog and live reservation comparison. Current scope: ${formatQueueScope(resourceRef)}. Target scope: ${formatQueueScope(compareTarget)}.`}
            metrics={[
              { label: "Summary", value: comparison.summary },
              { label: "Mode", value: comparison.comparisonMode },
              { label: "Source", value: comparison.derived ? "Derived" : "Live" },
            ]}
          />

          <DomainMetricTable
            title="Current scope"
            description={`Current durable backlog and live reservation counters for ${formatQueueScope(comparison.left.scope)}.`}
            metrics={[
              { label: "Backlog", value: comparison.left.metrics.backlog ?? "n/a" },
              { label: "Inflight", value: comparison.left.metrics.inflight ?? "n/a" },
              { label: "Ready", value: comparison.left.metrics.ready ?? "n/a" },
              { label: "Delayed", value: comparison.left.metrics.delayed ?? "n/a" },
              { label: "Dead letters", value: comparison.left.metrics.deadLetters ?? "n/a" },
              { label: "Waiters", value: comparison.left.metrics.waiters ?? "n/a" },
              {
                label: "Age",
                value:
                  comparison.left.metrics.ageSeconds == null
                    ? "n/a"
                    : humanizeSeconds(comparison.left.metrics.ageSeconds),
              },
            ]}
          />

          <DomainMetricTable
            title="Target scope"
            description={`Target durable backlog and live reservation counters for ${formatQueueScope(comparison.right.scope)}.`}
            metrics={[
              { label: "Backlog", value: comparison.right.metrics.backlog ?? "n/a" },
              { label: "Inflight", value: comparison.right.metrics.inflight ?? "n/a" },
              { label: "Ready", value: comparison.right.metrics.ready ?? "n/a" },
              { label: "Delayed", value: comparison.right.metrics.delayed ?? "n/a" },
              { label: "Dead letters", value: comparison.right.metrics.deadLetters ?? "n/a" },
              { label: "Waiters", value: comparison.right.metrics.waiters ?? "n/a" },
              {
                label: "Age",
                value:
                  comparison.right.metrics.ageSeconds == null
                    ? "n/a"
                    : humanizeSeconds(comparison.right.metrics.ageSeconds),
              },
            ]}
          />

          <DomainMetricTable
            title="Difference"
            description="Positive values mean the current scope is ahead of the target."
            metrics={[
              { label: "Backlog delta", value: formatComparisonValue(comparison.delta.backlog) },
              { label: "Inflight delta", value: formatComparisonValue(comparison.delta.inflight) },
              { label: "Ready delta", value: formatComparisonValue(comparison.delta.ready) },
              {
                label: "Dead-letter delta",
                value: formatComparisonValue(comparison.delta.deadLetters),
              },
              { label: "Waiter delta", value: formatComparisonValue(comparison.delta.waiters) },
              {
                label: "Recent transitions delta",
                value: formatComparisonValue(comparison.delta.recentTransitionCount),
              },
            ]}
          />
        </Stack>
      ) : null}

      {comparison && comparisonQuery.refreshing ? (
        <QueryRefreshingState description="Updating queue comparison..." />
      ) : null}
    </>
  );
}

export interface QueueResourceComparePanelProps {
  compareAreaValue: string;
  compareFamilyValue: string;
  compareHint: string | null;
  compareRealmValue: string;
  compareResourceValue: string;
  compareTarget: QueueComparisonTarget | null;
  compareTargetReady: boolean;
  onClear: () => void;
  onCompareSubmit: (event: Event) => void;
  resourceRef: QueueResourceRef;
  setCompareAreaInput: (value: string) => void;
  setCompareFamilyInput: (value: string) => void;
  setCompareRealmInput: (value: string) => void;
  setCompareResourceInput: (value: string) => void;
}

export default function QueueResourceComparePanel({
  compareAreaValue,
  compareFamilyValue,
  compareHint,
  compareRealmValue,
  compareResourceValue,
  compareTarget,
  compareTargetReady,
  onClear,
  onCompareSubmit,
  resourceRef,
  setCompareAreaInput,
  setCompareFamilyInput,
  setCompareRealmInput,
  setCompareResourceInput,
}: QueueResourceComparePanelProps) {
  return (
    <Card variant="raised">
      <CardHeader>
        <CardTitle>Compare scopes</CardTitle>
        <CardDescription>
          Compare durable backlog and live reservation counters against another scope. Family is
          optional.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Stack gap="3">
          <form onSubmit={onCompareSubmit}>
            <div class="form-grid">
              <div class="auth-field">
                <Label for="compare-realm">Target realm</Label>
                <Input
                  id="compare-realm"
                  value={compareRealmValue}
                  onInput={(event: Event) =>
                    setCompareRealmInput((event.target as HTMLInputElement).value)
                  }
                  placeholder="acme"
                />
              </div>

              <div class="auth-field">
                <Label for="compare-area">Target area</Label>
                <Input
                  id="compare-area"
                  value={compareAreaValue}
                  onInput={(event: Event) =>
                    setCompareAreaInput((event.target as HTMLInputElement).value)
                  }
                  placeholder="payments"
                />
              </div>

              <div class="auth-field">
                <Label for="compare-resource">Target resource</Label>
                <Input
                  id="compare-resource"
                  value={compareResourceValue}
                  onInput={(event: Event) =>
                    setCompareResourceInput((event.target as HTMLInputElement).value)
                  }
                  placeholder="inbox"
                />
              </div>

              <div class="auth-field">
                <Label for="compare-family">Target family (optional)</Label>
                <Input
                  id="compare-family"
                  value={compareFamilyValue}
                  onInput={(event: Event) =>
                    setCompareFamilyInput((event.target as HTMLInputElement).value)
                  }
                  placeholder="2"
                />
              </div>
            </div>

            {compareHint ? <p class="domain-muted">{compareHint}</p> : null}

            <Inline gap="2" wrap="wrap">
              <Button type="submit" disabled={!compareTargetReady}>
                Compare scope
              </Button>
              <Button type="button" variant="outline" onPress={onClear}>
                Clear comparison
              </Button>
            </Inline>
          </form>

          {compareTarget ? (
            <QueueResourceComparisonResults
              resourceRef={resourceRef}
              compareTarget={compareTarget}
            />
          ) : (
            <QueryEmptyState
              title="No comparison active"
              description="Enter a target realm, area, and resource. Family is optional."
            />
          )}
        </Stack>
      </CardContent>
    </Card>
  );
}
