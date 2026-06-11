import { EmptyState, Spinner } from "@askrjs/themes/feedback";
import { Card, CardContent } from "@askrjs/themes/surfaces";
import { Stack } from "@askrjs/themes/layouts";
import { formatUnknownError } from "@/shared/errors/format";

export interface QueryStateProps {
  class?: string;
  description: string;
}

export interface QueryErrorStateProps {
  class?: string;
  error: unknown;
}

function QueryStateCard({
  children,
  className = "domain-state",
}: {
  children?: unknown;
  className?: string;
}) {
  return (
    <Card class={className} padding="sm" variant="default">
      <CardContent>
        <Stack gap="3">{children}</Stack>
      </CardContent>
    </Card>
  );
}

export function QueryLoadingState({
  class: className = "domain-state",
  description,
}: QueryStateProps) {
  return (
    <QueryStateCard className={className}>
      <EmptyState icon={<Spinner label="Loading" />} description={description} />
    </QueryStateCard>
  );
}

export function QueryErrorState({
  class: className = "domain-state",
  error,
}: QueryErrorStateProps) {
  return (
    <QueryStateCard className={className}>
      <EmptyState title="Error" description={formatUnknownError(error)} />
    </QueryStateCard>
  );
}

export function QueryEmptyState({
  class: className = "domain-state",
  description,
}: QueryStateProps) {
  return (
    <QueryStateCard className={className}>
      <EmptyState description={description} />
    </QueryStateCard>
  );
}

export function QueryRefreshingState({
  class: className = "domain-state",
  description,
}: QueryStateProps) {
  return (
    <div class={`${className} domain-state-inline`} role="status">
      <Spinner label="Refreshing" />
      <p>{description}</p>
    </div>
  );
}
