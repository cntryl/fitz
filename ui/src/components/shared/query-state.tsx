import { EmptyState, Spinner } from "@askrjs/themes/feedback";
import { Badge } from "@askrjs/themes/surfaces";
import { Stack } from "@askrjs/themes/layouts";
import { Button } from "@askrjs/themes/controls";
import { formatUnknownError } from "@/shared/errors/format";

export interface QueryStateProps {
  class?: string;
  description: string;
  title?: string;
}

export interface QueryErrorStateProps {
  class?: string;
  error: unknown;
  onRetry?: () => void;
  retryLabel?: string;
  title?: string;
}

function QueryStateCard({
  children,
  className = "domain-state",
}: {
  children?: unknown;
  className?: string;
}) {
  return (
    <div class={className}>
      <Stack gap="3">{children}</Stack>
    </div>
  );
}

export function QueryLoadingState({
  class: className = "domain-state",
  description,
  title = "Loading",
}: QueryStateProps) {
  return (
    <QueryStateCard className={className}>
      <EmptyState
        aria-busy="true"
        title={title}
        icon={<Spinner label={title} />}
        description={description}
      />
    </QueryStateCard>
  );
}

export function QueryErrorState({
  class: className = "domain-state",
  error,
  onRetry,
  retryLabel = "Retry",
  title = "Unable to load",
}: QueryErrorStateProps) {
  return (
    <QueryStateCard className={className}>
      <EmptyState
        role="alert"
        title={title}
        description={formatUnknownError(error)}
        actions={
          onRetry ? (
            <Button variant="outline" onPress={onRetry}>
              {retryLabel}
            </Button>
          ) : undefined
        }
      />
    </QueryStateCard>
  );
}

export function QueryEmptyState({
  class: className = "domain-state",
  description,
  title = "Nothing to show",
}: QueryStateProps) {
  return (
    <QueryStateCard className={className}>
      <EmptyState title={title} description={description} />
    </QueryStateCard>
  );
}

export function QueryRefreshingState({
  class: className = "domain-state",
  description,
  title = "Refreshing",
}: QueryStateProps) {
  return (
    <div class={`${className} domain-state-inline`} role="status" aria-live="polite">
      <Badge variant="info">{title}</Badge>
      <Spinner label={title} />
      <p>{description}</p>
    </div>
  );
}
