import { EmptyState, Spinner, Block, Text } from "@askrjs/themes/components";
import { Badge } from "@askrjs/themes/components";
import { Button } from "@askrjs/themes/components";
import { formatUnknownError } from "@/shared/errors/format";
import { AppApiError } from "@/shared/errors/api";

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
      <Block direction="column" gap="sm">
        {children}
      </Block>
    </div>
  );
}

export function QueryLoadingState({
  class: className = "domain-state",
  description,
  title = "Loading",
}: QueryStateProps) {
  return (
    <div role="status" aria-live="polite" aria-busy="true">
      <QueryStateCard className={className}>
        <EmptyState
          minHeight="auto"
          padding="lg"
          title={title}
          icon={<Spinner label={title} />}
          description={description}
        />
      </QueryStateCard>
    </div>
  );
}

export function QueryErrorState({
  class: className = "domain-state",
  error,
  onRetry,
  retryLabel = "Retry",
  title = "Unable to load",
}: QueryErrorStateProps) {
  const forbidden = error instanceof AppApiError && error.code === "forbidden";
  const unavailable =
    error instanceof AppApiError &&
    (error.code === "serviceUnavailable" || error.code === "unknown");
  return (
    <QueryStateCard className={className}>
      <EmptyState
        minHeight="auto"
        padding="lg"
        role="alert"
        title={forbidden ? "Forbidden" : unavailable ? "Service unavailable" : title}
        description={
          forbidden
            ? "This admin session is not authorized for the selected Route Family."
            : unavailable
              ? "The broker did not return this snapshot. Your session is still active; retry when the service is available."
              : formatUnknownError(error)
        }
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
      <EmptyState minHeight="auto" padding="lg" title={title} description={description} />
    </QueryStateCard>
  );
}

export function QueryCompactEmptyState({
  class: className = "domain-state-compact",
  description,
  title = "Nothing to show",
}: QueryStateProps) {
  return (
    <Block className={className} direction="column" gap="xs" paddingY="sm" borderTop borderBottom>
      <Text as="strong" weight="semibold">
        {title}
      </Text>
      <Text size="sm" tone="muted">
        {description}
      </Text>
    </Block>
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
