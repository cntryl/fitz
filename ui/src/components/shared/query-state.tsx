import { EmptyState, Spinner } from "@askrjs/themes/feedback";
import { formatUnknownError } from "@/shared/errors/format";

export interface QueryStateProps {
  class?: string;
  description: string;
}

export interface QueryErrorStateProps {
  class?: string;
  error: unknown;
}

export function QueryLoadingState({
  class: className = "domain-state",
  description,
}: QueryStateProps) {
  return (
    <EmptyState class={className} icon={<Spinner label="Loading" />} description={description} />
  );
}

export function QueryErrorState({
  class: className = "domain-state",
  error,
}: QueryErrorStateProps) {
  return <EmptyState class={className} title="Error" description={formatUnknownError(error)} />;
}

export function QueryEmptyState({
  class: className = "domain-state",
  description,
}: QueryStateProps) {
  return <EmptyState class={className} description={description} />;
}

export function QueryRefreshingState({
  class: className = "domain-muted",
  description,
}: QueryStateProps) {
  return (
    <p class={className} role="status">
      {description}
    </p>
  );
}
