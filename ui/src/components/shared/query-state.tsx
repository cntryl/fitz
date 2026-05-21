import { AlertTriangleIcon, GaugeIcon } from "@askrjs/lucide";
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
    <EmptyState
      class={className}
      icon={<Spinner label="Loading" />}
      description={description}
    />
  );
}

export function QueryErrorState({
  class: className = "domain-state",
  error,
}: QueryErrorStateProps) {
  return (
    <EmptyState
      class={className}
      icon={<AlertTriangleIcon size={18} />}
      description={formatUnknownError(error)}
    />
  );
}

export function QueryEmptyState({
  class: className = "domain-state",
  description,
}: QueryStateProps) {
  return (
    <EmptyState
      class={className}
      icon={<GaugeIcon size={18} />}
      description={description}
    />
  );
}
