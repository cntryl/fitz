import { EmptyState, Spinner } from "@askrjs/themes/components";
import { AlertTriangleIcon, GaugeIcon } from "@askrjs/lucide";
import { formatUnknownError } from "@/shared/errors/format";

export type DomainStateKind = "loading" | "empty" | "error";

export interface DomainStateProps {
  kind: DomainStateKind;
  message: string;
  error?: unknown;
}

export default function DomainState({ kind, message, error }: DomainStateProps) {
  const resolvedMessage = kind === "error" && error != null ? formatUnknownError(error) : message;
  const icon =
    kind === "loading" ? (
      <Spinner label="Loading" />
    ) : kind === "error" ? (
      <AlertTriangleIcon size={18} />
    ) : (
      <GaugeIcon size={18} />
    );

  return <EmptyState class="domain-state" icon={icon} description={resolvedMessage} />;
}
