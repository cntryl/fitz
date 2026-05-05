import { GaugeIcon } from "@askrjs/lucide";
import { formatUnknownError } from "@/shared/errors/format";

export type DomainStateKind = "loading" | "empty" | "error";

export interface DomainStateProps {
  kind: DomainStateKind;
  message: string;
  error?: unknown;
}

export default function DomainState({ kind, message, error }: DomainStateProps) {
  const resolvedMessage = kind === "error" && error != null ? formatUnknownError(error) : message;

  return (
    <div class="domain-state">
      {kind === "empty" ? <GaugeIcon size={18} /> : null}
      <p>{resolvedMessage}</p>
    </div>
  );
}
