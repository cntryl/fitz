import { RefreshCwIcon } from "@askrjs/lucide";
import { Button } from "@askrjs/themes/controls";
import { Badge } from "@askrjs/themes/surfaces";
import { Stack } from "@askrjs/themes/layouts";

export interface DomainHeaderProps {
  eyebrow?: string;
  domain?: string;
  primaryAction?: {
    icon?: unknown;
    label: string;
    onPress: () => void;
  };
  status?: {
    detail?: string;
    label: string;
    tone?: "default" | "info" | "success" | "warning" | "danger";
  };
  title: string;
  description: string;
  onRefresh?: () => void;
}

export default function DomainHeader({
  eyebrow,
  domain: _domain,
  primaryAction,
  status,
  title,
  description,
  onRefresh,
}: DomainHeaderProps) {
  const action =
    primaryAction ??
    (onRefresh
      ? {
          label: "Refresh",
          onPress: onRefresh,
        }
      : null);

  return (
    <header class="domain-header">
      <div class="domain-header-copy">
        <p class="domain-header-kicker">{eyebrow ?? _domain ?? "Broker workspace"}</p>
        <div class="domain-header-title-row">
          <h1>{title}</h1>
          {status ? <Badge variant={status.tone ?? "info"}>{status.label}</Badge> : null}
        </div>
        <p>{description}</p>
        {status?.detail ? <p class="domain-header-detail">{status.detail}</p> : null}
      </div>

      {action ? (
        <Stack gap="2" class="domain-header-actions">
          <Button variant="outline" aria-label={action.label} title={action.label} onPress={action.onPress}>
            {action.icon ?? <RefreshCwIcon size={16} />}
            <span>{action.label}</span>
          </Button>
        </Stack>
      ) : null}
    </header>
  );
}
