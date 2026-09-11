import { RefreshCwIcon } from "@askrjs/lucide";
import { Block, Button, PageHeader, Text } from "@askrjs/themes/components";
import { Badge } from "@askrjs/themes/components";

export interface DomainHeaderProps {
  eyebrow?: string;
  domain?: string;
  primaryAction?: {
    busy?: boolean;
    disabled?: boolean;
    icon?: unknown;
    label: string;
    onPress: () => void;
  };
  secondaryAction?: {
    busy?: boolean;
    disabled?: boolean;
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
  secondaryAction,
  status,
  title,
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
      <Text as="span" class="domain-header-kicker">
        {eyebrow ?? _domain ?? "Broker workspace"}
      </Text>
      <PageHeader
        class="domain-header-page"
        title={
          <span class="domain-header-title-row">
            <span>{title}</span>
            {status ? (
              <Badge role="status" aria-live="polite" variant={status.tone ?? "info"}>
                {status.label}
              </Badge>
            ) : null}
          </span>
        }
        actions={
          action || secondaryAction ? (
            <Block direction="row" gap="xs" wrap={true}>
              {secondaryAction ? (
                <Button
                  variant="outline"
                  aria-label={secondaryAction.label}
                  aria-busy={secondaryAction.busy ? "true" : undefined}
                  disabled={secondaryAction.disabled || secondaryAction.busy}
                  title={secondaryAction.label}
                  onPress={secondaryAction.onPress}
                >
                  {secondaryAction.icon ?? <RefreshCwIcon size={16} />}
                  <span>{secondaryAction.label}</span>
                </Button>
              ) : null}
              {action ? (
                <Button
                  variant={secondaryAction ? undefined : "outline"}
                  aria-label={action.label}
                  aria-busy={action.busy ? "true" : undefined}
                  disabled={action.disabled || action.busy}
                  title={action.label}
                  onPress={action.onPress}
                >
                  {action.icon ??
                    (action.label.startsWith("Refresh") ? <RefreshCwIcon size={16} /> : null)}
                  <span>{action.label}</span>
                </Button>
              ) : null}
            </Block>
          ) : undefined
        }
      />
    </header>
  );
}
