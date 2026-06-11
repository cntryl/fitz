import { RefreshCwIcon } from "@askrjs/lucide";
import { Button } from "@askrjs/themes/controls";

export interface DomainHeaderProps {
  domain?: string;
  title: string;
  description: string;
  onRefresh?: () => void;
}

export default function DomainHeader({
  domain: _domain,
  title,
  description,
  onRefresh,
}: DomainHeaderProps) {
  return (
    <header class="domain-header">
      <div class="domain-header-copy">
        <p class="domain-header-kicker">Broker workspace</p>
        <h1>{title}</h1>
        <p>{description}</p>
      </div>

      {onRefresh ? (
        <Button
          size="icon"
          variant="outline"
          aria-label="Refresh"
          title="Refresh"
          onPress={onRefresh}
        >
          <RefreshCwIcon size={16} />
        </Button>
      ) : null}
    </header>
  );
}
