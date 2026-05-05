import { Button } from "@askrjs/ui";
import { ActivityIcon } from "@askrjs/lucide";

export interface DomainHeaderProps {
  domain: string;
  title: string;
  description: string;
  onRefresh?: () => void;
}

export default function DomainHeader({ domain, title, description, onRefresh }: DomainHeaderProps) {
  return (
    <header class="domain-header">
      <div class="domain-header-copy">
        <span class="status-badge">{domain}</span>
        <h1>{title}</h1>
        <p>{description}</p>
      </div>

      {onRefresh ? (
        <Button class="secondary-action" onPress={onRefresh}>
          <ActivityIcon size={16} />
          Refresh
        </Button>
      ) : null}
    </header>
  );
}
