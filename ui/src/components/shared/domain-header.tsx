import { ActivityIcon } from "@askrjs/lucide";
import { Button } from "@askrjs/themes/controls";

export interface DomainHeaderProps {
  domain: string;
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
