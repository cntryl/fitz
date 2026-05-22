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
        <Button onPress={onRefresh}>
          Refresh
        </Button>
      ) : null}
    </header>
  );
}
