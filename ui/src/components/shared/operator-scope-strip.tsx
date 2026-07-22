import { For } from "@askrjs/askr/control";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";
import { useOperatorScope } from "@/shared/operator-scope";

export interface OperatorScopeStripProps {
  area?: string | number | null;
  class?: string;
  freshness?: string | null;
  operation?: string | number | null;
  realm?: string | number | null;
  resource?: string | number | null;
  routeFamily?: string | number | null;
}

function scopeValue(value: string | number | null | undefined) {
  if (value === null || value === undefined) {
    return null;
  }

  const text = String(value).trim();
  return text.length > 0 ? text : null;
}

function routeFamilyDisplay(value: string) {
  return /^\d+$/.test(value) ? `Route Family ${value}` : value;
}

export default function OperatorScopeStrip({
  area,
  class: className,
  freshness,
  operation,
  realm,
  resource,
  routeFamily,
}: OperatorScopeStripProps) {
  const operator = useOperatorScope();
  const routeFamilyValue =
    scopeValue(routeFamily) ??
    scopeValue(operator.selectedRouteFamilyId) ??
    currentRouteFamilySegment();
  const routeFamilyOption = operator.routeFamilies.find((option) => option.id === routeFamilyValue);
  const routeFamilyLabel = routeFamilyOption?.label ?? routeFamilyDisplay(routeFamilyValue);
  const items = [
    { label: "Route Family", value: routeFamilyLabel },
    { label: "Realm", value: scopeValue(realm) },
    { label: "Area", value: scopeValue(area) },
    { label: "Resource", value: scopeValue(resource) },
    { label: "Operation", value: scopeValue(operation) },
    { label: "Freshness", value: scopeValue(freshness) },
  ].filter((item): item is { label: string; value: string } => item.value !== null);

  return (
    <section class={`operator-scope-strip${className ? ` ${className}` : ""}`} aria-label="Scope">
      <dl>
        <For each={items} by={(item) => item.label}>
          {(item) => (
            <div>
              <dt>{item.label}</dt>
              <dd>{item.value}</dd>
            </div>
          )}
        </For>
      </dl>
    </section>
  );
}
