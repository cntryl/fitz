import { state } from "@askrjs/askr";
import { navigate } from "@askrjs/askr/router";
import { SearchIcon } from "@askrjs/lucide";
import { Button } from "@askrjs/themes/controls";
import { Input } from "@askrjs/ui";
import { useOperatorContext } from "@/shared/operator-context";

function searchTarget(query: string, routeFamilyId: string) {
  const params = new URLSearchParams();
  params.set("q", query);
  if (routeFamilyId !== "all") {
    params.set("route_family", routeFamilyId);
  }

  return `/diagnostics?${params.toString()}`;
}

export default function OperatorSearch() {
  const [query, setQuery] = state("");
  const operator = useOperatorContext();
  const queryValue = query();

  function onSubmit(event: Event) {
    event.preventDefault();

    const trimmed = queryValue.trim();
    if (trimmed.length === 0) {
      return;
    }

    navigate(searchTarget(trimmed, operator.selectedRouteFamilyId));
  }

  return (
    <form class="operator-search" role="search" aria-label="Global search" onSubmit={onSubmit}>
      <SearchIcon aria-hidden="true" size={16} />
      <Input
        aria-label="Search route family, realm, area, resource, or operation"
        value={queryValue}
        onInput={(event: Event) => setQuery((event.target as HTMLInputElement).value)}
        placeholder={`Search ${operator.selectedRouteFamily.label}`}
      />
      <Button type="submit" variant="outline" size="sm">
        <SearchIcon aria-hidden="true" size={14} />
        Search
      </Button>
    </form>
  );
}
