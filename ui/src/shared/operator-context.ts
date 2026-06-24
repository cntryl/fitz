import { defineContext, readContext, type StateSetter } from "@askrjs/askr";
import type { SessionState } from "@/features/session/session-models";
import type { MessagingTopologyOverview } from "@/features/topology/topology-models";

const STORAGE_KEY = "fitz-admin-route-family";
const ALL_ROUTE_FAMILIES = "all";

export interface RouteFamilyOption {
  description: string;
  id: string;
  label: string;
}

export interface OperatorContextSnapshot {
  routeFamilies: RouteFamilyOption[];
  selectedRouteFamily: RouteFamilyOption;
  selectedRouteFamilyId: string;
  setRouteFamily: (routeFamilyId: string) => void;
}

const allRouteFamilies: RouteFamilyOption = {
  description: "Wildcard admin context across every authorized Route Family.",
  id: ALL_ROUTE_FAMILIES,
  label: "All Route Families",
};

export function readInitialRouteFamily() {
  if (typeof window === "undefined" || !window.localStorage) {
    return ALL_ROUTE_FAMILIES;
  }

  return window.localStorage.getItem(STORAGE_KEY) ?? ALL_ROUTE_FAMILIES;
}

function persistRouteFamily(routeFamilyId: string, setRouteFamilyState: StateSetter<string>) {
  setRouteFamilyState(routeFamilyId);

  if (typeof window !== "undefined" && window.localStorage) {
    window.localStorage.setItem(STORAGE_KEY, routeFamilyId);
  }
}

function optionFromTopologyRouteFamily(routeFamily: number): RouteFamilyOption {
  return {
    description: `Resolved broker route family ${routeFamily} from the current admin topology snapshot.`,
    id: routeFamily.toString(),
    label: `Route family ${routeFamily}`,
  };
}

function labelFromRouteFamily(routeFamily: string) {
  if (/^\d+$/.test(routeFamily)) {
    return `Route family ${routeFamily}`;
  }

  return routeFamily
    .split(/[-_\s]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function optionFromSessionRouteFamily(routeFamily: string): RouteFamilyOption {
  return {
    description: `Authorized Route Family ${routeFamily} from the current admin session.`,
    id: routeFamily,
    label: labelFromRouteFamily(routeFamily),
  };
}

function uniqueOptions(options: RouteFamilyOption[]) {
  const seen = new Set<string>();

  return options.filter((option) => {
    if (seen.has(option.id)) {
      return false;
    }

    seen.add(option.id);
    return true;
  });
}

export function routeFamilyLabel(routeFamilyId: string) {
  return useOperatorContext().routeFamilies.find((option) => option.id === routeFamilyId)?.label;
}

export function parseConcreteRouteFamilyId(routeFamilyId: string) {
  if (/^\d+$/.test(routeFamilyId)) {
    return Number(routeFamilyId);
  }

  return null;
}

export function createOperatorContextSnapshot(
  topology: MessagingTopologyOverview | null | undefined,
  session: SessionState | null | undefined,
  selectedRouteFamilyId: string,
  setRouteFamilyState: StateSetter<string>,
): OperatorContextSnapshot {
  const topologyFamilies =
    topology?.sessionGroups.map((group) => optionFromTopologyRouteFamily(group.routeFamily)) ?? [];
  const sessionFamilies = session?.routeFamilies?.map(optionFromSessionRouteFamily) ?? [];
  const routeFamiliesWildcard = session?.routeFamiliesWildcard ?? true;
  const routeFamilies = routeFamiliesWildcard
    ? uniqueOptions([allRouteFamilies, ...topologyFamilies])
    : uniqueOptions(sessionFamilies.length > 0 ? sessionFamilies : [allRouteFamilies]);
  const selectedRouteFamily =
    routeFamilies.find((option) => option.id === selectedRouteFamilyId) ??
    routeFamilies[0] ??
    allRouteFamilies;

  return {
    routeFamilies,
    selectedRouteFamily,
    selectedRouteFamilyId: selectedRouteFamily.id,
    setRouteFamily: (routeFamilyId) => persistRouteFamily(routeFamilyId, setRouteFamilyState),
  };
}

const defaultOperatorContext: OperatorContextSnapshot = {
  routeFamilies: [allRouteFamilies],
  selectedRouteFamily: allRouteFamilies,
  selectedRouteFamilyId: allRouteFamilies.id,
  setRouteFamily: () => undefined,
};

export const OperatorContext = defineContext(defaultOperatorContext);

export function useOperatorContext(): OperatorContextSnapshot {
  return readContext(OperatorContext);
}
