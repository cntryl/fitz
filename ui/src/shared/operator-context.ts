import { defineContext, readContext, type StateSetter } from "@askrjs/askr";
import type { SessionState } from "@/features/session/session-models";
import type { MessagingTopologyOverview } from "@/features/topology/topology-models";

const STORAGE_KEY = "fitz-admin-route-family";
const unselectedRouteFamilyId = "";
const routeFamilyPattern = /^\d+$/;

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

const unselectedRouteFamily: RouteFamilyOption = {
  description: "Choose a concrete Route Family before opening the operator workspace.",
  id: unselectedRouteFamilyId,
  label: "Select Route Family",
};

export function readInitialRouteFamily() {
  if (typeof window === "undefined" || !window.localStorage) {
    return unselectedRouteFamilyId;
  }

  const storedRouteFamily = window.localStorage.getItem(STORAGE_KEY);

  return routeFamilyPattern.test(storedRouteFamily ?? "")
    ? (storedRouteFamily ?? unselectedRouteFamilyId)
    : unselectedRouteFamilyId;
}

function persistRouteFamily(routeFamilyId: string, setRouteFamilyState?: StateSetter<string>) {
  setRouteFamilyState?.(routeFamilyId);

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
  if (routeFamilyPattern.test(routeFamilyId)) {
    return Number(routeFamilyId);
  }

  return null;
}

export function createOperatorContextSnapshot(
  topology: MessagingTopologyOverview | null | undefined,
  session: SessionState | null | undefined,
  selectedRouteFamilyId: string,
  setRouteFamilyState?: StateSetter<string>,
): OperatorContextSnapshot {
  const topologyFamilies =
    topology?.sessionGroups.map((group) => optionFromTopologyRouteFamily(group.routeFamily)) ?? [];
  const sessionFamilies =
    session?.routeFamilies
      ?.filter((routeFamily) => routeFamilyPattern.test(routeFamily))
      .map(optionFromSessionRouteFamily) ?? [];
  const routeFamilies = uniqueOptions([...sessionFamilies, ...topologyFamilies]);
  const selectedRouteFamily =
    routeFamilies.find((option) => option.id === selectedRouteFamilyId) ?? unselectedRouteFamily;

  return {
    routeFamilies,
    selectedRouteFamily,
    selectedRouteFamilyId: selectedRouteFamily.id,
    setRouteFamily: (routeFamilyId) => persistRouteFamily(routeFamilyId, setRouteFamilyState),
  };
}

const defaultOperatorContext: OperatorContextSnapshot = {
  routeFamilies: [],
  selectedRouteFamily: unselectedRouteFamily,
  selectedRouteFamilyId: unselectedRouteFamily.id,
  setRouteFamily: () => undefined,
};

export const OperatorContext = defineContext(defaultOperatorContext);

export function useOperatorContext(): OperatorContextSnapshot {
  return readContext(OperatorContext);
}
