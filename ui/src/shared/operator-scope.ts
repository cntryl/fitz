import { defineScope, readScope } from "@askrjs/askr";
import type { SessionState } from "@/features/session/session-models";
import type { MessagingTopologyOverview } from "@/features/topology/topology-models";

const unselectedRouteFamilyId = "";
const routeFamilyPattern = /^\d+$/;

export interface RouteFamilyOption {
  description: string;
  id: string;
  label: string;
}

export type RouteFamilyLoadState = "loading" | "error" | "empty" | "ready";

export interface OperatorScopeSnapshot {
  retryRouteFamilies: () => void;
  routeFamilies: RouteFamilyOption[];
  routeFamilyError: unknown;
  routeFamilyState: RouteFamilyLoadState;
  routeFamiliesWildcard: boolean;
  selectedRouteFamily: RouteFamilyOption;
  selectedRouteFamilyId: string;
}

const unselectedRouteFamily: RouteFamilyOption = {
  description: "Choose a concrete Route Family before opening the operator workspace.",
  id: unselectedRouteFamilyId,
  label: "Select Route Family",
};

function optionFromTopologyRouteFamily(routeFamily: number): RouteFamilyOption {
  return {
    description: `Resolved broker route family ${routeFamily} from the current admin topology snapshot.`,
    id: routeFamily.toString(),
    label: `Route family ${routeFamily}`,
  };
}

function labelFromRouteFamily(routeFamily: string) {
  return `Route family ${routeFamily}`;
}

function optionFromSessionRouteFamily(routeFamily: string, openAccess: boolean): RouteFamilyOption {
  return {
    description: openAccess
      ? `Available Route Family ${routeFamily} from admin feature discovery.`
      : `Authorized Route Family ${routeFamily} from the current admin session.`,
    id: routeFamily,
    label: labelFromRouteFamily(routeFamily),
  };
}

function optionFromWildcardRouteFamily(
  routeFamily: string,
  openAccess: boolean,
): RouteFamilyOption {
  return {
    description: openAccess
      ? `Available Route Family ${routeFamily} through open wildcard access.`
      : `Authorized Route Family ${routeFamily} through this session's wildcard access.`,
    id: routeFamily,
    label: labelFromRouteFamily(routeFamily),
  };
}

function uniqueOptions(options: RouteFamilyOption[]) {
  const seen = new Set<string>();

  return options.filter((option) => {
    if (seen.has(option.id)) return false;
    seen.add(option.id);
    return true;
  });
}

export function routeFamilyLabel(routeFamilyId: string) {
  return useOperatorScope().routeFamilies.find((option) => option.id === routeFamilyId)?.label;
}

export function parseConcreteRouteFamilyId(routeFamilyId: string) {
  return routeFamilyPattern.test(routeFamilyId) ? Number(routeFamilyId) : null;
}

export function createOperatorScopeSnapshot(
  topology: MessagingTopologyOverview | null | undefined,
  session: SessionState | null | undefined,
  selectedRouteFamilyId: string,
  load: {
    error?: unknown;
    loading?: boolean;
    retry?: () => void;
  } = {},
): OperatorScopeSnapshot {
  const topologyFamilies =
    topology?.sessionGroups.map((group) => optionFromTopologyRouteFamily(group.routeFamily)) ?? [];
  const openAccess = session?.authRequired === false;
  const sessionFamilies =
    session?.routeFamilies
      ?.filter((routeFamily) => routeFamilyPattern.test(routeFamily))
      .map((routeFamily) => optionFromSessionRouteFamily(routeFamily, openAccess)) ?? [];
  const routeFamiliesWildcard = session?.routeFamiliesWildcard === true;
  const knownRouteFamilies = routeFamiliesWildcard
    ? uniqueOptions([...sessionFamilies, ...topologyFamilies])
    : sessionFamilies;
  const wildcardSelection =
    routeFamiliesWildcard && routeFamilyPattern.test(selectedRouteFamilyId)
      ? optionFromWildcardRouteFamily(selectedRouteFamilyId, openAccess)
      : null;
  const routeFamilies = uniqueOptions(
    wildcardSelection ? [wildcardSelection, ...knownRouteFamilies] : knownRouteFamilies,
  );
  const selectedRouteFamily =
    routeFamilies.find((option) => option.id === selectedRouteFamilyId) ?? unselectedRouteFamily;
  const routeFamilyState: RouteFamilyLoadState =
    routeFamilies.length > 0 || routeFamiliesWildcard
      ? "ready"
      : load.loading
        ? "loading"
        : load.error
          ? "error"
          : "empty";

  return {
    retryRouteFamilies: load.retry ?? (() => undefined),
    routeFamilies,
    routeFamilyError: load.error ?? null,
    routeFamilyState,
    routeFamiliesWildcard,
    selectedRouteFamily,
    selectedRouteFamilyId: selectedRouteFamily.id,
  };
}

const defaultOperatorScope: OperatorScopeSnapshot = {
  retryRouteFamilies: () => undefined,
  routeFamilies: [],
  routeFamilyError: null,
  routeFamilyState: "empty",
  routeFamiliesWildcard: false,
  selectedRouteFamily: unselectedRouteFamily,
  selectedRouteFamilyId: unselectedRouteFamily.id,
};

export const OperatorScope = defineScope(defaultOperatorScope);

export function useOperatorScope(): OperatorScopeSnapshot {
  return readScope(OperatorScope);
}
