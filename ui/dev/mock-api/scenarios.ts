import type { Domain, MockResponse } from "./fixtures";
import { domains, json, now } from "./fixtures";

export type MockDomainState = "idle" | "healthy" | "unhealthy" | "chaos";

type FamilyScenario = {
  domainStates: Record<Domain, MockDomainState>;
  family: number;
  label: string;
  state: MockDomainState;
};

const allDomains = (state: MockDomainState): Record<Domain, MockDomainState> =>
  Object.fromEntries(domains.map((domain) => [domain, state])) as Record<Domain, MockDomainState>;

export const familyScenarios: Record<number, FamilyScenario> = {
  1: {
    domainStates: allDomains("idle"),
    family: 1,
    label: "Idle",
    state: "idle",
  },
  2: {
    domainStates: allDomains("healthy"),
    family: 2,
    label: "Healthy activity",
    state: "healthy",
  },
  3: {
    domainStates: {
      kv: "healthy",
      lease: "unhealthy",
      notice: "healthy",
      queue: "unhealthy",
      rpc: "unhealthy",
      schedule: "healthy",
      stream: "healthy",
    },
    family: 3,
    label: "Mixed health",
    state: "unhealthy",
  },
  4: {
    domainStates: allDomains("unhealthy"),
    family: 4,
    label: "Unhealthy",
    state: "unhealthy",
  },
  5: {
    domainStates: allDomains("chaos"),
    family: 5,
    label: "Chaos",
    state: "chaos",
  },
};

const issueKeyPattern =
  /(backpressure|conflict|dead.?letter|delayed|drop|fail|invalid|oldest|overdue|pending|pressure|reject|retr(?:y|ies)|timeout|waiter|wrong)/i;
const preservedNumberKeyPattern =
  /(^family$|route_family|_id$|^id$|^limit$|offset|cursor|len_bytes|generated_at|_at_ms$|fire_ms|uptime|subscription_id|message_id|tx_id|token$)/i;
const emptyCollectionKeys = new Set([
  "areas",
  "events",
  "family_watermarks",
  "hotspots",
  "inflight",
  "items",
  "messages",
  "observations",
  "operations",
  "queues",
  "realms",
  "records",
  "resources",
  "session_groups",
  "sessions",
  "subscriptions",
  "transactions",
  "workers",
]);

function scenarioForFamily(family: number) {
  return familyScenarios[family] ?? familyScenarios[2]!;
}

function isDomain(value: unknown): value is Domain {
  return typeof value === "string" && domains.some((domain) => domain === value);
}

function stateForDomain(scenario: FamilyScenario, domain: Domain | undefined) {
  return domain ? scenario.domainStates[domain] : scenario.state;
}

function scaleNumber(value: number, key: string, state: MockDomainState) {
  if (preservedNumberKeyPattern.test(key)) return value;
  if (state === "idle") return 0;

  const issueValue = issueKeyPattern.test(key);
  if (state === "healthy") return issueValue ? 0 : value;

  const factor = state === "chaos" ? 8 : 2.5;
  const minimum = state === "chaos" ? 17 : 3;
  const scaled = issueValue ? Math.max(value * factor, minimum) : value * (factor / 2);

  return Number.isInteger(value) ? Math.round(scaled) : Number(scaled.toFixed(2));
}

function diagnosticForState(
  diagnostic: Record<string, unknown>,
  state: MockDomainState,
  domain?: Domain,
) {
  if (state === "idle" || state === "healthy") {
    return {
      ...diagnostic,
      confidence: 1,
      confidence_justification: {
        rationale:
          state === "idle"
            ? "No broker-visible activity exists for this Route Family."
            : "Activity is flowing without current pressure signals.",
        signals_matched: [],
        signals_missing: [],
      },
      contention_count: 0,
      current_stage: state,
      delta_1h: 0,
      delta_5m: 0,
      explanation_hints: [
        state === "idle"
          ? "No active resources or sessions are visible."
          : "Current activity is within healthy operating bounds.",
      ],
      failure_count: 0,
      last_failure_at: null,
      likely_bottleneck: null,
      recent_transition_count: 0,
      severity: "informational",
      trend: "steady",
      waiter_count: 0,
    };
  }

  const chaos = state === "chaos";
  return {
    ...diagnostic,
    confidence: chaos ? 0.99 : 0.91,
    confidence_justification: {
      rationale: chaos
        ? "Multiple pressure, failure, and saturation signals are changing simultaneously."
        : "Current backlog, failure, and contention gauges agree on active pressure.",
      signals_matched: ["backlog", "failures", "contention", "active sessions"],
      signals_missing: [],
    },
    contention_count: chaos ? 89 : 14,
    current_stage: `${domain ?? "broker"}_${chaos ? "chaos" : "pressure"}`,
    delta_1h: chaos ? 377 : 64,
    delta_5m: chaos ? 144 : 18,
    explanation_hints: [
      chaos
        ? "Signals are volatile across delivery, storage, and coordination paths."
        : `${domain ?? "Broker"} pressure needs operator attention.`,
    ],
    failure_count: chaos ? 55 : 9,
    last_changed_at: now,
    last_failure_at: now,
    likely_bottleneck: chaos ? "multiple competing bottlenecks" : `${domain ?? "broker"} capacity`,
    recent_transition_count: chaos ? 233 : 24,
    severity: chaos ? "critical" : "high",
    trend: "growing",
    waiter_count: chaos ? 144 : 21,
  };
}

function looksLikeDiagnostic(value: Record<string, unknown>) {
  return (
    typeof value.current_stage === "string" &&
    typeof value.severity === "string" &&
    "confidence_justification" in value
  );
}

function transformString(value: string, key: string, family: number, state: MockDomainState) {
  const scoped = value
    .replace(/\/api\/v1\/\d+\//g, `/api/v1/${family}/`)
    .replace(/\/admin\/\d+\//g, `/admin/${family}/`);

  if (key === "health") {
    return state === "healthy" ? "healthy" : state === "idle" ? "idle" : "pressure";
  }
  if (key === "status" && ["falling_behind", "backlogged", "draining", "idle"].includes(value)) {
    if (state === "idle") return "idle";
    if (state === "healthy") return "draining";
    return state === "chaos" ? "backlogged" : "falling_behind";
  }

  return scoped;
}

function transformValue(
  value: unknown,
  key: string,
  family: number,
  scenario: FamilyScenario,
  inheritedDomain?: Domain,
): unknown {
  const state = stateForDomain(scenario, inheritedDomain);

  if (Array.isArray(value)) {
    if (state === "idle" && emptyCollectionKeys.has(key)) return [];
    return value.map((entry) => transformValue(entry, key, family, scenario, inheritedDomain));
  }

  if (value && typeof value === "object") {
    const object = value as Record<string, unknown>;
    const objectDomain = isDomain(object.domain) ? object.domain : inheritedDomain;
    const objectState = stateForDomain(scenario, objectDomain);

    if (looksLikeDiagnostic(object)) {
      return diagnosticForState(object, objectState, objectDomain);
    }

    return Object.fromEntries(
      Object.entries(object).map(([childKey, childValue]) => {
        if (childKey === "route_family" || childKey === "family") {
          return [childKey, typeof childValue === "string" ? family.toString() : family];
        }

        return [childKey, transformValue(childValue, childKey, family, scenario, objectDomain)];
      }),
    );
  }

  if (typeof value === "number") return scaleNumber(value, key, state);
  if (typeof value === "string") return transformString(value, key, family, state);
  return value;
}

function sessionsForScenario(body: Record<string, unknown>, scenario: FamilyScenario) {
  if (scenario.state === "idle") return { ...body, sessions: [] };

  const source = Array.isArray(body.sessions) ? body.sessions : [];
  const countByFamily = { 2: 3, 3: 4, 4: 5, 5: 8 } as Record<number, number>;
  const count = countByFamily[scenario.family] ?? 2;
  const sessions = Array.from({ length: count }, (_, index) => {
    const base = (source[index % Math.max(source.length, 1)] ?? {}) as Record<string, unknown>;
    const state = scenario.state;

    return {
      ...(transformValue(base, "session", scenario.family, scenario) as Record<string, unknown>),
      idle_seconds: state === "chaos" ? index * 19 : index * 7,
      messages_received: state === "chaos" ? 20_000 + index * 1_337 : 900 + index * 211,
      messages_sent: state === "chaos" ? 31_000 + index * 1_733 : 1_200 + index * 233,
      route_family: scenario.family,
      session_id: `sess-family-${scenario.family}-${index + 1}`,
    };
  });

  return { ...body, sessions };
}

function globalDiagnostics(body: Record<string, unknown>, scenario: FamilyScenario) {
  if (scenario.state === "idle" || scenario.state === "healthy") {
    return {
      ...body,
      hotspots: [],
      incident_summary: {
        confidence: 1,
        explanation:
          scenario.state === "idle"
            ? "No activity is currently visible for this Route Family."
            : "All domains are active and operating within healthy bounds.",
        likely_bottleneck: null,
        recommended_next_query: null,
        severity: "informational",
        status: "healthy",
        suggested_next_queries: [],
        title: scenario.state === "idle" ? "Idle Route Family" : "Healthy activity",
      },
      last_significant_transition_at: scenario.state === "idle" ? null : now,
      top_bottleneck: undefined,
    };
  }

  const affectedDomains = domains.filter(
    (domain) =>
      scenario.domainStates[domain] === "unhealthy" || scenario.domainStates[domain] === "chaos",
  );
  const sourceHotspots = Array.isArray(body.hotspots) ? body.hotspots : [];
  const hotspots = affectedDomains.map((domain, index) => {
    const source = (sourceHotspots[index % Math.max(sourceHotspots.length, 1)] ?? {}) as Record<
      string,
      unknown
    >;

    return {
      ...diagnosticForState(source, scenario.domainStates[domain], domain),
      area: "operations",
      domain,
      family: scenario.family,
      realm: "production",
      resource: `${domain}-primary`,
    };
  });
  const chaos = scenario.state === "chaos";

  return {
    ...body,
    hotspots,
    incident_summary: {
      confidence: chaos ? 0.99 : 0.92,
      explanation: chaos
        ? "Every domain is reporting volatile failures, saturation, or blocked delivery."
        : scenario.family === 3
          ? "Some domains are healthy while Queue, RPC, and Lease require attention."
          : "Every domain is reporting active pressure or failure signals.",
      likely_bottleneck: chaos ? "multiple competing bottlenecks" : "cross-domain capacity",
      recommended_next_query: `Open Route Family ${scenario.family} diagnostics`,
      severity: chaos ? "critical" : "high",
      status: chaos ? "stalled" : "degraded",
      suggested_next_queries: affectedDomains.slice(0, 3).map((domain, index) => ({
        endpoint: `/api/v1/${scenario.family}/${domain}/stats`,
        priority: index + 1,
        rationale: `${domain} has active ${chaos ? "chaotic" : "unhealthy"} signals.`,
        remediation: `Inspect ${domain} resources and current sessions.`,
        title: `Inspect ${domain}`,
      })),
      title: chaos
        ? "Route Family chaos"
        : scenario.family === 3
          ? "Mixed domain health"
          : "All domains unhealthy",
    },
    last_significant_transition_at: now,
    top_bottleneck: hotspots[0],
  };
}

function statsForScenario(body: Record<string, unknown>, scenario: FamilyScenario) {
  const transformed = transformValue(body, "stats", scenario.family, scenario) as Record<
    string,
    unknown
  >;
  const sourceDomains = (body.domains ?? {}) as Record<string, unknown>;

  return {
    ...transformed,
    broker: transformValue(body.broker, "broker", scenario.family, scenario),
    diagnostics: globalDiagnostics((body.diagnostics ?? {}) as Record<string, unknown>, scenario),
    domains: Object.fromEntries(
      domains.map((domain) => [
        domain,
        transformValue(sourceDomains[domain] ?? {}, domain, scenario.family, scenario, domain),
      ]),
    ),
  };
}

function topologyStateFor(state: MockDomainState) {
  if (state === "idle") return "quiet";
  if (state === "healthy") return "flowing";
  if (state === "chaos") return "blocked";
  return "pressure";
}

function topologyForScenario(body: Record<string, unknown>, scenario: FamilyScenario) {
  const transformed = transformValue(body, "topology", scenario.family, scenario) as Record<
    string,
    unknown
  >;
  const sourceLanes = Array.isArray(body.lanes) ? body.lanes : [];
  const scopedSessions = sessionsForScenario(
    {
      sessions: Array.isArray(body.session_groups)
        ? ((body.session_groups[0] as Record<string, unknown> | undefined)
            ?.representative_sessions ?? [])
        : [],
    },
    scenario,
  ).sessions as unknown[];

  const lanes = sourceLanes.map((entry) => {
    const lane = entry as Record<string, unknown>;
    const domain = isDomain(lane.id) ? lane.id : "queue";
    const state = scenario.domainStates[domain];
    const topologyState = topologyStateFor(state);
    const scopedLane = transformValue(lane, "lane", scenario.family, scenario, domain) as Record<
      string,
      unknown
    >;

    return {
      ...scopedLane,
      diagnostics: diagnosticForState(
        (lane.diagnostics ?? {}) as Record<string, unknown>,
        state,
        domain,
      ),
      state: topologyState,
      top_scoped_resources:
        state === "idle"
          ? []
          : ((scopedLane.top_scoped_resources ?? []) as Record<string, unknown>[]).map(
              (resource) => ({ ...resource, state: topologyState }),
            ),
    };
  });
  const sourceConnections = (transformed.connections ?? {}) as Record<string, unknown>;
  const connectionItems = Array.isArray(sourceConnections.items)
    ? sourceConnections.items.map((entry) => {
        const connection = entry as Record<string, unknown>;
        const source = typeof connection.source === "string" ? connection.source : "";
        const domain = domains.find((candidate) => source === `domain:${candidate}`);

        return {
          ...connection,
          state: topologyStateFor(stateForDomain(scenario, domain)),
        };
      })
    : [];
  const connections =
    scenario.state === "idle"
      ? { items: [], limit: 20, total: 0, truncated: false }
      : {
          ...sourceConnections,
          items: connectionItems,
          total: connectionItems.length,
          truncated: false,
        };

  return {
    ...transformed,
    connections,
    diagnostics: globalDiagnostics((body.diagnostics ?? {}) as Record<string, unknown>, scenario),
    lanes,
    session_groups:
      scenario.state === "idle"
        ? []
        : [
            {
              max_idle_seconds: scenario.state === "chaos" ? 133 : 21,
              messages_received: scenario.state === "chaos" ? 91_000 : 4_200,
              messages_sent: scenario.state === "chaos" ? 144_000 : 5_600,
              representative_sessions: scopedSessions.slice(0, 3),
              route_family: scenario.family,
              sessions: scopedSessions.length,
              transports: ["ws", "tcp"],
            },
          ],
  };
}

function metricsForScenario(body: Record<string, unknown>, scenario: FamilyScenario) {
  const samples = Array.isArray(body.samples) ? body.samples : [];

  return {
    ...body,
    family: scenario.family,
    samples: samples.map((entry) => {
      const sample = entry as Record<string, unknown>;
      const name = typeof sample.name === "string" ? sample.name : "";
      const domain = domains.find((candidate) => name.includes(`_${candidate}_`));
      const state = stateForDomain(scenario, domain);

      return {
        ...sample,
        labels: { ...((sample.labels ?? {}) as object), family: scenario.family.toString() },
        value:
          typeof sample.value === "number" ? scaleNumber(sample.value, name, state) : sample.value,
      };
    }),
  };
}

function domainBodyForScenario(
  body: Record<string, unknown>,
  scenario: FamilyScenario,
  domain: Domain,
  path: string,
) {
  const state = scenario.domainStates[domain];
  const transformed = transformValue(body, domain, scenario.family, scenario, domain) as Record<
    string,
    unknown
  >;

  if (state === "idle") {
    return transformed;
  }

  if (state === "healthy") {
    if (path.endsWith("/dead-letters")) return { ...transformed, messages: [] };
    if (path.endsWith("/pending")) return { ...transformed, requests: [] };
    if (path.endsWith("/missed")) return { ...transformed, observations: [] };
    if (path.endsWith("/calls") && Array.isArray(transformed.observations)) {
      return {
        ...transformed,
        observations: transformed.observations.filter(
          (entry) => (entry as Record<string, unknown>).state !== "pending",
        ),
      };
    }
  }

  return transformed;
}

export function applyFamilyScenario(
  response: MockResponse,
  family: number,
  path: string,
  domain?: Domain,
) {
  if (!response.headers["content-type"]?.includes("application/json") || response.body === "") {
    return response;
  }

  let body: unknown;
  try {
    body = JSON.parse(response.body);
  } catch {
    return response;
  }

  if (!body || typeof body !== "object" || Array.isArray(body)) return response;

  const scenario = scenarioForFamily(family);
  const object = body as Record<string, unknown>;
  let scenarioBody: Record<string, unknown>;

  if (path.endsWith("/stats") && !domain) {
    scenarioBody = statsForScenario(object, scenario);
  } else if (path.endsWith("/topology")) {
    scenarioBody = topologyForScenario(object, scenario);
  } else if (path.endsWith("/troubleshooting")) {
    scenarioBody = globalDiagnostics(object, scenario);
  } else if (path.endsWith("/sessions")) {
    scenarioBody = sessionsForScenario(object, scenario);
  } else if (path.endsWith("/metrics")) {
    scenarioBody = metricsForScenario(object, scenario);
  } else if (domain) {
    scenarioBody = domainBodyForScenario(object, scenario, domain, path);
  } else {
    scenarioBody = transformValue(object, "response", scenario.family, scenario) as Record<
      string,
      unknown
    >;
    if (scenario.state === "idle" && Array.isArray(scenarioBody.results)) {
      scenarioBody = { ...scenarioBody, results: [] };
    }
  }

  return json(scenarioBody, response.status);
}
