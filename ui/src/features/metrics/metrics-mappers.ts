import type { MetricsOverview, PrometheusMetricFamily, PrometheusSample } from "./metrics-models";

function parseLabels(input: string | undefined) {
  if (!input) return {};

  return Object.fromEntries(
    input
      .split(",")
      .map((part) => part.trim())
      .filter(Boolean)
      .map((part) => {
        const [key, ...valueParts] = part.split("=");
        return [key, valueParts.join("=").replace(/^"|"$/g, "")];
      }),
  );
}

function ensureFamily(families: Map<string, PrometheusMetricFamily>, name: string) {
  const baseName = name.replace(/_(bucket|sum|count)$/, "");
  const existing = families.get(baseName);

  if (existing) return existing;

  const family: PrometheusMetricFamily = { name: baseName, samples: [] };
  families.set(baseName, family);
  return family;
}

export function parsePrometheusMetrics(raw: string): MetricsOverview {
  const families = new Map<string, PrometheusMetricFamily>();

  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    if (trimmed.startsWith("# HELP ")) {
      const [, name, help] = trimmed.match(/^# HELP\s+(\S+)\s+(.+)$/) ?? [];
      if (name) ensureFamily(families, name).help = help;
      continue;
    }

    if (trimmed.startsWith("# TYPE ")) {
      const [, name, type] = trimmed.match(/^# TYPE\s+(\S+)\s+(.+)$/) ?? [];
      if (name) ensureFamily(families, name).type = type;
      continue;
    }

    if (trimmed.startsWith("#")) continue;

    const match = trimmed.match(/^([^{\s]+)(?:\{([^}]*)\})?\s+(-?\d+(?:\.\d+)?(?:e[+-]?\d+)?)$/i);
    if (!match) continue;

    const [, name, labels, value] = match;
    const sample: PrometheusSample = {
      labels: parseLabels(labels),
      name,
      value: Number(value),
    };
    ensureFamily(families, name).samples.push(sample);
  }

  return {
    families: [...families.values()].sort((left, right) => left.name.localeCompare(right.name)),
    raw,
  };
}
