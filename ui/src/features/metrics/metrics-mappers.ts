import type { StructuredMetricsResponse } from "@/adapters";
import type { MetricFamily, MetricsOverview } from "./metrics-models";

function ensureFamily(
  families: Map<string, MetricFamily>,
  name: string,
  help: string,
  type: string,
) {
  const baseName = name.replace(/_(bucket|sum|count)$/, "");
  const existing = families.get(baseName);

  if (existing) {
    existing.help ??= help;
    existing.type ??= type;
    return existing;
  }

  const family: MetricFamily = { help, name: baseName, samples: [], type };
  families.set(baseName, family);
  return family;
}

export function mapStructuredMetrics(data: StructuredMetricsResponse): MetricsOverview {
  const families = new Map<string, MetricFamily>();

  for (const sample of data.samples) {
    ensureFamily(families, sample.name, sample.help, sample.kind).samples.push({
      labels: sample.labels,
      name: sample.name,
      value: sample.value,
    });
  }

  const raw = JSON.stringify(data, null, 2);
  return {
    families: [...families.values()].sort((left, right) => left.name.localeCompare(right.name)),
    generatedAt: data.generated_at,
    raw,
    scope: data.scope,
  };
}
