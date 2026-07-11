export interface MetricSample {
  labels: Record<string, string>;
  name: string;
  value: number;
}

export interface MetricFamily {
  help?: string;
  name: string;
  samples: MetricSample[];
  type?: string;
}

export interface MetricsOverview {
  families: MetricFamily[];
  generatedAt: number;
  raw: string;
  scope: "all" | "family";
}
