export interface PrometheusSample {
  labels: Record<string, string>;
  name: string;
  value: number;
}

export interface PrometheusMetricFamily {
  help?: string;
  name: string;
  samples: PrometheusSample[];
  type?: string;
}

export interface MetricsOverview {
  families: PrometheusMetricFamily[];
  raw: string;
}
