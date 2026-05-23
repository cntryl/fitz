import { ChartPanel, ChartShell } from "@askrjs/charts/components";
import type { ChartValueFormatter, ValueChartDatumInput } from "@askrjs/charts/core";
import ChartMeter from "@/components/shared/chart-meter";

export interface DomainBarChartProps {
  data: ValueChartDatumInput[];
  description: string;
  label: string;
  title: string;
  valueFormatter?: ChartValueFormatter;
}

function normalizeData(data: ValueChartDatumInput[]) {
  return data.map((entry) => {
    if (Array.isArray(entry)) {
      return {
        description: entry[3],
        label: entry[0],
        value: entry[1] ?? 0,
      };
    }

    const objectEntry = entry as Exclude<ValueChartDatumInput, readonly unknown[]>;

    return {
      description: objectEntry.description,
      label: objectEntry.label,
      value: objectEntry.value,
    };
  });
}

export default function DomainBarChart({
  data,
  description,
  label,
  title,
  valueFormatter,
}: DomainBarChartProps) {
  const normalized = normalizeData(data);
  const max = Math.max(1, ...normalized.map((entry) => entry.value));

  return (
    <ChartShell className="domain-chart-shell" title={title} description={description}>
      <ChartPanel title={label}>
        <div class="chart-meter-grid">
          {normalized.map((entry) => (
            <div key={entry.label}>
              <ChartMeter
                label={entry.label}
                value={entry.value}
                max={max}
                description={entry.description}
                valueFormatter={valueFormatter}
              />
            </div>
          ))}
        </div>
      </ChartPanel>
    </ChartShell>
  );
}
