import ChartMeter from "@/components/shared/chart-meter";
import { ChartPanel, ChartShell } from "@/components/shared/chart-frame";

type ChartValueFormatter = (value: number) => string;

type ValueChartDatumObject = {
  description?: string;
  label: string;
  value: number;
};

type ValueChartDatumTuple = readonly [string, number?, unknown?, string?];

type ValueChartDatumInput = ValueChartDatumObject | ValueChartDatumTuple;

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

    const objectEntry = entry as ValueChartDatumObject;

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
