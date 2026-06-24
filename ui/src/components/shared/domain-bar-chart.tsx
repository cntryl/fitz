import { BarChart, ChartPanel, ChartShell } from "@askrjs/charts/components";

type ChartValueFormatter = (value: number) => string;

type ValueChartDatumObject = {
  description?: string;
  label: string;
  unitLabel?: string;
  value: number;
};

type ValueChartDatumTuple = readonly [string, number?, unknown?, string?];

type ValueChartDatumInput = ValueChartDatumObject | ValueChartDatumTuple;

export interface DomainBarChartProps {
  data: ValueChartDatumInput[];
  description: string;
  label: string;
  scope?: string;
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
      unitLabel: objectEntry.unitLabel,
      value: objectEntry.value,
    };
  });
}

export default function DomainBarChart({
  data,
  description,
  label,
  scope,
  title,
  valueFormatter,
}: DomainBarChartProps) {
  const normalized = normalizeData(data);
  const max = Math.max(1, ...normalized.map((entry) => entry.value));

  return (
    <ChartShell className="domain-chart-shell" title={title} description={description}>
      <ChartPanel title={label} description={scope}>
        <BarChart
          label={label}
          data={normalized.map((entry) => ({
            description: entry.description ?? entry.unitLabel,
            label: entry.label,
            value: entry.value,
          }))}
          max={max}
          summary={description}
          valueFormatter={valueFormatter}
        />
      </ChartPanel>
    </ChartShell>
  );
}
