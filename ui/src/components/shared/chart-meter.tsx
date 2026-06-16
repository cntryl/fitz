import { formatNumber } from "@/shared/format";

export interface ChartMeterProps {
  description?: string;
  label: string;
  max: number;
  unitLabel?: string;
  value: number;
  valueFormatter?: (value: number) => string;
}

export default function ChartMeter({
  description,
  label,
  max,
  unitLabel,
  value,
  valueFormatter = formatNumber,
}: ChartMeterProps) {
  const normalizedMax = Math.max(1, max);
  const normalizedValue = Math.max(0, Math.min(value, normalizedMax));
  const percentage = Math.round((normalizedValue / normalizedMax) * 100);
  const valueText = valueFormatter(normalizedValue);
  const maxText = valueFormatter(normalizedMax);
  const unitSuffix = unitLabel ? ` ${unitLabel}` : "";

  return (
    <section class="chart-meter" aria-label={label}>
      <div class="chart-meter-header">
        <span class="chart-meter-label">{label}</span>
        <span class="chart-meter-value">
          {valueText}
          {unitSuffix} / {maxText}
          {unitSuffix}
        </span>
      </div>

      <div
        class="chart-meter-track"
        role="meter"
        aria-label={label}
        aria-valuemin={0}
        aria-valuemax={normalizedMax}
        aria-valuenow={normalizedValue}
        aria-valuetext={`${valueText}${unitSuffix} of ${maxText}${unitSuffix} (${percentage}%)`}
      >
        <span class="chart-meter-fill" style={{ width: `${percentage}%` }} />
      </div>

      {description ? <p class="chart-meter-description">{description}</p> : null}
    </section>
  );
}
