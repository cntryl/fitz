import { formatNumber } from "@/shared/format";

export interface ChartMeterProps {
  description?: string;
  label: string;
  max: number;
  value: number;
  valueFormatter?: (value: number) => string;
}

export default function ChartMeter({
  description,
  label,
  max,
  value,
  valueFormatter = formatNumber,
}: ChartMeterProps) {
  const normalizedMax = Math.max(1, max);
  const normalizedValue = Math.max(0, Math.min(value, normalizedMax));
  const percentage = Math.round((normalizedValue / normalizedMax) * 100);

  return (
    <section class="chart-meter" aria-label={label}>
      <div class="chart-meter-header">
        <span class="chart-meter-label">{label}</span>
        <span class="chart-meter-value">
          {valueFormatter(normalizedValue)} / {valueFormatter(normalizedMax)}
        </span>
      </div>

      <div
        class="chart-meter-track"
        role="meter"
        aria-label={label}
        aria-valuemin={0}
        aria-valuemax={normalizedMax}
        aria-valuenow={normalizedValue}
        aria-valuetext={`${percentage}%`}
      >
        <span class="chart-meter-fill" style={{ width: `${percentage}%` }} />
      </div>

      {description ? <p class="chart-meter-description">{description}</p> : null}
    </section>
  );
}
