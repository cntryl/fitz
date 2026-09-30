export interface TitledCellProps {
  children: unknown;
  subtitle?: string | null;
  /** Full text on hover when the title is clamped. */
  title?: string;
}

/**
 * A row's name with its explanation underneath. Prose belongs here rather than in
 * its own column, so tables stay narrow enough to never scroll sideways.
 */
export default function TitledCell({ children, subtitle, title }: TitledCellProps) {
  return (
    <span class="titled-cell">
      <span class="titled-cell-title" title={title}>
        {children}
      </span>
      {subtitle ? <span class="titled-cell-subtitle">{subtitle}</span> : null}
    </span>
  );
}
