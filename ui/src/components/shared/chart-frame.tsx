export interface ChartShellProps {
  children?: unknown;
  className?: string;
  description?: string;
  title: string;
}

export interface ChartPanelProps {
  children?: unknown;
  description?: string;
  title: string;
}

export function ChartShell({ children, className, description, title }: ChartShellProps) {
  return (
    <section class={className ? `chart-shell ${className}` : "chart-shell"}>
      <header class="chart-shell-header">
        <h2>{title}</h2>
        {description ? <p>{description}</p> : null}
      </header>
      {children}
    </section>
  );
}

export function ChartPanel({ children, description, title }: ChartPanelProps) {
  return (
    <section class="chart-panel">
      <header class="chart-panel-header">
        <h3>{title}</h3>
        {description ? <p>{description}</p> : null}
      </header>
      {children}
    </section>
  );
}