function icon(pathD) {
  return function Icon(props) {
    const { size = 20, strokeWidth = 2, class: className, title, ...rest } = props ?? {};
    return (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width={size}
        height={size}
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        stroke-width={strokeWidth}
        stroke-linecap="round"
        stroke-linejoin="round"
        class={className}
        aria-hidden={title ? undefined : "true"}
        role={title ? "img" : undefined}
        {...rest}
      >
        {title ? <title>{title}</title> : null}
        <path d={pathD} />
      </svg>
    );
  };
}

export const Activity = icon("M22 12h-4l-3 9-6-18-3 9H2");
export const ArrowRight = icon("M5 12h14M13 5l7 7-7 7");
export const Gauge = icon("m12 14 4-4M3.34 17a10 10 0 1 1 17.32 0");
export const LockKeyhole = icon("M7 10V7a5 5 0 0 1 10 0v3M5 10h14v10H5zM12 14v2");
export const LogOut = icon("M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4M16 17l5-5-5-5M21 12H9");
export const Shield = icon("M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10");
export const ShieldCheck = icon("M9 12l2 2 4-4M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10");
