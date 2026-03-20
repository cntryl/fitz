function mergeStyle(base, style) {
  if (!style) return base;
  if (typeof style === "string") return `${base}; ${style}`;
  return { ...base, ...style };
}

export function Container(props) {
  const {
    centered = true,
    children,
    fluid = true,
    maxWidth,
    padding,
    style,
    ...rest
  } = props;

  const layout = {
    boxSizing: "border-box",
    width: fluid ? "100%" : undefined,
    maxWidth,
    paddingInline: padding,
    marginInline: centered ? "auto" : undefined,
  };

  return (
    <div data-slot="container" style={mergeStyle(layout, style)} {...rest}>
      {children}
    </div>
  );
}
