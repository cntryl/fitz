function mergeStyle(base, style) {
  if (!style) return base;
  if (typeof style === "string") return `${base}; ${style}`;
  return { ...base, ...style };
}

export function Stack(props) {
  const { align, children, gap, justify, style, wrap, ...rest } = props;

  const layout = {
    display: "flex",
    flexDirection: "column",
    gap,
    alignItems: align,
    justifyContent: justify,
    flexWrap: wrap,
  };

  return (
    <div data-slot="stack" style={mergeStyle(layout, style)} {...rest}>
      {children}
    </div>
  );
}
