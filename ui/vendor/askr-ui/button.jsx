export function Button(props) {
  const { asChild, children, onPress, type, ...rest } = props;

  if (asChild && children && typeof children === "object") {
    const child = children;
    const nextProps = {
      ...child.props,
      ...rest,
      "data-slot": "button",
      onClick: onPress ?? child.props?.onClick,
    };
    return { ...child, props: nextProps };
  }

  return (
    <button data-slot="button" type={type ?? "button"} onClick={onPress} {...rest}>
      {children}
    </button>
  );
}
