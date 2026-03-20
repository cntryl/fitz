export function Field(props) {
  return <div data-slot="field" {...props} />;
}

export function FieldLabel(props) {
  const { fieldId, ...rest } = props;
  return <label data-slot="field-label" for={`${fieldId}-control`} {...rest} />;
}

export function FieldDescription(props) {
  const { fieldId, ...rest } = props;
  return <div data-slot="field-description" id={`${fieldId}-description`} {...rest} />;
}

export function FieldError(props) {
  const { fieldId, ...rest } = props;
  return (
    <div data-slot="field-error" id={`${fieldId}-error`} role="alert" {...rest} />
  );
}

export function FieldControl(props) {
  const { asChild, children, fieldId, invalid, required, disabled, ...rest } = props;

  if (asChild && children && typeof children === "object") {
    const child = children;
    const nextProps = {
      ...child.props,
      ...rest,
      id: `${fieldId}-control`,
      "aria-describedby": invalid
        ? `${fieldId}-description ${fieldId}-error`
        : `${fieldId}-description`,
      "aria-invalid": invalid ? "true" : undefined,
      "aria-required": required ? "true" : undefined,
      "aria-disabled": disabled ? "true" : undefined,
      disabled,
    };
    return { ...child, props: nextProps };
  }

  return (
    <div data-slot="field-control" id={`${fieldId}-control`} {...rest}>
      {children}
    </div>
  );
}
