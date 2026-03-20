import type { BaseProps } from "./shared";

export interface FieldBaseProps extends BaseProps {
  fieldId?: string;
  invalid?: boolean;
  required?: boolean;
  disabled?: boolean;
  asChild?: boolean;
}

export function Field(props: BaseProps): JSX.Element;
export function FieldLabel(props: FieldBaseProps): JSX.Element;
export function FieldDescription(props: FieldBaseProps): JSX.Element;
export function FieldError(props: FieldBaseProps): JSX.Element;
export function FieldControl(props: FieldBaseProps): JSX.Element;
