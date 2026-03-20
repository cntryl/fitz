import type { BaseProps } from "./shared";

export interface InputProps extends BaseProps {
  autocomplete?: string;
  name?: string;
  placeholder?: string;
  type?: string;
  value?: string;
  onInput?: (event: Event) => void;
}

export function Input(props: InputProps): JSX.Element;
