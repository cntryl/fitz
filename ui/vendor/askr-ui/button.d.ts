import type { BaseProps } from "./shared";

export interface ButtonProps extends BaseProps {
  asChild?: boolean;
  onPress?: (event: Event) => void | Promise<void>;
  type?: "button" | "submit" | "reset";
}

export function Button(props: ButtonProps): JSX.Element;
