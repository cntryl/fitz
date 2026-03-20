import type { BaseProps } from "./shared";

export interface StackProps extends BaseProps {
  align?: string;
  gap?: string;
  justify?: string;
  wrap?: string;
}

export function Stack(props: StackProps): JSX.Element;
