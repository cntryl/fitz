import type { BaseProps } from "./shared";

export interface ContainerProps extends BaseProps {
  centered?: boolean;
  fluid?: boolean;
  maxWidth?: string;
  padding?: string;
}

export function Container(props: ContainerProps): JSX.Element;
