import "@askrjs/askr/jsx-runtime";

export type Children = unknown;

export interface BaseProps {
  children?: Children;
  class?: string;
  style?: string | Record<string, string | number>;
  ref?: unknown;
  [key: string]: unknown;
}
