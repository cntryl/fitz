declare module "@askrjs/askr/jsx-runtime" {
  export const Fragment: unique symbol;
  export function jsx(type: unknown, props: Record<string, unknown>): unknown;
  export function jsxs(type: unknown, props: Record<string, unknown>): unknown;
}

declare module "@askrjs/askr/jsx-dev-runtime" {
  export const Fragment: unique symbol;
  export function jsxDEV(
    type: unknown,
    props: Record<string, unknown>,
  ): unknown;
}

declare global {
  namespace JSX {
    interface IntrinsicElements {
      [elemName: string]: Record<string, unknown>;
    }
  }
}

export {};
