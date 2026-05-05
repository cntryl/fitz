import type { Props } from "@askrjs/askr";

declare global {
  namespace JSX {
    interface Element {
      $$typeof: symbol;
      type: string | ((props: Props) => unknown) | symbol;
      props: Props;
      key?: string | number | null;
    }

    interface IntrinsicElements {
      [elem: string]: Props;
    }

    interface ElementAttributesProperty {
      props: Props;
    }

    interface ElementChildrenAttribute {
      children: unknown;
    }
  }
}

export {};
