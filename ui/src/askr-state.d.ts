import type { State } from "@askrjs/askr";

declare module "@askrjs/askr" {
  type StateSetter<T> = State<T>["set"];

  function state<T>(initialValue: T): [get: State<T>, set: StateSetter<T>];
}
