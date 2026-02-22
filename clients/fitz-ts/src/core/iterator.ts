/**
 * Iterator[T] is a generic streaming iterator modeled after fitz-go's Iterator[T].
 *
 * Usage pattern (manual):
 * ```ts
 * const it = await tx.scan(prefix, 100);
 * try {
 *   while (it.next()) {
 *     const value = it.value();
 *     // use value
 *   }
 *   if (it.err()) throw it.err();
 * } finally {
 *   it.close();
 * }
 * ```
 *
 * Usage pattern (with forEach helper):
 * ```ts
 * const it = await tx.scan(prefix, 100);
 * return forEach(it, (value) => {
 *   // use value
 *   return Promise.resolve(undefined);
 * });
 * ```
 */
export interface Iterator<T> {
  /**
   * Advances the iterator and returns true if a value is available.
   */
  next(): boolean;

  /**
   * Returns the current item (valid only after a successful next()).
   */
  value(): T;

  /**
   * Returns the first non-EOF error encountered.
   */
  err(): Error | null;

  /**
   * Closes/releases any resources associated with the iterator.
   */
  close(): void;
}

/**
 * SliceIterator iterates over an in-memory slice/array.
 * Used for batch results like KV SCAN where all items arrive in one response.
 */
export class SliceIterator<T> implements Iterator<T> {
  private items: T[];
  private index: number = -1;

  constructor(items: T[]) {
    this.items = items;
  }

  next(): boolean {
    this.index++;
    return this.index < this.items.length;
  }

  value(): T {
    if (this.index < 0 || this.index >= this.items.length) {
      throw new Error("SliceIterator.value() called in invalid state");
    }
    return this.items[this.index];
  }

  err(): Error | null {
    return null; // Slice iteration never produces errors
  }

  close(): void {
    // No-op: no resources to release
  }
}

/**
 * AsyncIterableIterator is a JavaScript
 * AsyncIterable/AsyncIterator that wraps an Iterator[T].
 * Useful for for-await-of loops in TypeScript.
 */
export class AsyncIterableIterator<T> implements AsyncIterable<T> {
  constructor(private iterator: Iterator<T>) {}

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: async () => {
        if (this.iterator.next()) {
          return {
            done: false,
            value: this.iterator.value(),
          };
        }
        const err = this.iterator.err();
        this.iterator.close();
        if (err) {
          throw err;
        }
        return { done: true, value: undefined };
      },
    };
  }
}

/**
 * forEach iterates over all items in the iterator, calling fn for each.
 * Automatically handles close() and error checking.
 * Iteration stops on first error from either callback or iterator.
 *
 * Example:
 * ```ts
 * const it = await tx.scan(startKey, 100);
 * return forEach(it, async (kv) => {
 *   console.log(`${kv.key}: ${kv.value}`);
 * });
 * ```
 */
export async function forEach<T>(
  it: Iterator<T>,
  fn: (item: T) => Promise<void>,
): Promise<void> {
  try {
    while (it.next()) {
      await fn(it.value());
    }
    const err = it.err();
    if (err) {
      throw err;
    }
  } finally {
    it.close();
  }
}
