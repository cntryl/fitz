package iter

// SliceIterator iterates over an in-memory slice.
// Used for batch results like KV SCAN where all items arrive in one response.
// Thread-safe for single-threaded iteration (no concurrent Next() calls).
type SliceIterator[T any] struct {
	items []T
	index int
}

// NewSliceIterator creates an iterator over the provided slice.
// The slice is not copied, so modifications to the underlying slice
// during iteration will be visible.
func NewSliceIterator[T any](items []T) *SliceIterator[T] {
	return &SliceIterator[T]{
		items: items,
		index: -1, // Start before first element
	}
}

// Next advances the iterator and returns true if a value is available.
func (it *SliceIterator[T]) Next() bool {
	it.index++
	return it.index < len(it.items)
}

// Value returns the current item (valid only after a successful Next()).
// Panics if called before Next() or after Next() returns false.
func (it *SliceIterator[T]) Value() T {
	if it.index < 0 || it.index >= len(it.items) {
		panic("SliceIterator.Value() called in invalid state")
	}
	return it.items[it.index]
}

// Err returns the first non-EOF error encountered.
// Slice iteration never produces errors, so this always returns nil.
func (it *SliceIterator[T]) Err() error {
	return nil
}

// Close releases any resources associated with the iterator.
// SliceIterator has no resources to release, so this is a no-op.
// Safe to call multiple times.
func (it *SliceIterator[T]) Close() error {
	return nil
}
