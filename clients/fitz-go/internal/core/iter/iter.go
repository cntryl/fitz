package iter

// Iterator is a generic streaming iterator used across the codebase.
//
// Usage pattern (manual):
//
//	it, err := tx.Scan(ctx, prefix, 100)
//	if err != nil { /* handle */ }
//	defer it.Close()
//	for it.Next() {
//	    v := it.Value()
//	    // use v
//	}
//	if err := it.Err(); err != nil { /* handle */ }
//
// Usage pattern (with ForEach helper):
//
//	it, err := tx.Scan(ctx, prefix, 100)
//	if err != nil { /* handle */ }
//	return iter.ForEach(it, func(v T) error {
//	    // use v
//	    return nil
//	})
//
// Implementations MUST ensure Close() releases any underlying resources.
type Iterator[T any] interface {
	// Next advances the iterator and returns true if a value is available.
	Next() bool
	// Value returns the current item (valid only after a successful Next()).
	Value() T
	// Err returns the first non-EOF error encountered.
	Err() error
	// Close releases any resources associated with the iterator.
	Close() error
}

// ForEach iterates over all items in the iterator, automatically handling
// Close() and error checking. The callback fn is called for each item.
// Iteration stops on first error from either the callback or the iterator.
//
// Returns the first error encountered (callback error takes precedence),
// or nil if iteration completes successfully.
//
// Example:
//
//	it, err := tx.Scan(ctx, startKey, endKey, 100)
//	if err != nil { return err }
//	return iter.ForEach(it, func(kv KVPair) error {
//	    fmt.Printf("%s: %s\n", kv.Key, kv.Value)
//	    return nil
//	})
func ForEach[T any](it Iterator[T], fn func(T) error) error {
	defer it.Close()
	for it.Next() {
		if err := fn(it.Value()); err != nil {
			return err
		}
	}
	return it.Err()
}
