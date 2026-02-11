package iter

// Iterator is a generic streaming iterator used across the codebase.
// Usage pattern:
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
