package iter

import (
	"context"
	"sync"
)

// ChannelIterator is a generic iterator backed by a channel of items and a
// separate error channel. It implements Iterator[T] and is suitable for any
// domain that streams results from a background goroutine.
//
// Usage:
//
//	items := make(chan T, bufSize)
//	errCh := make(chan error, 1)
//	ctx, cancel := context.WithCancel(parentCtx)
//	go producer(items, errCh) // writes items, then closes items, then sends error
//	it := iter.NewChannelIterator(items, errCh, cancel)
//	defer it.Close()
//	for it.Next() { use(it.Value()) }
//	if err := it.Err(); err != nil { handle(err) }
type ChannelIterator[T any] struct {
	items     <-chan T
	errCh     <-chan error
	curr      T
	err       error
	cancel    context.CancelFunc
	closeOnce sync.Once
}

// NewChannelIterator creates a ChannelIterator.
// cancel is called on Close() to signal the producer to stop.
func NewChannelIterator[T any](items <-chan T, errCh <-chan error, cancel context.CancelFunc) *ChannelIterator[T] {
	return &ChannelIterator[T]{items: items, errCh: errCh, cancel: cancel}
}

func (it *ChannelIterator[T]) Next() bool {
	v, ok := <-it.items
	if !ok {
		if it.err == nil {
			// Wait for producer error, but don't block forever if producer crashed.
			select {
			case e := <-it.errCh:
				it.err = e
			default:
				// Producer didn't send an error yet; this can happen if
				// the items channel was closed without an errCh write.
			}
		}
		return false
	}
	it.curr = v
	return true
}

func (it *ChannelIterator[T]) Value() T { return it.curr }

func (it *ChannelIterator[T]) Err() error { return it.err }

func (it *ChannelIterator[T]) Close() error {
	it.closeOnce.Do(func() {
		if it.cancel != nil {
			it.cancel()
		}
	})
	// drain error channel to prevent goroutine leak
	select {
	case e := <-it.errCh:
		if it.err == nil {
			it.err = e
		}
	default:
	}
	return nil
}
