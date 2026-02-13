package connection

import (
	"sync"
)

// ByteSlicePool manages reusable byte slices for response values.
// This reduces allocations in hot paths like KV Get operations.
type ByteSlicePool struct {
	pool sync.Pool
}

// NewByteSlicePool creates a new byte slice pool.
func NewByteSlicePool() *ByteSlicePool {
	return &ByteSlicePool{
		pool: sync.Pool{
			New: func() interface{} {
				// Start with 1KB default capacity
				b := make([]byte, 0, 1024)
				return &b
			},
		},
	}
}

// Get retrieves a byte slice from the pool.
// The slice is reset to length 0 but retains capacity.
// Caller should use append() to fill it.
func (p *ByteSlicePool) Get() *[]byte {
	return p.pool.Get().(*[]byte)
}

// Put returns a byte slice to the pool.
// Don't return oversized slices to prevent memory bloat.
func (p *ByteSlicePool) Put(b *[]byte) {
	if b == nil {
		return
	}
	// Don't pool slices larger than 64KB
	if cap(*b) > 64*1024 {
		return
	}
	// Reset length but keep capacity
	*b = (*b)[:0]
	p.pool.Put(b)
}

// GetWithCapacity retrieves a byte slice with at least the requested capacity.
// If pool slice is too small, allocates a new one.
func (p *ByteSlicePool) GetWithCapacity(minCap int) *[]byte {
	b := p.Get()
	if cap(*b) < minCap {
		// Need larger slice, allocate directly
		newSlice := make([]byte, 0, minCap)
		return &newSlice
	}
	return b
}

// Global byte slice pool for response values
var responseValuePool = NewByteSlicePool()

// GetResponseValue gets a pooled byte slice for response values.
// Caller MUST call PutResponseValue when done to return to pool.
func GetResponseValue() *[]byte {
	return responseValuePool.Get()
}

// GetResponseValueWithCapacity gets a pooled byte slice with minimum capacity.
// Caller MUST call PutResponseValue when done to return to pool.
func GetResponseValueWithCapacity(minCap int) *[]byte {
	return responseValuePool.GetWithCapacity(minCap)
}

// PutResponseValue returns a byte slice to the response value pool.
func PutResponseValue(b *[]byte) {
	responseValuePool.Put(b)
}
