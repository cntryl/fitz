package encoding

import (
	"bytes"

	"github.com/cntryl/fitz-go/internal/core/connection"
)

// OwnedBuffer wraps a pooled buffer with explicit release semantics.
// Call Release() when the bytes are no longer needed.
type OwnedBuffer struct {
	buf *bytes.Buffer
}

// Bytes returns the underlying buffer bytes.
func (o *OwnedBuffer) Bytes() []byte {
	if o == nil || o.buf == nil {
		return nil
	}
	return o.buf.Bytes()
}

// Len returns the length of the buffer bytes.
func (o *OwnedBuffer) Len() int {
	if o == nil || o.buf == nil {
		return 0
	}
	return o.buf.Len()
}

// Release returns the buffer to the pool.
func (o *OwnedBuffer) Release() {
	if o == nil || o.buf == nil {
		return
	}
	connection.PutBuffer(o.buf)
	o.buf = nil
}

// EncodeWithBuffer provides a standard pattern for using buffer pools.
// It handles Get/Put lifecycle and copies the result safely.
//
// Usage:
//
//	return EncodeWithBuffer(func(buf *bytes.Buffer) {
//	    WriteU64(buf, txID)
//	    WriteRoute(buf, route)
//	    WriteBytes(buf, key)
//	})
func EncodeWithBuffer(fn func(*bytes.Buffer)) []byte {
	owned := EncodeWithBufferOwned(fn)
	if owned == nil {
		return nil
	}
	defer owned.Release()

	result := make([]byte, owned.Len())
	copy(result, owned.Bytes())
	return result
}

// EncodeWithBufferOwned provides a pooled buffer with explicit release.
func EncodeWithBufferOwned(fn func(*bytes.Buffer)) *OwnedBuffer {
	buf := connection.GetBuffer()
	fn(buf)
	return &OwnedBuffer{buf: buf}
}

// WriteU64 writes a uint64 in big-endian format.
func WriteU64(buf *bytes.Buffer, v uint64) {
	connection.WriteU64BE(buf, v)
}

// WriteU32 writes a uint32 in big-endian format.
func WriteU32(buf *bytes.Buffer, v uint32) {
	connection.WriteU32BE(buf, v)
}

// WriteOptionalU64 writes an optional uint64: [u8 has_value][u64 value if has_value=1].
// Per payload_codec.rs: flag byte (1 = Some, 0 = None), then 8 bytes if present.
func WriteOptionalU64(buf *bytes.Buffer, v uint64) {
	if v == 0 {
		// Treat 0 as not provided (matches server default behavior)
		buf.WriteByte(0)
	} else {
		buf.WriteByte(1)
		connection.WriteU64BE(buf, v)
	}
}

// WriteString writes a length-prefixed string: [u32 length][bytes].
// This is the most common pattern across all domains.
func WriteString(buf *bytes.Buffer, s string) {
	connection.WriteU32BE(buf, uint32(len(s)))
	buf.WriteString(s)
}

// WriteBytes writes length-prefixed bytes: [u32 length][bytes].
// This is the standard pattern for keys, values, and payloads.
func WriteBytes(buf *bytes.Buffer, data []byte) {
	connection.WriteU32BE(buf, uint32(len(data)))
	buf.Write(data)
}

// WriteRoute is an alias for WriteString, semantically indicating route encoding.
// Routes are always encoded as [u32 length][bytes].
func WriteRoute(buf *bytes.Buffer, route string) {
	WriteString(buf, route)
}

// WriteBytesRaw writes bytes without length prefix.
// Use this when the length is implicit or encoded separately.
func WriteBytesRaw(buf *bytes.Buffer, data []byte) {
	buf.Write(data)
}
