package encoding

import (
	"bytes"

	"github.com/cntryl/fitz-go/internal/core/connection"
)

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
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	fn(buf)

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result
}

// WriteU64 writes a uint64 in big-endian format.
func WriteU64(buf *bytes.Buffer, v uint64) {
	connection.WriteU64BE(buf, v)
}

// WriteU32 writes a uint32 in big-endian format.
func WriteU32(buf *bytes.Buffer, v uint32) {
	connection.WriteU32BE(buf, v)
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
