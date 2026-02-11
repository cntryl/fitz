package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// TLV layer invariants:
// - Duplicate tags are NOT permitted within a single frame (per CLIENT_SPEC.md rule 5).
//   NewTLVDecoder returns an error if a tag appears more than once.
// - Each TLV value length is encoded as a 2-byte big-endian uint16, which limits a single
//   TLV value to 65535 bytes (64 KiB).
// - Unknown tags are accepted and preserved (forward-compatibility).

// MaxTLVValueLen is the maximum allowed length for a single TLV value (2-byte length field).
const MaxTLVValueLen uint16 = 65535

// TLV tag constants (canonical from CLIENT_SPEC.md).
const (
	TagToken          uint8 = 0x10
	TagOp             uint8 = 0x11 // operation tag used by request frames
	TagRoute          uint8 = 0x20
	TagID             uint8 = 0x21
	TagBody           uint8 = 0x22
	TagErr            uint8 = 0x3F
	TagKey            uint8 = 0x23
	TagValue          uint8 = 0x24
	TagStartKey       uint8 = 0x25
	TagEndKey         uint8 = 0x26
	TagLimit          uint8 = 0x27
	TagFrom           uint8 = 0x28
	TagExpectedOffset uint8 = 0x29
	TagLease          uint8 = 0x2A
	TagBatchSize      uint8 = 0x2B
	TagDeliveryToken  uint8 = 0x2C
	TagTTL            uint8 = 0x2D
	TagReplyRoute     uint8 = 0x2E
	TagSeq            uint8 = 0x30
	TagStreamEnd      uint8 = 0x31
	TagCron           uint8 = 0x32
)

// KV operation type constants (mapped in KV domain).
const (
	KVOpGet         uint8 = 1
	KVOpPut         uint8 = 2
	KVOpDelete      uint8 = 3
	KVOpScan        uint8 = 4
	KVOpDeleteRange uint8 = 5
	KVOpInsert      uint8 = 6
	KVOpCommit      uint8 = 7
	KVOpRollback    uint8 = 8
)

// TLVValue represents a single TLV (tag-length-value) entry.
type TLVValue struct {
	Tag   uint8
	Value []byte
}

// TLVEncoder accumulates TLV entries and can be marshaled to bytes.
// Duplicate tags are rejected at encode time (panics, matching oversized-value behaviour).
type TLVEncoder struct {
	entries []TLVValue
	seen    map[uint8]bool
}

// NewTLVEncoder creates a new TLV encoder.
func NewTLVEncoder() *TLVEncoder {
	return &TLVEncoder{
		entries: make([]TLVValue, 0),
		seen:    make(map[uint8]bool),
	}
}

// ErrTLVValueTooLarge is returned when a TLV value exceeds the 64KiB limit.
var ErrTLVValueTooLarge = errors.New("TLV value exceeds maximum length of 65535 bytes")

// AddTag appends a TLV entry with the given tag and raw value.
// Panics if the value exceeds MaxTLVValueLen or if the tag has already been added
// (duplicate tags violate CLIENT_SPEC.md rule 5).
func (e *TLVEncoder) AddTag(tag uint8, value []byte) *TLVEncoder {
	if len(value) > int(MaxTLVValueLen) {
		panic(fmt.Sprintf("TLV value for tag 0x%02X is %d bytes, exceeds maximum %d", tag, len(value), MaxTLVValueLen))
	}
	if e.seen[tag] {
		panic(fmt.Sprintf("duplicate TLV tag 0x%02X: tags must be unique within a frame (CLIENT_SPEC.md rule 5)", tag))
	}
	e.seen[tag] = true
	e.entries = append(e.entries, TLVValue{
		Tag:   tag,
		Value: value,
	})
	return e
}

// AddString adds a string value with the given tag.
func (e *TLVEncoder) AddString(tag uint8, value string) *TLVEncoder {
	return e.AddTag(tag, []byte(value))
}

// AddBytes adds a byte slice value with the given tag.
func (e *TLVEncoder) AddBytes(tag uint8, value []byte) *TLVEncoder {
	return e.AddTag(tag, value)
}

// AddUint32 adds a u32 BE value with the given tag.
func (e *TLVEncoder) AddUint32(tag uint8, value uint32) *TLVEncoder {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, value)
	return e.AddTag(tag, b)
}

// AddUint64 adds a u64 BE value with the given tag.
func (e *TLVEncoder) AddUint64(tag uint8, value uint64) *TLVEncoder {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, value)
	return e.AddTag(tag, b)
}

// AddUint8 adds a single byte value with the given tag.
func (e *TLVEncoder) AddUint8(tag uint8, value uint8) *TLVEncoder {
	b := []byte{value}
	return e.AddTag(tag, b)
}

// Encode marshals all accumulated TLV entries into a single byte slice.
// Format: [tag(1)][len(2 BE)][value...]...
func (e *TLVEncoder) Encode() []byte {
	// Pre-calculate total size.
	totalSize := 0
	for _, entry := range e.entries {
		totalSize += 1 + 2 + len(entry.Value) // tag + len + value
	}

	result := make([]byte, 0, totalSize)
	var lenBuf [2]byte
	for _, entry := range e.entries {
		result = append(result, entry.Tag)
		binary.BigEndian.PutUint16(lenBuf[:], uint16(len(entry.Value)))
		result = append(result, lenBuf[:]...)
		result = append(result, entry.Value...)
	}
	return result
}

// TLVDecoder decodes a byte slice into TLV entries.
// Duplicate tags are rejected per CLIENT_SPEC.md rule 5.
type TLVDecoder struct {
	entries map[uint8][]byte
}

// NewTLVDecoder creates a decoder from raw TLV bytes.
// Returns an error if the data is malformed or contains duplicate tags.
func NewTLVDecoder(data []byte) (*TLVDecoder, error) {
	dec := &TLVDecoder{
		entries: make(map[uint8][]byte),
	}
	if err := dec.parse(data); err != nil {
		return nil, err
	}
	return dec, nil
}

// parse decodes the raw TLV data.
// Returns an error on truncated data or duplicate tags.
func (d *TLVDecoder) parse(data []byte) error {
	offset := 0
	for offset < len(data) {
		if offset+3 > len(data) {
			return errors.New("truncated TLV: insufficient bytes for tag and length")
		}
		tag := data[offset]
		offset++
		length := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2
		if offset+int(length) > len(data) {
			return errors.New("truncated TLV: value exceeds remaining bytes")
		}
		value := make([]byte, length)
		copy(value, data[offset:offset+int(length)])
		offset += int(length)
		if _, exists := d.entries[tag]; exists {
			return fmt.Errorf("duplicate TLV tag 0x%02X: tags must be unique within a frame", tag)
		}
		d.entries[tag] = value
	}
	return nil
}

// GetBytes retrieves the raw byte value for a tag, or nil if not present.
func (d *TLVDecoder) GetBytes(tag uint8) []byte {
	val, ok := d.entries[tag]
	if !ok {
		return nil
	}
	return append([]byte(nil), val...)
}

// GetString retrieves the string value for a tag, or empty string if not present.
func (d *TLVDecoder) GetString(tag uint8) string {
	b := d.GetBytes(tag)
	if b == nil {
		return ""
	}
	return string(b)
}

// GetUint32 retrieves a u32 BE value for a tag (first value), or 0 if not present or invalid.
func (d *TLVDecoder) GetUint32(tag uint8) (uint32, error) {
	value := d.GetBytes(tag)
	if value == nil {
		return 0, nil
	}
	if len(value) != 4 {
		return 0, fmt.Errorf("invalid u32 length for tag %d: expected 4, got %d", tag, len(value))
	}
	return binary.BigEndian.Uint32(value), nil
}

// GetUint64 retrieves a u64 BE value for a tag (first value), or 0 if not present or invalid.
func (d *TLVDecoder) GetUint64(tag uint8) (uint64, error) {
	value := d.GetBytes(tag)
	if value == nil {
		return 0, nil
	}
	if len(value) != 8 {
		return 0, fmt.Errorf("invalid u64 length for tag %d: expected 8, got %d", tag, len(value))
	}
	return binary.BigEndian.Uint64(value), nil
}

// Has checks if a tag is present in the decoded TLV.
func (d *TLVDecoder) Has(tag uint8) bool {
	_, ok := d.entries[tag]
	return ok
}

// All returns a copy of all decoded entries (useful for debugging).
func (d *TLVDecoder) All() map[uint8][]byte {
	result := make(map[uint8][]byte)
	for k, v := range d.entries {
		result[k] = append([]byte(nil), v...)
	}
	return result
}

// EncodeOpTag encodes a uint16 operation code using escape byte encoding per CLIENT_SPEC.md.
// Values 0x00-0xFE are encoded as single byte; values 0xFF+ are escaped as 0xFF followed by u16 BE.
// Example: NoticePublish(500) encodes as 0xFF 0x01 0xF4
func EncodeOpTag(opCode uint16) []byte {
	if opCode <= 0xFE {
		return []byte{uint8(opCode)}
	}
	// Escape sequence: 0xFF followed by 2-byte big-endian code
	b := make([]byte, 3)
	b[0] = 0xFF
	binary.BigEndian.PutUint16(b[1:3], opCode)
	return b
}

// DecodeOpTag decodes an operation code from escape byte format.
// Returns the decoded code and the number of bytes consumed, or an error if truncated.
func DecodeOpTag(data []byte) (uint16, int, error) {
	if len(data) == 0 {
		return 0, 0, errors.New("empty TLV value for TagOp")
	}
	if data[0] != 0xFF {
		// Single byte code (0x00-0xFE)
		return uint16(data[0]), 1, nil
	}
	// Escape sequence: 0xFF followed by u16 BE
	if len(data) < 3 {
		return 0, 0, errors.New("truncated TagOp escape sequence")
	}
	code := binary.BigEndian.Uint16(data[1:3])
	return code, 3, nil
}

// AddOp adds an operation code with TagOp using escape byte encoding.
func (e *TLVEncoder) AddOp(opCode uint16) *TLVEncoder {
	encoded := EncodeOpTag(opCode)
	return e.AddTag(TagOp, encoded)
}

// GetOp retrieves an operation code from TagOp, decoding escape bytes.
// Returns 0 if TagOp is not present.
func (d *TLVDecoder) GetOp() (uint16, error) {
	value := d.GetBytes(TagOp)
	if value == nil {
		return 0, nil
	}
	code, _, err := DecodeOpTag(value)
	return code, err
}
