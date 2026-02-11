package kv

import (
	"encoding/binary"
	"errors"
	"strings"
)

// Wire opcodes for KV domain (per CLIENT_SPEC.md: 100+).
const (
	KVBegin       uint16 = 100
	KVCommit      uint16 = 101
	KVRollback    uint16 = 102
	KVGet         uint16 = 103
	KVPut         uint16 = 104
	KVInsert      uint16 = 105
	KVDelete      uint16 = 106
	KVDeleteRange uint16 = 107
	KVScan        uint16 = 108
)

// Transaction modes (per CLIENT_SPEC.md).
const (
	TxModeReadOnly  uint8 = 0
	TxModeReadWrite uint8 = 1
)

// Durability modes (per CLIENT_SPEC.md).
const (
	DurabilityBuffered uint8 = 0
	DurabilitySync     uint8 = 1
)

// EncodeBegin encodes a KV BEGIN request payload per CLIENT_SPEC.md.
func EncodeBegin(route string, mode uint8, durability uint8) []byte {
	routeBytes := []byte(route)
	routeLen := uint32(len(routeBytes))

	// Calculate total payload size
	payloadSize := 4 + len(routeBytes) + 1 + 1 // route_len(4) + route + mode(1) + durability(1)
	payload := make([]byte, 0, payloadSize)

	// [u32 BE] route_len
	routeLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(routeLenBytes, routeLen)
	payload = append(payload, routeLenBytes...)

	// [bytes] route
	payload = append(payload, routeBytes...)

	// [u8] mode
	payload = append(payload, mode)

	// [u8] durability
	payload = append(payload, durability)

	return payload
}

// EncodePut encodes a KV PUT request payload per CLIENT_SPEC.md.
// Spec: [tx_id (u64 BE)][route_len (u32 BE)][route][key_len (u32 BE)][key][value_len (u32 BE)][value]
func EncodePut(txID uint64, route string, key, value []byte) []byte {
	routeBytes := []byte(route)
	routeLen := uint32(len(routeBytes))
	keyLen := uint32(len(key))
	valueLen := uint32(len(value))

	// Calculate total payload size
	payloadSize := 8 + 4 + len(routeBytes) + 4 + len(key) + 4 + len(value)
	payload := make([]byte, 0, payloadSize)

	// [u64 BE] tx_id
	txIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(txIDBytes, txID)
	payload = append(payload, txIDBytes...)

	// [u32 BE] route_len
	routeLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(routeLenBytes, routeLen)
	payload = append(payload, routeLenBytes...)

	// [bytes] route
	payload = append(payload, routeBytes...)

	// [u32 BE] key_len
	keyLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLenBytes, keyLen)
	payload = append(payload, keyLenBytes...)

	// [bytes] key
	payload = append(payload, key...)

	// [u32 BE] value_len
	valueLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(valueLenBytes, valueLen)
	payload = append(payload, valueLenBytes...)

	// [bytes] value
	payload = append(payload, value...)

	return payload
}

// EncodeGet encodes a KV GET request payload per CLIENT_SPEC.md.
// Spec: [tx_id (u64 BE)][route_len (u32 BE)][route][key_len (u32 BE)][key]
func EncodeGet(txID uint64, route string, key []byte) []byte {
	routeBytes := []byte(route)
	routeLen := uint32(len(routeBytes))
	keyLen := uint32(len(key))

	// Calculate total payload size
	payloadSize := 8 + 4 + len(routeBytes) + 4 + len(key)
	payload := make([]byte, 0, payloadSize)

	// [u64 BE] tx_id
	txIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(txIDBytes, txID)
	payload = append(payload, txIDBytes...)

	// [u32 BE] route_len
	routeLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(routeLenBytes, routeLen)
	payload = append(payload, routeLenBytes...)

	// [bytes] route
	payload = append(payload, routeBytes...)

	// [u32 BE] key_len
	keyLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLenBytes, keyLen)
	payload = append(payload, keyLenBytes...)

	// [bytes] key
	payload = append(payload, key...)

	return payload
}

// EncodeCommit encodes a KV COMMIT request payload per CLIENT_SPEC.md.
// Spec: [tx_id (u64 BE)]
func EncodeCommit(txID uint64) []byte {
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, txID)
	return payload
}

// EncodeRollback encodes a KV ROLLBACK request payload per CLIENT_SPEC.md.
// Spec: [tx_id (u64 BE)]
func EncodeRollback(txID uint64) []byte {
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, txID)
	return payload
}

// Domain-specific errors.
var (
	ErrNotFound            = errors.New("key not found")
	ErrKeyExists           = errors.New("key already exists")
	ErrConcurrencyConflict = errors.New("concurrency conflict")
	ErrInvalidRange        = errors.New("invalid key range")
	ErrKeyTooLarge         = errors.New("key too large")
	ErrValueTooLarge       = errors.New("value too large")
	ErrTransactionAborted  = errors.New("transaction aborted")
)

// mapKVError maps a broker error message to a domain-specific Go error.
func mapKVError(msg string) error {
	l := strings.ToLower(msg)
	switch {
	case strings.Contains(l, "not found"):
		return ErrNotFound
	case strings.Contains(l, "exists") || strings.Contains(l, "already"):
		return ErrKeyExists
	case strings.Contains(l, "conflict") || strings.Contains(l, "concurrency"):
		return ErrConcurrencyConflict
	case strings.Contains(l, "range"):
		return ErrInvalidRange
	case strings.Contains(l, "key too large"):
		return ErrKeyTooLarge
	case strings.Contains(l, "value too large"):
		return ErrValueTooLarge
	case strings.Contains(l, "abort"):
		return ErrTransactionAborted
	default:
		return errors.New(msg)
	}
}
