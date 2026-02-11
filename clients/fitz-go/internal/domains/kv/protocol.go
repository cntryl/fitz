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

// Size limits for keys and values (safety constraints).
const (
	MaxKeySize   = 64 * 1024        // 64 KB max key size
	MaxValueSize = 16 * 1024 * 1024 // 16 MB max value size (transport frame limit)
)

// EncodeBegin encodes a KV BEGIN request payload per CLIENT_SPEC.md.
func EncodeBegin(route string, mode uint8, durability uint8) ([]byte, error) {
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

	return payload, nil
}

// EncodePut encodes a KV PUT request payload per CLIENT_SPEC.md.
// Spec: [tx_id (u64 BE)][route_len (u32 BE)][route][key_len (u32 BE)][key][value_len (u32 BE)][value]
func EncodePut(txID uint64, route string, key, value []byte) ([]byte, error) {
	// Validate key and value sizes
	if err := ValidateKeySize(key); err != nil {
		return nil, err
	}
	if err := ValidateValueSize(value); err != nil {
		return nil, err
	}

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

	return payload, nil
}

// EncodeGet encodes a KV GET request payload per CLIENT_SPEC.md.
// Spec: [tx_id (u64 BE)][route_len (u32 BE)][route][key_len (u32 BE)][key]
func EncodeGet(txID uint64, route string, key []byte) ([]byte, error) {
	// Validate key size
	if err := ValidateKeySize(key); err != nil {
		return nil, err
	}

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

	return payload, nil
}

// EncodeInsert encodes a KV INSERT request payload per CLIENT_SPEC.md.
// Wire format is identical to PUT (server distinguishes by MessageType).
func EncodeInsert(txID uint64, route string, key, value []byte) ([]byte, error) {
	return EncodePut(txID, route, key, value)
}

// EncodeDelete encodes a KV DELETE request payload per CLIENT_SPEC.md.
// Spec: [tx_id (u64 BE)][route_len (u32 BE)][route][key_len (u32 BE)][key]
func EncodeDelete(txID uint64, route string, key []byte) ([]byte, error) {
	// Validate key size
	if err := ValidateKeySize(key); err != nil {
		return nil, err
	}

	routeBytes := []byte(route)
	routeLen := uint32(len(routeBytes))
	keyLen := uint32(len(key))

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

	return payload, nil
}

// EncodeDeleteRange encodes a KV DELETE_RANGE request payload per CLIENT_SPEC.md.
// Spec: [tx_id][route_len][route][start_key_len][start_key][end_key_len][end_key]
func EncodeDeleteRange(txID uint64, route string, startKey, endKey []byte) ([]byte, error) {
	// Validate key sizes
	if err := ValidateKeySize(startKey); err != nil {
		return nil, err
	}
	if err := ValidateKeySize(endKey); err != nil {
		return nil, err
	}

	routeBytes := []byte(route)
	routeLen := uint32(len(routeBytes))
	startKeyLen := uint32(len(startKey))
	endKeyLen := uint32(len(endKey))

	payloadSize := 8 + 4 + len(routeBytes) + 4 + len(startKey) + 4 + len(endKey)
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

	// [u32 BE] start_key_len
	startKeyLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(startKeyLenBytes, startKeyLen)
	payload = append(payload, startKeyLenBytes...)

	// [bytes] start_key
	payload = append(payload, startKey...)

	// [u32 BE] end_key_len
	endKeyLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(endKeyLenBytes, endKeyLen)
	payload = append(payload, endKeyLenBytes...)

	// [bytes] end_key
	payload = append(payload, endKey...)

	return payload, nil
}

// ScanQuery represents SCAN operation parameters.
type ScanQuery struct {
	StartKey []byte // Inclusive lower bound (nil = from beginning)
	EndKey   []byte // Exclusive upper bound (nil = to end)
	Limit    uint32 // Max items to return (0 = unlimited)
	Reverse  bool   // true = descending order
}

// EncodeScan encodes a KV SCAN request payload per CLIENT_SPEC.md.
// Spec: [tx_id][route_len][route][has_start][start_key_len?][start_key?]
//
//	[has_end][end_key_len?][end_key?][has_limit][limit?][reverse]
func EncodeScan(txID uint64, route string, query ScanQuery) ([]byte, error) {
	// Validate key sizes if present
	if query.StartKey != nil {
		if err := ValidateKeySize(query.StartKey); err != nil {
			return nil, err
		}
	}
	if query.EndKey != nil {
		if err := ValidateKeySize(query.EndKey); err != nil {
			return nil, err
		}
	}

	routeBytes := []byte(route)
	routeLen := uint32(len(routeBytes))

	// Start with fixed fields
	payloadSize := 8 + 4 + len(routeBytes) + 1 + 1 + 1 + 1
	if query.StartKey != nil {
		payloadSize += 4 + len(query.StartKey)
	}
	if query.EndKey != nil {
		payloadSize += 4 + len(query.EndKey)
	}
	if query.Limit > 0 {
		payloadSize += 4
	}

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

	// [u8] has_start
	if query.StartKey != nil {
		payload = append(payload, 1)
		// [u32 BE] start_key_len
		startKeyLen := uint32(len(query.StartKey))
		startKeyLenBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(startKeyLenBytes, startKeyLen)
		payload = append(payload, startKeyLenBytes...)
		// [bytes] start_key
		payload = append(payload, query.StartKey...)
	} else {
		payload = append(payload, 0)
	}

	// [u8] has_end
	if query.EndKey != nil {
		payload = append(payload, 1)
		// [u32 BE] end_key_len
		endKeyLen := uint32(len(query.EndKey))
		endKeyLenBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(endKeyLenBytes, endKeyLen)
		payload = append(payload, endKeyLenBytes...)
		// [bytes] end_key
		payload = append(payload, query.EndKey...)
	} else {
		payload = append(payload, 0)
	}

	// [u8] has_limit
	if query.Limit > 0 {
		payload = append(payload, 1)
		// [u32 BE] limit
		limitBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(limitBytes, query.Limit)
		payload = append(payload, limitBytes...)
	} else {
		payload = append(payload, 0)
	}

	// [u8] reverse
	if query.Reverse {
		payload = append(payload, 1)
	} else {
		payload = append(payload, 0)
	}

	return payload, nil
}

// EncodeCommit encodes a KV COMMIT request payload per CLIENT_SPEC.md.
// Spec: [tx_id (u64 BE)][route_len (u32 BE)][route]
// Operations are self-contained per CLIENT_SPEC.md design.
func EncodeCommit(txID uint64, route string) ([]byte, error) {
	routeBytes := []byte(route)
	routeLen := uint32(len(routeBytes))

	payloadSize := 8 + 4 + len(routeBytes)
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

	return payload, nil
}

// EncodeRollback encodes a KV ROLLBACK request payload per CLIENT_SPEC.md.
// Spec: [tx_id (u64 BE)][route_len (u32 BE)][route]
// Operations are self-contained per CLIENT_SPEC.md design.
func EncodeRollback(txID uint64, route string) ([]byte, error) {
	routeBytes := []byte(route)
	routeLen := uint32(len(routeBytes))

	payloadSize := 8 + 4 + len(routeBytes)
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

	return payload, nil
}

// ValidateKeySize checks if key size is within limits.
func ValidateKeySize(key []byte) error {
	if key == nil {
		return errors.New("key cannot be nil")
	}
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}
	if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	return nil
}

// ValidateValueSize checks if value size is within limits.
func ValidateValueSize(value []byte) error {
	if value == nil {
		return errors.New("value cannot be nil")
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}
	return nil
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
