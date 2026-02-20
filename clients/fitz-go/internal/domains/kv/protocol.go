package kv

import (
	"bytes"
	"errors"
	"strings"

	"github.com/cntryl/fitz-go/internal/core/encoding"
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
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		buf.WriteByte(mode)
		buf.WriteByte(durability)
	}), nil
}

func beginPayloadWriter(route string, mode uint8, durability uint8) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		buf.WriteByte(mode)
		buf.WriteByte(durability)
	}
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

	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)
		encoding.WriteBytes(buf, key)
		encoding.WriteBytes(buf, value)
	}), nil
}

func putPayloadWriter(txID uint64, route string, key, value []byte) (func(*bytes.Buffer), error) {
	if err := ValidateKeySize(key); err != nil {
		return nil, err
	}
	if err := ValidateValueSize(value); err != nil {
		return nil, err
	}
	return func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)
		encoding.WriteBytes(buf, key)
		encoding.WriteBytes(buf, value)
	}, nil
}

// EncodeGet encodes a KV GET request payload per CLIENT_SPEC.md.
// Spec: [tx_id (u64 BE)][route_len (u32 BE)][route][key_len (u32 BE)][key]
func EncodeGet(txID uint64, route string, key []byte) ([]byte, error) {
	// Validate key size
	if err := ValidateKeySize(key); err != nil {
		return nil, err
	}

	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)
		encoding.WriteBytes(buf, key)
	}), nil
}

func getPayloadWriter(txID uint64, route string, key []byte) (func(*bytes.Buffer), error) {
	if err := ValidateKeySize(key); err != nil {
		return nil, err
	}
	return func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)
		encoding.WriteBytes(buf, key)
	}, nil
}

// EncodeInsert encodes a KV INSERT request payload per CLIENT_SPEC.md.
// Wire format is identical to PUT (server distinguishes by MessageType).
func EncodeInsert(txID uint64, route string, key, value []byte) ([]byte, error) {
	return EncodePut(txID, route, key, value)
}

func insertPayloadWriter(txID uint64, route string, key, value []byte) (func(*bytes.Buffer), error) {
	return putPayloadWriter(txID, route, key, value)
}

// EncodeDelete encodes a KV DELETE request payload per CLIENT_SPEC.md.
// Spec: [tx_id (u64 BE)][route_len (u32 BE)][route][key_len (u32 BE)][key]
func EncodeDelete(txID uint64, route string, key []byte) ([]byte, error) {
	// Validate key size
	if err := ValidateKeySize(key); err != nil {
		return nil, err
	}

	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)
		encoding.WriteBytes(buf, key)
	}), nil
}

func deletePayloadWriter(txID uint64, route string, key []byte) (func(*bytes.Buffer), error) {
	if err := ValidateKeySize(key); err != nil {
		return nil, err
	}
	return func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)
		encoding.WriteBytes(buf, key)
	}, nil
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

	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)
		encoding.WriteBytes(buf, startKey)
		encoding.WriteBytes(buf, endKey)
	}), nil
}

func deleteRangePayloadWriter(txID uint64, route string, startKey, endKey []byte) (func(*bytes.Buffer), error) {
	if err := ValidateKeySize(startKey); err != nil {
		return nil, err
	}
	if err := ValidateKeySize(endKey); err != nil {
		return nil, err
	}
	return func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)
		encoding.WriteBytes(buf, startKey)
		encoding.WriteBytes(buf, endKey)
	}, nil
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

	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)

		// [u8] has_start + optional key
		if query.StartKey != nil {
			buf.WriteByte(1)
			encoding.WriteBytes(buf, query.StartKey)
		} else {
			buf.WriteByte(0)
		}

		// [u8] has_end + optional key
		if query.EndKey != nil {
			buf.WriteByte(1)
			encoding.WriteBytes(buf, query.EndKey)
		} else {
			buf.WriteByte(0)
		}

		// [u8] has_limit + optional limit
		if query.Limit > 0 {
			buf.WriteByte(1)
			encoding.WriteU32(buf, query.Limit)
		} else {
			buf.WriteByte(0)
		}

		// [u8] reverse
		if query.Reverse {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	}), nil
}

func scanPayloadWriter(txID uint64, route string, query ScanQuery) (func(*bytes.Buffer), error) {
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

	return func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)

		// [u8] has_start + optional key
		if query.StartKey != nil {
			buf.WriteByte(1)
			encoding.WriteBytes(buf, query.StartKey)
		} else {
			buf.WriteByte(0)
		}

		// [u8] has_end + optional key
		if query.EndKey != nil {
			buf.WriteByte(1)
			encoding.WriteBytes(buf, query.EndKey)
		} else {
			buf.WriteByte(0)
		}

		// [u8] has_limit + optional limit
		if query.Limit > 0 {
			buf.WriteByte(1)
			encoding.WriteU32(buf, query.Limit)
		} else {
			buf.WriteByte(0)
		}

		// [u8] reverse
		if query.Reverse {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	}, nil
}

// EncodeCommit encodes a KV COMMIT request payload per CLIENT_SPEC.md.
// Spec: [tx_id (u64 BE)][route_len (u32 BE)][route]
// Operations are self-contained per CLIENT_SPEC.md design.
func EncodeCommit(txID uint64, route string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)
	}), nil
}

func commitPayloadWriter(txID uint64, route string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)
	}
}

// EncodeRollback encodes a KV ROLLBACK request payload per CLIENT_SPEC.md.
// Spec: [tx_id (u64 BE)][route_len (u32 BE)][route]
// Operations are self-contained per CLIENT_SPEC.md design.
func EncodeRollback(txID uint64, route string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)
	}), nil
}

func rollbackPayloadWriter(txID uint64, route string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)
	}
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
// Domain-specific errors (mapped from broker error responses).
//   - ErrNotFound: key not found (e.g. Get, Delete).
//   - ErrKeyExists: Insert failed because the key already exists.
//   - ErrConcurrencyConflict: isolation conflict; another transaction holds the resource.
//   - ErrInvalidRange: scan range or key format invalid.
//   - ErrKeyTooLarge / ErrValueTooLarge: key or value exceeds size limit.
//   - ErrTransactionAborted: transaction was aborted by the server.
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
