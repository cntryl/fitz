package kv

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/iter"
	"github.com/cntryl/fitz-go/internal/protocol"
)

// KVPair is a key/value pair returned by Scan operations.
type KVPair struct {
	Key   []byte
	Value []byte
}

// ReadTx exposes read-only operations within a transaction.
//
// CRITICAL: Per CLIENT_SPEC.md, operations within a single transaction
// MUST be sequential. Do NOT call methods concurrently on the same transaction.
type ReadTx interface {
	Get(ctx context.Context, key []byte) (value []byte, found bool, err error)
	Scan(ctx context.Context, query ScanQuery) (iter.Iterator[KVPair], bool, error)
}

// Tx is a read/write transaction. It embeds ReadTx and adds mutations.
//
// CRITICAL: Per CLIENT_SPEC.md lines 323-361, operations within a single
// transaction (same tx_id) MUST be sequential. Concurrent calls will corrupt
// transaction state on the server.
type Tx interface {
	ReadTx
	Put(ctx context.Context, key, value []byte) error
	Insert(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
	DeleteRange(ctx context.Context, startKey, endKey []byte) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// transaction is a concrete implementation of both ReadTx and Tx.
// NOT thread-safe - caller must serialize operations per CLIENT_SPEC.md.
type transaction struct {
	route      string
	conn       *connection.Connection
	readOnly   bool
	txID       uint64 // Server-assigned per CLIENT_SPEC.md
	committed  atomic.Bool
	rolledback atomic.Bool
}

// Get retrieves a value by key.
// Returns (value, true, nil) if key exists.
// Returns (nil, false, nil) if key does not exist (not an error per CLIENT_SPEC.md).
// Returns (nil, false, err) on actual errors.
func (t *transaction) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	// Validate state
	if err := t.checkState(); err != nil {
		return nil, false, err
	}

	// Validate key
	if err := ValidateKeySize(key); err != nil {
		return nil, false, err
	}

	// Encode request per CLIENT_SPEC.md
	payload, err := EncodeGet(t.txID, t.route, key)
	if err != nil {
		return nil, false, fmt.Errorf("encode GET: %w", err)
	}

	// Send request
	resp, err := t.conn.SendRequest(ctx, protocol.MessageTypeKvGet, payload)
	if err != nil {
		return nil, false, fmt.Errorf("GET request failed: %w", err)
	}

	// Parse standard response status
	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return nil, false, fmt.Errorf("GET failed: %w", mapKVError(err.Error()))
	}
	if !success {
		return nil, false, fmt.Errorf("GET failed: unexpected status")
	}

	// Parse GET-specific response: [found][value_len?][value?]
	if len(remaining) < 1 {
		return nil, false, fmt.Errorf("invalid GET response: expected at least 1 byte for found flag")
	}

	found := remaining[0] == 1

	if !found {
		return nil, false, nil // Key not found (normal case, not an error)
	}

	// Extract value
	if len(remaining) < 5 { // found(1) + value_len(4)
		return nil, false, fmt.Errorf("invalid GET response: missing value length")
	}

	valueLen, offset, err := connection.ReadU32BE(remaining, 1)
	if err != nil {
		return nil, false, fmt.Errorf("parse value length: %w", err)
	}

	if len(remaining) < offset+int(valueLen) {
		return nil, false, fmt.Errorf("invalid GET response: truncated value")
	}

	value := make([]byte, valueLen)
	copy(value, remaining[offset:offset+int(valueLen)])

	return value, true, nil
}

// Put upserts a key/value pair (create or overwrite).
func (t *transaction) Put(ctx context.Context, key, value []byte) error {
	// Validate state
	if err := t.checkState(); err != nil {
		return err
	}

	// Validate inputs
	if err := ValidateKeySize(key); err != nil {
		return err
	}
	if err := ValidateValueSize(value); err != nil {
		return err
	}

	// Encode request
	payload, err := EncodePut(t.txID, t.route, key, value)
	if err != nil {
		return fmt.Errorf("encode PUT: %w", err)
	}

	// Send request
	resp, err := t.conn.SendRequest(ctx, protocol.MessageTypeKvPut, payload)
	if err != nil {
		return fmt.Errorf("PUT request failed: %w", err)
	}

	// Parse standard response
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("PUT failed: %w", mapKVError(err.Error()))
	}
	if !success {
		return fmt.Errorf("PUT failed: unexpected status")
	}

	return nil
}

// Insert creates a new key/value pair. Fails if key already exists.
func (t *transaction) Insert(ctx context.Context, key, value []byte) error {
	// Validate state
	if err := t.checkState(); err != nil {
		return err
	}

	// Validate inputs
	if err := ValidateKeySize(key); err != nil {
		return err
	}
	if err := ValidateValueSize(value); err != nil {
		return err
	}

	// Encode request (wire format same as PUT)
	payload, err := EncodeInsert(t.txID, t.route, key, value)
	if err != nil {
		return fmt.Errorf("encode INSERT: %w", err)
	}

	// Send request
	resp, err := t.conn.SendRequest(ctx, protocol.MessageTypeKvInsert, payload)
	if err != nil {
		return fmt.Errorf("INSERT request failed: %w", err)
	}

	// Parse standard response
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("INSERT failed: %w", mapKVError(err.Error()))
	}
	if !success {
		return fmt.Errorf("INSERT failed: unexpected status")
	}

	return nil
}

// Delete removes a key. Idempotent (deleting non-existent key succeeds).
func (t *transaction) Delete(ctx context.Context, key []byte) error {
	// Validate state
	if err := t.checkState(); err != nil {
		return err
	}

	// Validate key
	if err := ValidateKeySize(key); err != nil {
		return err
	}

	// Encode request
	payload, err := EncodeDelete(t.txID, t.route, key)
	if err != nil {
		return fmt.Errorf("encode DELETE: %w", err)
	}

	// Send request
	resp, err := t.conn.SendRequest(ctx, protocol.MessageTypeKvDelete, payload)
	if err != nil {
		return fmt.Errorf("DELETE request failed: %w", err)
	}

	// Parse standard response
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("DELETE failed: %w", mapKVError(err.Error()))
	}
	if !success {
		return fmt.Errorf("DELETE failed: unexpected status")
	}

	return nil
}

// DeleteRange removes all keys in range [startKey, endKey) (exclusive end).
func (t *transaction) DeleteRange(ctx context.Context, startKey, endKey []byte) error {
	// Validate state
	if err := t.checkState(); err != nil {
		return err
	}

	// Validate keys
	if err := ValidateKeySize(startKey); err != nil {
		return fmt.Errorf("invalid start key: %w", err)
	}
	if err := ValidateKeySize(endKey); err != nil {
		return fmt.Errorf("invalid end key: %w", err)
	}

	// Encode request
	payload, err := EncodeDeleteRange(t.txID, t.route, startKey, endKey)
	if err != nil {
		return fmt.Errorf("encode DELETE_RANGE: %w", err)
	}

	// Send request
	resp, err := t.conn.SendRequest(ctx, protocol.MessageTypeKvDeleteRange, payload)
	if err != nil {
		return fmt.Errorf("DELETE_RANGE request failed: %w", err)
	}

	// Parse standard response
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("DELETE_RANGE failed: %w", mapKVError(err.Error()))
	}
	if !success {
		return fmt.Errorf("DELETE_RANGE failed: unexpected status")
	}

	return nil
}

// Scan returns an iterator over key/value pairs matching the query.
// Returns (iterator, hasMore, error) where hasMore indicates server has additional results.
//
// Per CLIENT_SPEC.md: SCAN returns batch results in one response (not streaming).
// Use SliceIterator for simple in-memory iteration.
func (t *transaction) Scan(ctx context.Context, query ScanQuery) (iter.Iterator[KVPair], bool, error) {
	// Validate state
	if err := t.checkState(); err != nil {
		return nil, false, err
	}

	// Encode request
	payload, err := EncodeScan(t.txID, t.route, query)
	if err != nil {
		return nil, false, fmt.Errorf("encode SCAN: %w", err)
	}

	// Send request
	resp, err := t.conn.SendRequest(ctx, protocol.MessageTypeKvScan, payload)
	if err != nil {
		return nil, false, fmt.Errorf("SCAN request failed: %w", err)
	}

	// Parse standard response status
	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return nil, false, fmt.Errorf("SCAN failed: %w", mapKVError(err.Error()))
	}
	if !success {
		return nil, false, fmt.Errorf("SCAN failed: unexpected status")
	}

	// Parse SCAN response: [item_count][items...][has_more]
	pairs, hasMore, err := parseScanResponse(remaining)
	if err != nil {
		return nil, false, fmt.Errorf("parse SCAN response: %w", err)
	}

	// Return SliceIterator for batch results
	return iter.NewSliceIterator(pairs), hasMore, nil
}

// parseScanResponse parses batch SCAN results per CLIENT_SPEC.md.
// Response: [item_count][key_len][key][value_len][value]...[has_more]
func parseScanResponse(remaining []byte) ([]KVPair, bool, error) {
	if len(remaining) < 4 { // item_count(4)
		return nil, false, fmt.Errorf("invalid SCAN response: too short")
	}

	itemCount, offset, err := connection.ReadU32BE(remaining, 0)
	if err != nil {
		return nil, false, fmt.Errorf("parse item_count: %w", err)
	}

	pairs := make([]KVPair, 0, itemCount)

	for i := uint32(0); i < itemCount; i++ {
		// Parse key
		if offset+4 > len(remaining) {
			return nil, false, fmt.Errorf("truncated key length at item %d", i)
		}
		keyLen, newOffset, err := connection.ReadU32BE(remaining, offset)
		if err != nil {
			return nil, false, fmt.Errorf("parse key length: %w", err)
		}
		offset = newOffset

		if offset+int(keyLen) > len(remaining) {
			return nil, false, fmt.Errorf("truncated key at item %d", i)
		}
		key := make([]byte, keyLen)
		copy(key, remaining[offset:offset+int(keyLen)])
		offset += int(keyLen)

		// Parse value
		if offset+4 > len(remaining) {
			return nil, false, fmt.Errorf("truncated value length at item %d", i)
		}
		valueLen, newOffset, err := connection.ReadU32BE(remaining, offset)
		if err != nil {
			return nil, false, fmt.Errorf("parse value length: %w", err)
		}
		offset = newOffset

		if offset+int(valueLen) > len(remaining) {
			return nil, false, fmt.Errorf("truncated value at item %d", i)
		}
		value := make([]byte, valueLen)
		copy(value, remaining[offset:offset+int(valueLen)])
		offset += int(valueLen)

		pairs = append(pairs, KVPair{Key: key, Value: value})
	}

	// Parse has_more flag
	if offset+1 > len(remaining) {
		return nil, false, fmt.Errorf("missing has_more flag")
	}
	hasMore := remaining[offset] == 1

	return pairs, hasMore, nil
}

// Commit finalizes the transaction durably.
func (t *transaction) Commit(ctx context.Context) error {
	// Validate state
	if err := t.checkState(); err != nil {
		return err
	}

	// Encode request
	payload, err := EncodeCommit(t.txID, t.route)
	if err != nil {
		return fmt.Errorf("encode COMMIT: %w", err)
	}

	// Send request
	resp, err := t.conn.SendRequest(ctx, protocol.MessageTypeKvCommit, payload)
	if err != nil {
		return fmt.Errorf("COMMIT request failed: %w", err)
	}

	// Parse standard response
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("COMMIT failed: %w", mapKVError(err.Error()))
	}
	if !success {
		return fmt.Errorf("COMMIT failed: unexpected status")
	}

	// Mark transaction as committed
	t.committed.Store(true)

	return nil
}

// Rollback aborts the transaction, discarding all changes.
func (t *transaction) Rollback(ctx context.Context) error {
	// Validate state (allow rollback even if committed)
	if t.rolledback.Load() {
		return fmt.Errorf("transaction already rolled back")
	}

	// Encode request
	payload, err := EncodeRollback(t.txID, t.route)
	if err != nil {
		return fmt.Errorf("encode ROLLBACK: %w", err)
	}

	// Send request
	resp, err := t.conn.SendRequest(ctx, protocol.MessageTypeKvRollback, payload)
	if err != nil {
		return fmt.Errorf("ROLLBACK request failed: %w", err)
	}

	// Parse standard response
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("ROLLBACK failed: %w", mapKVError(err.Error()))
	}
	if !success {
		return fmt.Errorf("ROLLBACK failed: unexpected status")
	}

	// Mark transaction as rolled back
	t.rolledback.Store(true)

	return nil
}

// checkState validates transaction state before operations.
func (t *transaction) checkState() error {
	if t.committed.Load() {
		return fmt.Errorf("transaction already committed")
	}
	if t.rolledback.Load() {
		return fmt.Errorf("transaction already rolled back")
	}
	return nil
}
