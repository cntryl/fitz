package kv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/iter"
	"github.com/cntryl/cntryl-go/internal/core/transport"
)

// KVPair is a simple key/value pair returned by Scan operations.
type KVPair struct {
	Key   []byte
	Value []byte
}

// ReadTx exposes read-only operations within a transaction.
//
// Scan returns a streaming `iter.Iterator[KVPair]` for results in the range
// [startKey, endKey), ordered lexicographically, up to `limit` items.
type ReadTx interface {
	Get(ctx context.Context, key []byte) ([]byte, bool, error)
	Scan(ctx context.Context, startKey []byte, endKey []byte, limit uint32) (iter.Iterator[KVPair], error)
}

// Tx is a read/write transaction. It embeds ReadTx and adds mutation and transaction control methods.
type Tx interface {
	ReadTx
	Put(ctx context.Context, key []byte, value []byte) error
	Insert(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	DeleteRange(ctx context.Context, startKey []byte, endKey []byte) (int, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// transaction is a concrete implementation of both ReadTx and Tx using the transport mux.
type transaction struct {
	route      Route
	mux        transport.MuxProvider
	readOnly   bool
	mu         sync.RWMutex
	txID       uint64
	committed  atomic.Bool
	rolledback atomic.Bool
}

// nextTxID provides unique transaction IDs.
var nextTxID atomic.Uint64

func init() {
	nextTxID.Store(1)
}

// Get retrieves a value by key from the broker.
func (t *transaction) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, fmt.Errorf("key cannot be empty")
	}
	if t.committed.Load() {
		return nil, false, fmt.Errorf("transaction already committed")
	}
	if t.rolledback.Load() {
		return nil, false, fmt.Errorf("transaction already rolled back")
	}

	// Use a unique request ID for correlation.
	requestID := nextTxID.Add(1)

	// Build the request TLV payload.
	enc := transport.NewTLVEncoder()
	enc.AddOp(KVGet)
	enc.AddString(transport.TagRoute, t.route.String())
	enc.AddBytes(transport.TagKey, key)
	enc.AddUint64(transport.TagID, requestID)

	// Send request frame on the KV channel.
	frame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Flags:   0,
		Channel: transport.ChannelKV,
		Body:    enc.Encode(),
	}
	if err := t.mux.Send(frame); err != nil {
		return nil, false, fmt.Errorf("send Get request: %w", err)
	}

	// Wait for response frame with matching ID.
	for {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case respFrame, ok := <-t.mux.In():
			if !ok {
				return nil, false, errors.New("mux closed")
			}

			// Check if this is our response (same channel, matching ID).
			if respFrame.Channel != transport.ChannelKV {
				continue
			}

			dec, err := transport.NewTLVDecoder(respFrame.Body)
			if err != nil {
				return nil, false, fmt.Errorf("decode response: %w", err)
			}

			// Check if response has the matching ID first.
			respID, _ := dec.GetUint64(transport.TagID)
			if respID != requestID {
				continue // Not our response.
			}

			// Check for error response (only after ID match).
			if dec.Has(transport.TagErr) {
				errMsg := dec.GetString(transport.TagErr)
				return nil, false, mapKVError(errMsg)
			}

			// Response payload.
			if dec.Has(transport.TagBody) {
				value := dec.GetBytes(transport.TagBody)
				return value, true, nil
			}
			// No body = key not found.
			return nil, false, nil
		}
	}
}

// Scan returns an iterator over key/value pairs in the range [startKey, endKey).
func (t *transaction) Scan(ctx context.Context, startKey []byte, endKey []byte, limit uint32) (iter.Iterator[KVPair], error) {
	if limit == 0 {
		return nil, fmt.Errorf("limit must be > 0")
	}
	if t.committed.Load() {
		return nil, fmt.Errorf("transaction already committed")
	}
	if t.rolledback.Load() {
		return nil, fmt.Errorf("transaction already rolled back")
	}

	// Use a unique request ID for correlation.
	requestID := nextTxID.Add(1)

	// Build the request TLV payload.
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, t.route.String())
	if len(startKey) > 0 {
		enc.AddBytes(transport.TagStartKey, startKey)
	}
	if len(endKey) > 0 {
		enc.AddBytes(transport.TagEndKey, endKey)
	}
	enc.AddUint32(transport.TagLimit, limit)
	enc.AddUint64(transport.TagID, requestID)

	// Add operation tag and send request frame.
	enc.AddOp(KVScan)
	frame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Flags:   0,
		Channel: transport.ChannelKV,
		Body:    enc.Encode(),
	}
	if err := t.mux.Send(frame); err != nil {
		return nil, fmt.Errorf("send Scan request: %w", err)
	}

	// Return an iterator that streams results from the mux.
	scanCtx, scanCancel := context.WithCancel(ctx)
	return &scanIterator{
		ctx:    scanCtx,
		cancel: scanCancel,
		mux:    t.mux,
		txID:   requestID,
		done:   false,
	}, nil
}

// scanIterator implements iter.Iterator[KVPair] for Scan results.
type scanIterator struct {
	ctx     context.Context
	cancel  context.CancelFunc
	mux     transport.MuxProvider
	txID    uint64
	current KVPair
	done    bool
	err     error
	mu      sync.Mutex
}

// Next advances the iterator and returns true if a value is available.
func (si *scanIterator) Next() bool {
	si.mu.Lock()
	defer si.mu.Unlock()

	if si.done || si.err != nil {
		return false
	}

	// Wait for the next response frame with matching ID.
	for {
		select {
		case <-si.ctx.Done():
			si.err = si.ctx.Err()
			si.done = true
			return false
		case respFrame, ok := <-si.mux.In():
			if !ok {
				si.err = errors.New("mux closed")
				si.done = true
				return false
			}

			if respFrame.Channel != transport.ChannelKV {
				continue
			}

			dec, err := transport.NewTLVDecoder(respFrame.Body)
			if err != nil {
				si.err = fmt.Errorf("decode response: %w", err)
				si.done = true
				return false
			}

			// Check ID match first.
			respID, _ := dec.GetUint64(transport.TagID)
			if respID != si.txID {
				continue
			}

			// Check for error (only after ID match).
			if dec.Has(transport.TagErr) {
				errMsg := dec.GetString(transport.TagErr)
				si.err = mapKVError(errMsg)
				si.done = true
				return false
			}

			// Extract key/value pair.
			key := dec.GetBytes(transport.TagKey)
			value := dec.GetBytes(transport.TagValue)
			if len(key) == 0 {
				// No more results.
				si.done = true
				return false
			}

			si.current = KVPair{Key: key, Value: value}
			return true
		}
	}
}

// Value returns the current item (valid only after a successful Next()).
func (si *scanIterator) Value() KVPair {
	si.mu.Lock()
	defer si.mu.Unlock()
	return si.current
}

// Err returns the first non-EOF error encountered.
func (si *scanIterator) Err() error {
	si.mu.Lock()
	defer si.mu.Unlock()
	return si.err
}

// Close releases any resources associated with the iterator.
func (si *scanIterator) Close() error {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.done = true
	if si.cancel != nil {
		si.cancel()
	}
	return nil
}

// Put sets a key/value pair (read/write transaction only).
func (t *transaction) Put(ctx context.Context, key []byte, value []byte) error {
	if t.readOnly {
		return fmt.Errorf("cannot mutate in read-only transaction")
	}
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Check transaction state.
	if t.committed.Load() {
		return fmt.Errorf("transaction already committed")
	}
	if t.rolledback.Load() {
		return fmt.Errorf("transaction already rolled back")
	}

	// Build and send PUT request immediately.
	enc := transport.NewTLVEncoder()
	enc.AddOp(KVPut)
	enc.AddString(transport.TagRoute, t.route.String())
	enc.AddBytes(transport.TagKey, key)
	enc.AddBytes(transport.TagBody, value)
	enc.AddUint64(transport.TagID, t.txID)

	frame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Flags:   0,
		Channel: transport.ChannelKV,
		Body:    enc.Encode(),
	}
	return t.mux.Send(frame)
}

// Insert sets a key/value pair only if the key does not exist.
func (t *transaction) Insert(ctx context.Context, key []byte, value []byte) error {
	if t.readOnly {
		return fmt.Errorf("cannot mutate in read-only transaction")
	}
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Check transaction state.
	if t.committed.Load() {
		return fmt.Errorf("transaction already committed")
	}
	if t.rolledback.Load() {
		return fmt.Errorf("transaction already rolled back")
	}

	// Build and send INSERT request immediately.
	enc := transport.NewTLVEncoder()
	enc.AddOp(KVInsert)
	enc.AddString(transport.TagRoute, t.route.String())
	enc.AddBytes(transport.TagKey, key)
	enc.AddBytes(transport.TagBody, value)
	enc.AddUint64(transport.TagID, t.txID)

	frame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Flags:   0,
		Channel: transport.ChannelKV,
		Body:    enc.Encode(),
	}
	return t.mux.Send(frame)
}

// Delete removes a key.
func (t *transaction) Delete(ctx context.Context, key []byte) error {
	if t.readOnly {
		return fmt.Errorf("cannot mutate in read-only transaction")
	}
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Check transaction state.
	if t.committed.Load() {
		return fmt.Errorf("transaction already committed")
	}
	if t.rolledback.Load() {
		return fmt.Errorf("transaction already rolled back")
	}

	// Build and send DELETE request immediately.
	enc := transport.NewTLVEncoder()
	enc.AddOp(KVDelete)
	enc.AddString(transport.TagRoute, t.route.String())
	enc.AddBytes(transport.TagKey, key)
	enc.AddUint64(transport.TagID, t.txID)

	frame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Flags:   0,
		Channel: transport.ChannelKV,
		Body:    enc.Encode(),
	}
	return t.mux.Send(frame)
}

// DeleteRange removes all keys in the range [startKey, endKey).
func (t *transaction) DeleteRange(ctx context.Context, startKey []byte, endKey []byte) (int, error) {
	if t.readOnly {
		return 0, fmt.Errorf("cannot mutate in read-only transaction")
	}
	if len(startKey) == 0 || len(endKey) == 0 {
		return 0, fmt.Errorf("startKey and endKey cannot be empty")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Check transaction state.
	if t.committed.Load() {
		return 0, fmt.Errorf("transaction already committed")
	}
	if t.rolledback.Load() {
		return 0, fmt.Errorf("transaction already rolled back")
	}

	// Build and send DELETE_RANGE request immediately.
	enc := transport.NewTLVEncoder()
	enc.AddOp(KVDeleteRange)
	enc.AddString(transport.TagRoute, t.route.String())
	enc.AddBytes(transport.TagStartKey, startKey)
	enc.AddBytes(transport.TagEndKey, endKey)
	enc.AddUint64(transport.TagID, t.txID)

	frame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Flags:   0,
		Channel: transport.ChannelKV,
		Body:    enc.Encode(),
	}
	if err := t.mux.Send(frame); err != nil {
		return 0, err
	}
	// Count returned by broker in response; for now, return 0.
	return 0, nil
}

// Commit marks the transaction as complete. For immediate-send design, this signals
// the broker that the transaction is done.
func (t *transaction) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// If already committed or rolled back, error.
	if t.committed.Load() {
		return fmt.Errorf("transaction already committed")
	}
	if t.rolledback.Load() {
		return fmt.Errorf("transaction already rolled back")
	}

	// For read-only transactions or if no mutations were sent, just mark as committed.
	if t.readOnly {
		t.committed.Store(true)
		return nil
	}

	// Send COMMIT signal to broker to finalize the transaction.
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, t.route.String())
	enc.AddUint64(transport.TagID, t.txID)
	// Include operation tag for commit so receivers can easily identify it.
	enc.AddOp(KVCommit)

	// Use KV-specific request frame type to match broker expectations.
	frame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Flags:   0,
		Channel: transport.ChannelKV,
		Body:    enc.Encode(),
	}

	if err := t.mux.Send(frame); err != nil {
		return err
	}

	// Wait for server acknowledgement for commit (best-effort, with timeout).
	ackCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		select {
		case <-ackCtx.Done():
			// Timeout — we cannot confirm the commit succeeded.
			return fmt.Errorf("commit timed out waiting for broker acknowledgement")
		case respFrame, ok := <-t.mux.In():
			if !ok {
				return fmt.Errorf("mux closed while waiting for commit ack")
			}
			if respFrame.Channel != transport.ChannelKV {
				continue
			}
			dec, err := transport.NewTLVDecoder(respFrame.Body)
			if err != nil {
				continue
			}
			if dec.Has(transport.TagErr) {
				return mapKVError(dec.GetString(transport.TagErr))
			}
			if id, _ := dec.GetUint64(transport.TagID); id == t.txID {
				// Commit acknowledged
				t.committed.Store(true)
				return nil
			}
		}
	}
}

// Rollback abandons the transaction. For immediate-send design, mutations already sent
// are at the broker's discretion to handle (typically abort pending txn).
func (t *transaction) Rollback(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// If already committed or rolled back, error.
	if t.committed.Load() {
		return fmt.Errorf("transaction already committed")
	}
	if t.rolledback.Load() {
		return fmt.Errorf("transaction already rolled back")
	}

	// Send ROLLBACK signal to broker using KV-specific frame type.
	enc := transport.NewTLVEncoder()
	enc.AddOp(KVRollback)
	enc.AddString(transport.TagRoute, t.route.String())
	enc.AddUint64(transport.TagID, t.txID)

	frame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Flags:   0,
		Channel: transport.ChannelKV,
		Body:    enc.Encode(),
	}

	_ = t.mux.Send(frame) // Best effort.

	t.rolledback.Store(true)
	return nil
}
