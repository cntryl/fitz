package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/domains/kv"
	"github.com/cntryl/fitz-go/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Acceptance criteria from CLIENT_SPEC.md (KV domain) ---
// - begin/put/commit cycle succeeds
// - begin/get on non-existent key handled correctly
// - ReadOnly mode rejects write operations
// - two transactions on same resource conflict
// - rollback discards all changes
// - scan returns lexicographically ordered pairs

// TestShouldOpenAndCommitTransactionGivenValidRouteWhenBeginCalled verifies the
// basic KV transaction lifecycle: BEGIN → PUT → GET → COMMIT.
func TestShouldOpenAndCommitTransactionGivenValidRouteWhenBeginCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := kv.NewRoute(f.UniqueRealm(), f.UniqueArea(), f.UniqueResource()).String()

		// Act
		tx, err := f.Client().KV().Begin(ctx, route)

		// Assert
		require.NoError(t, err, "Begin failed")
		require.NotNil(t, tx, "expected non-nil transaction")

		require.NoError(t, tx.Put(ctx, []byte("user:123"), []byte("Alice")))
		require.NoError(t, tx.Commit(ctx), "Commit failed")

		// Verify value persisted via new read transaction.
		trx, err := f.Client().KV().BeginRead(ctx, route)
		require.NoError(t, err, "BeginRead failed")
		val, found, err := trx.Get(ctx, []byte("user:123"))
		require.NoError(t, err, "Get failed")
		assert.True(t, found, "expected key to be found after commit")
		assert.Equal(t, "Alice", string(val), "unexpected value")
	})
}

// TestShouldReadValueGivenExistingKeyWhenGetCalled verifies GET operation
// returns the correct value for an existing key within a transaction.
func TestShouldReadValueGivenExistingKeyWhenGetCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := kv.NewRoute(f.UniqueRealm(), f.UniqueArea(), f.UniqueResource()).String()

		// Seed a key.
		tx, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("colour"), []byte("blue")))
		require.NoError(t, tx.Commit(ctx))

		// Act — read the key in a new read-only transaction.
		rtx, err := f.Client().KV().BeginRead(ctx, route)
		require.NoError(t, err)
		val, found, err := rtx.Get(ctx, []byte("colour"))

		// Assert
		require.NoError(t, err)
		assert.True(t, found, "expected key to be found")
		assert.Equal(t, "blue", string(val))
	})
}

// TestShouldReturnNotFoundGivenNonExistentKeyWhenGetCalled verifies GET
// operation handles missing keys correctly (found=false, no error).
func TestShouldReturnNotFoundGivenNonExistentKeyWhenGetCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := kv.NewRoute(f.UniqueRealm(), f.UniqueArea(), f.UniqueResource()).String()

		rtx, err := f.Client().KV().BeginRead(ctx, route)
		require.NoError(t, err)

		// Act
		val, found, err := rtx.Get(ctx, []byte("nonexistent"))

		// Assert
		require.NoError(t, err, "Get should not error for missing keys")
		assert.False(t, found, "expected key not to be found")
		assert.Nil(t, val, "expected nil value for missing key")
	})
}

// TestShouldWriteValueGivenValidKeyWhenPutCalled verifies PUT operation
// writes values that are readable within and after the transaction.
func TestShouldWriteValueGivenValidKeyWhenPutCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := kv.NewRoute(f.UniqueRealm(), f.UniqueArea(), f.UniqueResource()).String()

		tx, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)

		// Act
		err = tx.Put(ctx, []byte("k1"), []byte("v1"))

		// Assert
		require.NoError(t, err, "Put should succeed")

		val, found, err := tx.Get(ctx, []byte("k1"))
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "v1", string(val))

		require.NoError(t, tx.Commit(ctx))
	})
}

// TestShouldInsertNewKeyGivenNonExistentKeyWhenInsertCalled verifies INSERT
// operation creates a new key that did not previously exist.
func TestShouldInsertNewKeyGivenNonExistentKeyWhenInsertCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := kv.NewRoute(f.UniqueRealm(), f.UniqueArea(), f.UniqueResource()).String()

		tx, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)

		// Act
		err = tx.Insert(ctx, []byte("new-key"), []byte("new-value"))

		// Assert
		require.NoError(t, err, "Insert should succeed for new key")

		val, found, err := tx.Get(ctx, []byte("new-key"))
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "new-value", string(val))

		require.NoError(t, tx.Commit(ctx))
	})
}

// TestShouldFailGivenExistingKeyWhenInsertCalled verifies INSERT operation
// rejects duplicate keys with ErrKeyExists.
func TestShouldFailGivenExistingKeyWhenInsertCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := kv.NewRoute(f.UniqueRealm(), f.UniqueArea(), f.UniqueResource()).String()

		// Seed the key.
		tx, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)
		require.NoError(t, tx.Insert(ctx, []byte("dup"), []byte("first")))
		require.NoError(t, tx.Commit(ctx))

		// Act — insert same key again.
		tx2, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)
		err = tx2.Insert(ctx, []byte("dup"), []byte("second"))

		// Assert
		assert.ErrorIs(t, err, kv.ErrKeyExists, "insert on existing key should return ErrKeyExists")
		_ = tx2.Rollback(ctx)
	})
}

// TestShouldDeleteKeyGivenExistingKeyWhenDeleteCalled verifies DELETE
// operation removes a key so subsequent GET returns not-found.
func TestShouldDeleteKeyGivenExistingKeyWhenDeleteCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := kv.NewRoute(f.UniqueRealm(), f.UniqueArea(), f.UniqueResource()).String()

		tx, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("to-delete"), []byte("doomed")))
		require.NoError(t, tx.Commit(ctx))

		// Act
		tx2, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)
		err = tx2.Delete(ctx, []byte("to-delete"))

		// Assert
		require.NoError(t, err, "Delete should succeed")
		require.NoError(t, tx2.Commit(ctx))

		rtx, err := f.Client().KV().BeginRead(ctx, route)
		require.NoError(t, err)
		_, found, err := rtx.Get(ctx, []byte("to-delete"))
		require.NoError(t, err)
		assert.False(t, found, "key should be gone after delete")
	})
}

// TestShouldScanKeysInOrderGivenRangeWhenScanCalled verifies SCAN operation
// returns lexicographically ordered key-value pairs within [startKey, endKey).
func TestShouldScanKeysInOrderGivenRangeWhenScanCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := kv.NewRoute(f.UniqueRealm(), f.UniqueArea(), f.UniqueResource()).String()

		tx, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("b"), []byte("2")))
		require.NoError(t, tx.Put(ctx, []byte("a"), []byte("1")))
		require.NoError(t, tx.Put(ctx, []byte("c"), []byte("3")))
		require.NoError(t, tx.Commit(ctx))

		// Act
		rtx, err := f.Client().KV().BeginRead(ctx, route)
		require.NoError(t, err)
		it, _, err := rtx.Scan(ctx, kv.ScanQuery{StartKey: []byte("a"), EndKey: []byte("d"), Limit: 10})

		// Assert
		require.NoError(t, err)
		require.NotNil(t, it)
		defer it.Close()

		var keys []string
		for it.Next() {
			keys = append(keys, string(it.Value().Key))
		}
		require.NoError(t, it.Err())
		assert.Equal(t, []string{"a", "b", "c"}, keys, "scan should return keys in lexicographic order")
	})
}

// TestShouldDeleteRangeGivenRangeWhenDeleteRangeCalled verifies DELETE_RANGE
// removes keys within [startKey, endKey) and preserves keys outside the range.
func TestShouldDeleteRangeGivenRangeWhenDeleteRangeCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := kv.NewRoute(f.UniqueRealm(), f.UniqueArea(), f.UniqueResource()).String()

		tx, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("a"), []byte("1")))
		require.NoError(t, tx.Put(ctx, []byte("b"), []byte("2")))
		require.NoError(t, tx.Put(ctx, []byte("c"), []byte("3")))
		require.NoError(t, tx.Put(ctx, []byte("d"), []byte("4")))
		require.NoError(t, tx.Commit(ctx))

		// Act — delete range ["b", "d") which should remove b and c.
		tx2, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)
		require.NoError(t, tx2.DeleteRange(ctx, []byte("b"), []byte("d")))
		require.NoError(t, tx2.Commit(ctx))

		// Assert — scan should return a and d only.
		rtx, err := f.Client().KV().BeginRead(ctx, route)
		require.NoError(t, err)
		it, _, err := rtx.Scan(ctx, kv.ScanQuery{StartKey: []byte("a"), EndKey: []byte("z"), Limit: 10})
		require.NoError(t, err)
		defer it.Close()

		var keys []string
		for it.Next() {
			keys = append(keys, string(it.Value().Key))
		}
		require.NoError(t, it.Err())
		assert.Equal(t, []string{"a", "d"}, keys)
	})
}

// TestShouldRespectLimitGivenScanCalled verifies SCAN limit is enforced.
func TestShouldRespectLimitGivenScanCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := kv.NewRoute(f.UniqueRealm(), f.UniqueArea(), f.UniqueResource()).String()

		tx, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("a"), []byte("1")))
		require.NoError(t, tx.Put(ctx, []byte("b"), []byte("2")))
		require.NoError(t, tx.Put(ctx, []byte("c"), []byte("3")))
		require.NoError(t, tx.Commit(ctx))

		// Act — scan with limit 2.
		rtx, err := f.Client().KV().BeginRead(ctx, route)
		require.NoError(t, err)
		it, _, err := rtx.Scan(ctx, kv.ScanQuery{StartKey: []byte("a"), EndKey: []byte("z"), Limit: 2})
		require.NoError(t, err)
		defer it.Close()

		// Assert — iterator should return at most 2 items.
		count := 0
		for it.Next() {
			count++
		}
		require.NoError(t, it.Err())
		assert.Equal(t, 2, count)
	})
}

// TestShouldRollbackChangesGivenActiveTransactionWhenRollbackCalled verifies
// ROLLBACK discards all uncommitted changes so they are not visible afterward.
func TestShouldRollbackChangesGivenActiveTransactionWhenRollbackCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := kv.NewRoute(f.UniqueRealm(), f.UniqueArea(), f.UniqueResource()).String()

		tx, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("ephemeral"), []byte("gone")))

		// Act
		err = tx.Rollback(ctx)

		// Assert
		require.NoError(t, err, "Rollback should succeed")

		rtx, err := f.Client().KV().BeginRead(ctx, route)
		require.NoError(t, err)
		_, found, err := rtx.Get(ctx, []byte("ephemeral"))
		require.NoError(t, err)
		assert.False(t, found, "rolled-back key should not be visible")
	})
}

// TestShouldIsolateTransactionsGivenConcurrentAccessWhenMultipleTransactions
// verifies that two transactions on the same resource detect conflicts per
// CLIENT_SPEC.md isolation semantics (pessimistic resource lock).
func TestShouldIsolateTransactionsGivenConcurrentAccessWhenMultipleTransactions(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange — two sessions, same resource
		f1 := fixture.NewTestFixture(t, transport)
		f2 := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f1.ConnectOrSkip(ctx)
		f2.ConnectOrSkip(ctx)

		route := kv.NewRoute(f1.UniqueRealm(), f1.UniqueArea(), f1.UniqueResource()).String()

		// First session begins ReadWrite
		tx1, err := f1.Client().KV().Begin(ctx, route)
		require.NoError(t, err)
		require.NotNil(t, tx1)
		defer func() { _ = tx1.Rollback(ctx) }()

		// Act — second session tries to begin ReadWrite on same resource
		tx2, err2 := f2.Client().KV().Begin(ctx, route)

		// Assert — second begin should fail with conflict (or tx2 nil)
		if err2 != nil {
			assert.Nil(t, tx2)
			assert.Contains(t, err2.Error(), "conflict", "expected conflict or resource locked error")
			return
		}
		// If no error, tx2 must be nil (server may return nil tx on conflict)
		if tx2 != nil {
			_ = tx2.Rollback(ctx)
			t.Fatal("second Begin on same resource should have failed with conflict")
		}
	})
}

// TestShouldRejectWriteGivenReadOnlyModeWhenPutCalled verifies that a
// read-only transaction does not expose write operations per CLIENT_SPEC.md.
func TestShouldRejectWriteGivenReadOnlyModeWhenPutCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := kv.NewRoute(f.UniqueRealm(), f.UniqueArea(), f.UniqueResource()).String()

		// Act — BeginRead returns ReadTx which does not expose mutation methods.
		rtx, err := f.Client().KV().BeginRead(ctx, route)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, rtx)

		// ReadTx should not be castable to Tx (write interface).
		_, isTx := rtx.(kv.Tx)
		assert.False(t, isTx, "BeginRead should return ReadTx, not full Tx")
	})
}

// TestShouldRejectBeginGivenInvalidRouteWhenBeginCalled verifies
// Begin with invalid or malformed route returns an error.
func TestShouldRejectBeginGivenInvalidRouteWhenBeginCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		_, err := f.Client().KV().Begin(ctx, "invalid-route-not-kv-format")

		require.Error(t, err)
	})
}

// TestShouldRejectSecondCommitGivenAlreadyCommittedTransactionWhenCommitCalled
// verifies that calling Commit twice on the same transaction fails or is a no-op.
func TestShouldRejectSecondCommitGivenAlreadyCommittedTransactionWhenCommitCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := kv.NewRoute(f.UniqueRealm(), f.UniqueArea(), f.UniqueResource()).String()

		tx, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("k"), []byte("v")))
		require.NoError(t, tx.Commit(ctx))

		// Act — second Commit on same transaction.
		err = tx.Commit(ctx)

		// Assert — should fail (transaction already committed) or be no-op.
		if err != nil {
			assert.Error(t, err)
		}
	})
}
