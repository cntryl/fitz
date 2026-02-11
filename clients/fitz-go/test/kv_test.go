package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/internal/domains/kv"
	"github.com/cntryl/cntryl-go/test/fixture"
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

		route := kv.Route{
			Realm:    f.UniqueRealm(),
			Area:     f.UniqueArea(),
			Resource: f.UniqueResource(),
		}

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

		route := kv.Route{
			Realm:    f.UniqueRealm(),
			Area:     f.UniqueArea(),
			Resource: f.UniqueResource(),
		}

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

		route := kv.Route{
			Realm:    f.UniqueRealm(),
			Area:     f.UniqueArea(),
			Resource: f.UniqueResource(),
		}

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

		route := kv.Route{
			Realm:    f.UniqueRealm(),
			Area:     f.UniqueArea(),
			Resource: f.UniqueResource(),
		}

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

		route := kv.Route{
			Realm:    f.UniqueRealm(),
			Area:     f.UniqueArea(),
			Resource: f.UniqueResource(),
		}

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

		route := kv.Route{
			Realm:    f.UniqueRealm(),
			Area:     f.UniqueArea(),
			Resource: f.UniqueResource(),
		}

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

		route := kv.Route{
			Realm:    f.UniqueRealm(),
			Area:     f.UniqueArea(),
			Resource: f.UniqueResource(),
		}

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

		route := kv.Route{
			Realm:    f.UniqueRealm(),
			Area:     f.UniqueArea(),
			Resource: f.UniqueResource(),
		}

		tx, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("b"), []byte("2")))
		require.NoError(t, tx.Put(ctx, []byte("a"), []byte("1")))
		require.NoError(t, tx.Put(ctx, []byte("c"), []byte("3")))
		require.NoError(t, tx.Commit(ctx))

		// Act
		rtx, err := f.Client().KV().BeginRead(ctx, route)
		require.NoError(t, err)
		it, err := rtx.Scan(ctx, []byte("a"), []byte("d"), 10)

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

// TestShouldRollbackChangesGivenActiveTransactionWhenRollbackCalled verifies
// ROLLBACK discards all uncommitted changes so they are not visible afterward.
func TestShouldRollbackChangesGivenActiveTransactionWhenRollbackCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := kv.Route{
			Realm:    f.UniqueRealm(),
			Area:     f.UniqueArea(),
			Resource: f.UniqueResource(),
		}

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
// CLIENT_SPEC.md isolation semantics.
func TestShouldIsolateTransactionsGivenConcurrentAccessWhenMultipleTransactions(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := kv.Route{
			Realm:    f.UniqueRealm(),
			Area:     f.UniqueArea(),
			Resource: f.UniqueResource(),
		}

		// Act — open two concurrent write transactions on the same resource.
		tx1, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)
		tx2, err := f.Client().KV().Begin(ctx, route)
		require.NoError(t, err)

		require.NoError(t, tx1.Put(ctx, []byte("key"), []byte("tx1")))
		require.NoError(t, tx2.Put(ctx, []byte("key"), []byte("tx2")))

		// Commit tx1 first.
		err1 := tx1.Commit(ctx)
		// Commit tx2 — should conflict.
		err2 := tx2.Commit(ctx)

		// Assert — at least one should succeed, the other should conflict.
		if err1 == nil {
			assert.Error(t, err2, "second concurrent commit should conflict")
		} else {
			assert.NoError(t, err2, "if tx1 conflicted, tx2 should succeed")
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

		route := kv.Route{
			Realm:    f.UniqueRealm(),
			Area:     f.UniqueArea(),
			Resource: f.UniqueResource(),
		}

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
