package kv

import (
	"encoding/binary"
	"testing"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/stretchr/testify/require"
)

// TestShouldEncodeBeginGivenValidRoute tests BEGIN request encoding with route and mode.
func TestShouldEncodeBeginGivenValidRoute(t *testing.T) {
	// Arrange
	route := "kv://acme/app/users"
	mode := TxModeReadOnly
	durability := DurabilityBuffered

	// Act
	payload, err := EncodeBegin(route, mode, durability)

	// Assert
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	// Verify structure: [route_len][route][mode][durability]
	offset := 0
	routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
	require.NoError(t, err)
	require.Equal(t, uint32(len(route)), routeLen)

	offset = newOffset
	actualRoute := string(payload[offset : offset+int(routeLen)])
	require.Equal(t, route, actualRoute)

	offset += int(routeLen)
	require.Equal(t, uint8(TxModeReadOnly), payload[offset])
	require.Equal(t, uint8(DurabilityBuffered), payload[offset+1])
}

// TestShouldEncodeBeginGivenReadWriteMode tests BEGIN encoding with ReadWrite mode.
func TestShouldEncodeBeginGivenReadWriteMode(t *testing.T) {
	// Arrange
	route := "kv://acme/app/users"
	mode := TxModeReadWrite
	durability := DurabilitySync

	// Act
	payload, err := EncodeBegin(route, mode, durability)

	// Assert
	require.NoError(t, err)

	// Skip to mode/durability bytes
	routeLen, _, err := connection.ReadU32BE(payload, 0)
	require.NoError(t, err)
	offset := 4 + int(routeLen)

	require.Equal(t, uint8(TxModeReadWrite), payload[offset])
	require.Equal(t, uint8(DurabilitySync), payload[offset+1])
}

// TestShouldEncodeGetGivenValidKeyAndTxID tests GET request encoding.
func TestShouldEncodeGetGivenValidKeyAndTxID(t *testing.T) {
	// Arrange
	txID := uint64(12345)
	route := "kv://acme/app/users"
	key := []byte("user:123")

	// Act
	payload, err := EncodeGet(txID, route, key)

	// Assert
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	// Verify structure: [tx_id][route_len][route][key_len][key]
	offset := 0
	actualTxID, newOffset, err := connection.ReadU64BE(payload, offset)
	require.NoError(t, err)
	require.Equal(t, txID, actualTxID)

	offset = newOffset
	routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
	require.NoError(t, err)
	require.Equal(t, uint32(len(route)), routeLen)

	offset = newOffset
	actualRoute := string(payload[offset : offset+int(routeLen)])
	require.Equal(t, route, actualRoute)

	offset += int(routeLen)
	keyLen, newOffset, err := connection.ReadU32BE(payload, offset)
	require.NoError(t, err)
	require.Equal(t, uint32(len(key)), keyLen)

	offset = newOffset
	actualKey := payload[offset : offset+int(keyLen)]
	require.Equal(t, key, actualKey)
}

// TestShouldEncodePutGivenValidKeyAndValue tests PUT request encoding.
func TestShouldEncodePutGivenValidKeyAndValue(t *testing.T) {
	// Arrange
	txID := uint64(67890)
	route := "kv://acme/app/users"
	key := []byte("user:456")
	value := []byte(`{"name":"Alice"}`)

	// Act
	payload, err := EncodePut(txID, route, key, value)

	// Assert
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	// Verify structure: [tx_id][route_len][route][key_len][key][value_len][value]
	offset := 0
	actualTxID, newOffset, err := connection.ReadU64BE(payload, offset)
	require.NoError(t, err)
	require.Equal(t, txID, actualTxID)

	offset = newOffset
	routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
	require.NoError(t, err)

	offset = newOffset + int(routeLen) // Skip route
	keyLen, newOffset, err := connection.ReadU32BE(payload, offset)
	require.NoError(t, err)

	offset = newOffset + int(keyLen) // Skip key
	valueLen, newOffset, err := connection.ReadU32BE(payload, offset)
	require.NoError(t, err)
	require.Equal(t, uint32(len(value)), valueLen)

	offset = newOffset
	actualValue := payload[offset : offset+int(valueLen)]
	require.Equal(t, value, actualValue)
}

// TestShouldEncodeInsertGivenValidData tests INSERT request encoding.
func TestShouldEncodeInsertGivenValidData(t *testing.T) {
	// Arrange
	txID := uint64(11111)
	route := "kv://acme/app/users"
	key := []byte("new:key")
	value := []byte("new value")

	// Act
	payload, err := EncodeInsert(txID, route, key, value)

	// Assert
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	// Verify tx_id
	actualTxID, _, err := connection.ReadU64BE(payload, 0)
	require.NoError(t, err)
	require.Equal(t, txID, actualTxID)
}

// TestShouldEncodeDeleteGivenValidKey tests DELETE request encoding.
func TestShouldEncodeDeleteGivenValidKey(t *testing.T) {
	// Arrange
	txID := uint64(22222)
	route := "kv://acme/app/users"
	key := []byte("del:key")

	// Act
	payload, err := EncodeDelete(txID, route, key)

	// Assert
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	// Verify structure contains tx_id and key
	actualTxID, _, err := connection.ReadU64BE(payload, 0)
	require.NoError(t, err)
	require.Equal(t, txID, actualTxID)
}

// TestShouldEncodeDeleteRangeGivenValidRange tests DELETE_RANGE request encoding.
func TestShouldEncodeDeleteRangeGivenValidRange(t *testing.T) {
	// Arrange
	txID := uint64(33333)
	route := "kv://acme/app/users"
	startKey := []byte("range:start")
	endKey := []byte("range:end")

	// Act
	payload, err := EncodeDeleteRange(txID, route, startKey, endKey)

	// Assert
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	// Verify tx_id
	actualTxID, _, err := connection.ReadU64BE(payload, 0)
	require.NoError(t, err)
	require.Equal(t, txID, actualTxID)

	// Verify structure: [tx_id][route_len][route][start_key_len][start_key][end_key_len][end_key]
	// Skip to start_key after route
	offset := 8 // tx_id
	routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
	require.NoError(t, err)

	offset = newOffset + int(routeLen)
	startKeyLen, newOffset, err := connection.ReadU32BE(payload, offset)
	require.NoError(t, err)
	require.Equal(t, uint32(len(startKey)), startKeyLen)

	offset = newOffset + int(startKeyLen)
	endKeyLen, _, err := connection.ReadU32BE(payload, offset)
	require.NoError(t, err)
	require.Equal(t, uint32(len(endKey)), endKeyLen)
}

// TestShouldEncodeScanGivenFullRangeQuery tests SCAN request encoding with full range.
func TestShouldEncodeScanGivenFullRangeQuery(t *testing.T) {
	// Arrange
	txID := uint64(44444)
	route := "kv://acme/app/users"
	query := ScanQuery{
		StartKey: []byte("scan:a"),
		EndKey:   []byte("scan:z"),
		Limit:    100,
		Reverse:  false,
	}

	// Act
	payload, err := EncodeScan(txID, route, query)

	// Assert
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	// Verify tx_id is encoded correctly
	actualTxID, _, err := connection.ReadU64BE(payload, 0)
	require.NoError(t, err)
	require.Equal(t, txID, actualTxID)

	// Verify reverse flag (last byte)
	reverse := payload[len(payload)-1]
	require.Equal(t, uint8(0), reverse) // false = 0
}

// TestShouldEncodeScanGivenReverseQuery tests SCAN request encoding with reverse=true.
func TestShouldEncodeScanGivenReverseQuery(t *testing.T) {
	// Arrange
	txID := uint64(55555)
	route := "kv://acme/app/users"
	query := ScanQuery{
		StartKey: nil,
		EndKey:   nil,
		Limit:    50,
		Reverse:  true,
	}

	// Act
	payload, err := EncodeScan(txID, route, query)

	// Assert
	require.NoError(t, err)

	// Find reverse flag (last byte)
	reverse := payload[len(payload)-1]
	require.Equal(t, uint8(1), reverse) // true = 1
}

// TestShouldEncodeCommitGivenValidTxID tests COMMIT request encoding.
func TestShouldEncodeCommitGivenValidTxID(t *testing.T) {
	// Arrange
	txID := uint64(66666)
	route := "kv://acme/app/users"

	// Act
	payload, err := EncodeCommit(txID, route)

	// Assert
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	// Verify structure: [tx_id][route_len][route]
	offset := 0
	actualTxID, newOffset, err := connection.ReadU64BE(payload, offset)
	require.NoError(t, err)
	require.Equal(t, txID, actualTxID)

	offset = newOffset
	routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
	require.NoError(t, err)
	require.Greater(t, routeLen, uint32(0))

	offset = newOffset
	actualRoute := string(payload[offset : offset+int(routeLen)])
	require.Equal(t, route, actualRoute)
}

// TestShouldEncodeRollbackGivenValidTxID tests ROLLBACK request encoding.
func TestShouldEncodeRollbackGivenValidTxID(t *testing.T) {
	// Arrange
	txID := uint64(77777)
	route := "kv://acme/app/users"

	// Act
	payload, err := EncodeRollback(txID, route)

	// Assert
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	// Verify structure: [tx_id][route_len][route]
	actualTxID, _, err := connection.ReadU64BE(payload, 0)
	require.NoError(t, err)
	require.Equal(t, txID, actualTxID)
}

// TestShouldRejectOversizedKeyGivenKeyTooLarge tests ValidateKeySize enforcement.
func TestShouldRejectOversizedKeyGivenKeyTooLarge(t *testing.T) {
	// Arrange
	txID := uint64(88888)
	route := "kv://acme/app/users"
	key := make([]byte, MaxKeySize+1) // Exceed limit

	// Act
	_, err := EncodeGet(txID, route, key)

	// Assert
	require.Error(t, err)
	require.Contains(t, err.Error(), "key too large")
}

// TestShouldRejectOversizedValueGivenValueTooLarge tests ValidateValueSize enforcement.
func TestShouldRejectOversizedValueGivenValueTooLarge(t *testing.T) {
	// Arrange
	txID := uint64(99999)
	route := "kv://acme/app/users"
	key := []byte("user:999")
	value := make([]byte, MaxValueSize+1) // Exceed limit

	// Act
	_, err := EncodePut(txID, route, key, value)

	// Assert
	require.Error(t, err)
	require.Contains(t, err.Error(), "value too large")
}

// TestShouldAcceptMaxSizeKeyGivenExactLimit tests edge case at exact size limit.
func TestShouldAcceptMaxSizeKeyGivenExactLimit(t *testing.T) {
	// Arrange
	txID := uint64(10101)
	route := "kv://acme/app/users"
	key := make([]byte, MaxKeySize) // Exactly at limit

	// Act
	payload, err := EncodeGet(txID, route, key)

	// Assert
	require.NoError(t, err)
	require.NotEmpty(t, payload)
}

// TestShouldAcceptMaxSizeValueGivenExactLimit tests edge case at exact value size limit.
func TestShouldAcceptMaxSizeValueGivenExactLimit(t *testing.T) {
	// Arrange
	txID := uint64(20202)
	route := "kv://acme/app/users"
	key := []byte("user:max")
	value := make([]byte, MaxValueSize) // Exactly at limit

	// Act
	payload, err := EncodePut(txID, route, key, value)

	// Assert
	require.NoError(t, err)
	require.NotEmpty(t, payload)
}

// TestShouldRejectEmptyKeyGivenZeroLengthKey tests that empty keys are rejected.
func TestShouldRejectEmptyKeyGivenZeroLengthKey(t *testing.T) {
	// Arrange
	txID := uint64(30303)
	route := "kv://acme/app/users"
	key := []byte{} // Empty key

	// Act
	_, err := EncodeGet(txID, route, key)

	// Assert
	require.Error(t, err)
	require.Contains(t, err.Error(), "key cannot be empty")
}

// TestShouldEncodeEmptyValueGivenZeroLengthValue tests empty value handling (allowed).
func TestShouldEncodeEmptyValueGivenZeroLengthValue(t *testing.T) {
	// Arrange
	txID := uint64(40404)
	route := "kv://acme/app/users"
	key := []byte("empty:value")
	value := []byte{} // Empty value (allowed)

	// Act
	payload, err := EncodePut(txID, route, key, value)

	// Assert
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	// Verify value_len = 0 is encoded properly
	// Find value_len after tx_id(8), route_len(4), route, key_len(4), key
	offset := 8 // tx_id
	routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
	require.NoError(t, err)

	offset = newOffset + int(routeLen) // skip route
	keyLen, newOffset, err := connection.ReadU32BE(payload, offset)
	require.NoError(t, err)

	offset = newOffset + int(keyLen) // skip key
	valueLen, _, err := connection.ReadU32BE(payload, offset)
	require.NoError(t, err)
	require.Equal(t, uint32(0), valueLen)
}

// Benchmarks

func BenchmarkEncodeBegin(b *testing.B) {
	route := "kv://acme/app/users"
	mode := TxModeReadWrite
	durability := DurabilitySync

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeBegin(route, mode, durability)
	}
}

func BenchmarkEncodeGet(b *testing.B) {
	txID := uint64(12345)
	route := "kv://acme/app/users"
	key := []byte("user:123")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeGet(txID, route, key)
	}
}

func BenchmarkEncodePut(b *testing.B) {
	b.Run("small value", func(b *testing.B) {
		txID := uint64(12345)
		route := "kv://acme/app/users"
		key := []byte("user:456")
		value := []byte(`{"name":"Alice"}`)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodePut(txID, route, key, value)
		}
	})

	b.Run("large value", func(b *testing.B) {
		txID := uint64(12345)
		route := "kv://acme/app/users"
		key := []byte("user:456")
		value := make([]byte, 10000)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodePut(txID, route, key, value)
		}
	})
}

func BenchmarkEncodeDelete(b *testing.B) {
	txID := uint64(22222)
	route := "kv://acme/app/users"
	key := []byte("del:key")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeDelete(txID, route, key)
	}
}

func BenchmarkEncodeScan(b *testing.B) {
	txID := uint64(44444)
	route := "kv://acme/app/users"
	query := ScanQuery{
		StartKey: []byte("scan:a"),
		EndKey:   []byte("scan:z"),
		Limit:    100,
		Reverse:  false,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeScan(txID, route, query)
	}
}

func BenchmarkEncodeCommit(b *testing.B) {
	txID := uint64(66666)
	route := "kv://acme/app/users"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeCommit(txID, route)
	}
}

func BenchmarkEncodeRollback(b *testing.B) {
	txID := uint64(77777)
	route := "kv://acme/app/users"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeRollback(txID, route)
	}
}

func BenchmarkParseBeginResponse(b *testing.B) {
	// [status=0][u64 BE tx_id]
	payload := make([]byte, 1+8)
	payload[0] = 0
	binary.BigEndian.PutUint64(payload[1:9], 12345)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		success, remaining, _ := connection.ParseStandardResponse(payload)
		if success && len(remaining) >= 8 {
			_, _, _ = connection.ReadU64BE(remaining, 0)
		}
	}
}

func BenchmarkParseScanResponse(b *testing.B) {
	// [item_count=1][key_len=3][key][value_len=5][value][has_more=0]
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	connection.WriteU32BE(buf, 1)
	connection.WriteBytes(buf, []byte("key"))
	connection.WriteBytes(buf, []byte("value"))
	buf.WriteByte(0)
	payload := make([]byte, buf.Len())
	copy(payload, buf.Bytes())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = parseScanResponse(payload)
	}
}
