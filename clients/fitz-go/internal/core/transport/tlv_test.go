package transport

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func buildTLV(tag uint8, value []byte) []byte {
	enc := NewTLVEncoder()
	enc.AddTag(tag, value)
	return enc.Encode()
}

func TestShouldRejectDuplicateTLVTagWhenDecoding(t *testing.T) {
	// Arrange — manually construct raw bytes with duplicate TagToken
	b := make([]byte, 0)
	b = append(b, buildTLV(TagToken, []byte("abc"))...)
	b = append(b, buildTLV(TagToken, []byte("def"))...)

	// Act
	_, err := NewTLVDecoder(b)

	// Assert
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate TLV tag")
}

func TestShouldDecodeUint64AndStringWhenEncoded(t *testing.T) {
	// Arrange
	enc := NewTLVEncoder()
	enc.AddUint64(TagID, 0x0102030405060708)
	enc.AddString(TagRoute, "route1")
	b := enc.Encode()

	// Act
	dec, err := NewTLVDecoder(b)

	// Assert
	require.NoError(t, err)
	id, err := dec.GetUint64(TagID)
	require.NoError(t, err)
	require.Equal(t, uint64(0x0102030405060708), id)
	r := dec.GetString(TagRoute)
	require.Equal(t, "route1", r)
}

func TestShouldReturnErrorWhenDecodingTruncatedTLV(t *testing.T) {
	// Arrange
	b := []byte{TagRoute, 0x00, 0x05, 'a', 'b'}

	// Act
	_, err := NewTLVDecoder(b)

	// Assert
	require.Error(t, err)
}

func TestShouldPanicGivenDuplicateTagWhenAddTagCalled(t *testing.T) {
	// Arrange
	enc := NewTLVEncoder()
	enc.AddString(TagRoute, "a")

	// Act & Assert
	require.Panics(t, func() {
		enc.AddString(TagRoute, "b")
	})
}

func TestShouldReturnErrorGivenBadUint64LengthWhenGetUint64Called(t *testing.T) {
	// Arrange
	enc := NewTLVEncoder()
	enc.AddBytes(TagID, []byte{1, 2, 3})
	b := enc.Encode()

	// Act
	dec, err := NewTLVDecoder(b)

	// Assert
	require.NoError(t, err)
	_, err = dec.GetUint64(TagID)
	require.Error(t, err)
}

func TestShouldReturnValueWhenUint64LengthCorrectWhenGetUint64Called(t *testing.T) {
	// Arrange
	enc := NewTLVEncoder()
	enc.AddUint64(TagID, binary.BigEndian.Uint64([]byte{0, 0, 0, 0, 0, 0, 0, 1}))
	b := enc.Encode()

	// Act
	dec, err := NewTLVDecoder(b)

	// Assert
	require.NoError(t, err)
	v, err := dec.GetUint64(TagID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), v)
}

func TestShouldPanicGivenOversizedValueWhenAddTagCalled(t *testing.T) {
	// Arrange
	enc := NewTLVEncoder()
	hugeValue := make([]byte, int(MaxTLVValueLen)+1)

	// Act & Assert
	require.Panics(t, func() {
		enc.AddTag(TagBody, hugeValue)
	})
}
