package connection_test

import (
	"context"
	"testing"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShouldCreateConnectionGivenValidConfigWhenCalled tests basic connection creation.
func TestShouldCreateConnectionGivenValidConfigWhenCalled(t *testing.T) {
	// Arrange
	transport := &mockTransport{}
	cfg := connection.DefaultConfig()

	// Act
	conn := connection.New(transport, cfg)

	// Assert
	require.NotNil(t, conn)
}

// mockTransport is a simple mock for testing.
type mockTransport struct {
	readFrames  [][]byte
	writeFrames [][]byte
}

func (m *mockTransport) Write(ctx context.Context, frame []byte) error {
	m.writeFrames = append(m.writeFrames, frame)
	return nil
}

func (m *mockTransport) Read(ctx context.Context) ([]byte, error) {
	// Block until context cancelled for now
	<-ctx.Done()
	return nil, ctx.Err()
}

func (m *mockTransport) Close() error {
	return nil
}

func (m *mockTransport) RemoteAddr() string {
	return "mock://test"
}

// TestShouldEncodeDecodeRequestResponseGivenValidPayloadWhenCalled tests response helpers.
func TestShouldParseStandardResponseGivenSuccessStatusWhenCalled(t *testing.T) {
	// Arrange - Success response: [status=0][remaining data]
	payload := []byte{0x00, 0x01, 0x02, 0x03}

	// Act
	success, remaining, err := connection.ParseStandardResponse(payload)

	// Assert
	require.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, remaining)
}

// TestShouldReturnErrorGivenErrorStatusWhenParsingResponse tests error response parsing.
func TestShouldReturnErrorGivenErrorStatusWhenParsingResponse(t *testing.T) {
	// Arrange - Error response: [status=1][u32 BE len][error message]
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteU8(buf, 1) // Error status
	connection.WriteString(buf, "test error message")
	payload := buf.Bytes()

	// Act
	success, _, err := connection.ParseStandardResponse(payload)

	//Assert
	require.Error(t, err)
	assert.False(t, success)
	assert.Contains(t, err.Error(), "test error message")
}

// TestShouldMatchResponsesInFIFOOrderGivenMultiplexerWhenDispatched tests FIFO correlation.
func TestShouldMatchResponsesInFIFOOrderGivenMultiplexerWhenDispatched(t *testing.T) {
	// Arrange
	mux := connection.NewMultiplexer()
	defer mux.Close()

	// Register 3 requests for same MessageType
	resp1 := make(chan []byte, 1)
	resp2 := make(chan []byte, 1)
	resp3 := make(chan []byte, 1)

	mux.RegisterRequest(100, resp1, nil)
	mux.RegisterRequest(100, resp2, nil)
	mux.RegisterRequest(100, resp3, nil)

	// Act - Dispatch responses
	mux.Dispatch(100, []byte("response_1"))
	mux.Dispatch(100, []byte("response_2"))
	mux.Dispatch(100, []byte("response_3"))

	// Assert - Verify FIFO order
	assert.Equal(t, []byte("response_1"), <-resp1)
	assert.Equal(t, []byte("response_2"), <-resp2)
	assert.Equal(t, []byte("response_3"), <-resp3)
}

// TestShouldReadAndWriteHelpersGivenValidDataWhenCalled tests encoding helpers.
func TestShouldEncodeDecodeU32BEGivenValidValueWhenCalled(t *testing.T) {
	// Arrange
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	expectedValue := uint32(0x12345678)

	// Act - Write
	connection.WriteU32BE(buf, expectedValue)

	// Act - Read
	actualValue, _, err := connection.ReadU32BE(buf.Bytes(), 0)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, expectedValue, actualValue)
}

// TestShouldEncodeDecodeStringGivenValidDataWhenCalled tests string encoding.
func TestShouldEncodeDecodeStringGivenValidDataWhenCalled(t *testing.T) {
	// Arrange
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	expectedString := "test string with special chars: ñ 测试"

	// Act - Write
	connection.WriteString(buf, expectedString)

	// Act - Read
	actualString, _, err := connection.ReadString(buf.Bytes(), 0)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, expectedString, actualString)
}

// TestShouldReturnMetricsGivenMultiplexerWhenCalled tests metrics collection.
func TestShouldReturnMetricsGivenMultiplexerWhenCalled(t *testing.T) {
	// Arrange
	mux := connection.NewMultiplexer()
	defer mux.Close()

	respChan := make(chan []byte, 1)
	mux.RegisterRequest(100, respChan, nil)

	// Act
	metrics := mux.Metrics()

	// Assert
	assert.Equal(t, int64(1), metrics.RequestsInFlight)
	assert.Equal(t, uint64(1), metrics.RequestsTotal)
}
