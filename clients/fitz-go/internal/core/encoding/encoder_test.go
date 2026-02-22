package encoding

import (
	"bytes"
	"testing"
)

// TestShouldEncodeU64GivenValidValue tests WriteU64 encoding.
func TestShouldEncodeU64GivenValidValue(t *testing.T) {
	// Arrange
	buf := &bytes.Buffer{}

	// Act
	WriteU64(buf, 0x0102030405060708)

	// Assert
	expected := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("Expected %v, got %v", expected, buf.Bytes())
	}
}

// TestShouldEncodeU32GivenValidValue tests WriteU32 encoding.
func TestShouldEncodeU32GivenValidValue(t *testing.T) {
	// Arrange
	buf := &bytes.Buffer{}

	// Act
	WriteU32(buf, 0x01020304)

	// Assert
	expected := []byte{0x01, 0x02, 0x03, 0x04}
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("Expected %v, got %v", expected, buf.Bytes())
	}
}

// TestShouldEncodeStringGivenValidString tests WriteString encoding.
func TestShouldEncodeStringGivenValidString(t *testing.T) {
	// Arrange
	buf := &bytes.Buffer{}

	// Act
	WriteString(buf, "test")

	// Assert
	expected := []byte{
		0x00, 0x00, 0x00, 0x04, // length = 4
		't', 'e', 's', 't',
	}
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("Expected %v, got %v", expected, buf.Bytes())
	}
}

// TestShouldEncodeBytesGivenValidData tests WriteBytes encoding.
func TestShouldEncodeBytesGivenValidData(t *testing.T) {
	// Arrange
	buf := &bytes.Buffer{}
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	// Act
	WriteBytes(buf, data)

	// Assert
	expected := []byte{
		0x00, 0x00, 0x00, 0x04, // length = 4
		0xDE, 0xAD, 0xBE, 0xEF,
	}
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("Expected %v, got %v", expected, buf.Bytes())
	}
}

// TestShouldEncodeRouteGivenValidRoute tests WriteRoute encoding.
func TestShouldEncodeRouteGivenValidRoute(t *testing.T) {
	// Arrange
	buf := &bytes.Buffer{}

	// Act
	WriteRoute(buf, "kv://realm/area/users")

	// Assert
	route := "kv://realm/area/users"
	expected := make([]byte, 4+len(route))
	expected[0] = 0x00
	expected[1] = 0x00
	expected[2] = 0x00
	expected[3] = byte(len(route))
	copy(expected[4:], route)

	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("Expected %v, got %v", expected, buf.Bytes())
	}
}

// TestShouldEncodeWithBufferGivenSimpleEncoding tests EncodeWithBuffer pattern.
func TestShouldEncodeWithBufferGivenSimpleEncoding(t *testing.T) {
	// Arrange & Act
	result := EncodeWithBuffer(func(buf *bytes.Buffer) {
		WriteU64(buf, 123)
		WriteString(buf, "test")
	})

	// Assert
	expected := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7B, // u64 = 123
		0x00, 0x00, 0x00, 0x04, // string length = 4
		't', 'e', 's', 't',
	}
	if !bytes.Equal(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// TestShouldReturnEmptySliceGivenNoData tests EncodeWithBuffer with no data.
func TestShouldReturnEmptySliceGivenNoData(t *testing.T) {
	// Arrange & Act
	result := EncodeWithBuffer(func(buf *bytes.Buffer) {
		// No writes
	})

	// Assert
	if len(result) != 0 {
		t.Errorf("Expected empty slice, got %v", result)
	}
}

// TestShouldWriteBytesRawGivenNoLengthPrefix tests WriteBytesRaw.
func TestShouldWriteBytesRawGivenNoLengthPrefix(t *testing.T) {
	// Arrange
	buf := &bytes.Buffer{}
	data := []byte{0x01, 0x02, 0x03}

	// Act
	WriteBytesRaw(buf, data)

	// Assert
	if !bytes.Equal(buf.Bytes(), data) {
		t.Errorf("Expected %v, got %v", data, buf.Bytes())
	}
}

// TestShouldEncodeComplexMessageGivenMultipleFields tests complex encoding.
func TestShouldEncodeComplexMessageGivenMultipleFields(t *testing.T) {
	// Arrange & Act
	result := EncodeWithBuffer(func(buf *bytes.Buffer) {
		WriteU64(buf, 42)                        // txID
		WriteRoute(buf, "kv://realm/area/users") // route
		WriteBytes(buf, []byte("key1"))          // key
		WriteBytes(buf, []byte("value1"))        // value
	})

	// Assert - verify structure
	if len(result) == 0 {
		t.Fatal("Expected non-empty result")
	}

	// Verify it starts with u64 (8 bytes for txID)
	if len(result) < 8 {
		t.Fatalf("Expected at least 8 bytes, got %d", len(result))
	}

	// Verify txID = 42
	txID := uint64(result[0])<<56 | uint64(result[1])<<48 | uint64(result[2])<<40 | uint64(result[3])<<32 |
		uint64(result[4])<<24 | uint64(result[5])<<16 | uint64(result[6])<<8 | uint64(result[7])
	if txID != 42 {
		t.Errorf("Expected txID=42, got %d", txID)
	}
}

// Benchmarks

func BenchmarkWriteU64(b *testing.B) {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteU64(buf, 0x0102030405060708)
	}
}

func BenchmarkWriteU32(b *testing.B) {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteU32(buf, 0x01020304)
	}
}

func BenchmarkWriteString(b *testing.B) {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	s := "route://acme/app/example"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteString(buf, s)
	}
}

func BenchmarkWriteBytes(b *testing.B) {
	small := []byte("payload")
	large := make([]byte, 4096)
	b.Run("small", func(b *testing.B) {
		buf := bytes.NewBuffer(make([]byte, 0, 64))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			WriteBytes(buf, small)
		}
	})
	b.Run("large", func(b *testing.B) {
		buf := bytes.NewBuffer(make([]byte, 0, 4100))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			WriteBytes(buf, large)
		}
	})
}

func BenchmarkWriteRoute(b *testing.B) {
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	route := "schedule://acme/jobs/backup/run"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteRoute(buf, route)
	}
}

func BenchmarkEncodeWithBuffer(b *testing.B) {
	route := "kv://acme/app/users"
	key := []byte("user:123")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeWithBuffer(func(buf *bytes.Buffer) {
			WriteU64(buf, 12345)
			WriteRoute(buf, route)
			WriteBytes(buf, key)
		})
	}
}

func BenchmarkEncodeWithBufferOwned(b *testing.B) {
	route := "kv://acme/app/users"
	key := []byte("user:123")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		owned := EncodeWithBufferOwned(func(buf *bytes.Buffer) {
			WriteU64(buf, 12345)
			WriteRoute(buf, route)
			WriteBytes(buf, key)
		})
		_ = owned.Bytes()
		owned.Release()
	}
}
