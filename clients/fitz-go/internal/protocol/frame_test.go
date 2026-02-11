package protocol

import (
	"bytes"
	"testing"
)

func Test_EncodeMessageType_SingleByte(t *testing.T) {
	// Types 0-254 should encode as single byte
	tests := []struct {
		msgType  uint16
		expected []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{100, []byte{0x64}},
		{254, []byte{0xFE}},
	}

	for _, tt := range tests {
		result := EncodeMessageType(tt.msgType)
		if !bytes.Equal(result, tt.expected) {
			t.Errorf("EncodeMessageType(%d) = %v, want %v", tt.msgType, result, tt.expected)
		}
	}
}

func Test_EncodeMessageType_Escaped(t *testing.T) {
	// Types 255+ should encode with escape byte
	tests := []struct {
		msgType  uint16
		expected []byte
	}{
		{255, []byte{0xFF, 0x00, 0xFF}},
		{500, []byte{0xFF, 0x01, 0xF4}},
		{65535, []byte{0xFF, 0xFF, 0xFF}},
	}

	for _, tt := range tests {
		result := EncodeMessageType(tt.msgType)
		if !bytes.Equal(result, tt.expected) {
			t.Errorf("EncodeMessageType(%d) = %v, want %v", tt.msgType, result, tt.expected)
		}
	}
}

func Test_DecodeMessageType_SingleByte(t *testing.T) {
	tests := []struct {
		data         []byte
		expectedType uint16
		expectedLen  int
	}{
		{[]byte{0x01}, 1, 1},
		{[]byte{0x64}, 100, 1},
		{[]byte{0xFE}, 254, 1},
	}

	for _, tt := range tests {
		msgType, bytesRead, err := DecodeMessageType(tt.data)
		if err != nil {
			t.Errorf("DecodeMessageType(%v) unexpected error: %v", tt.data, err)
			continue
		}
		if msgType != tt.expectedType {
			t.Errorf("DecodeMessageType(%v) type = %d, want %d", tt.data, msgType, tt.expectedType)
		}
		if bytesRead != tt.expectedLen {
			t.Errorf("DecodeMessageType(%v) bytesRead = %d, want %d", tt.data, bytesRead, tt.expectedLen)
		}
	}
}

func Test_DecodeMessageType_Escaped(t *testing.T) {
	tests := []struct {
		data         []byte
		expectedType uint16
		expectedLen  int
	}{
		{[]byte{0xFF, 0x00, 0xFF}, 255, 3},
		{[]byte{0xFF, 0x01, 0xF4}, 500, 3},
		{[]byte{0xFF, 0x01, 0xF5}, 501, 3},
	}

	for _, tt := range tests {
		msgType, bytesRead, err := DecodeMessageType(tt.data)
		if err != nil {
			t.Errorf("DecodeMessageType(%v) unexpected error: %v", tt.data, err)
			continue
		}
		if msgType != tt.expectedType {
			t.Errorf("DecodeMessageType(%v) type = %d, want %d", tt.data, msgType, tt.expectedType)
		}
		if bytesRead != tt.expectedLen {
			t.Errorf("DecodeMessageType(%v) bytesRead = %d, want %d", tt.data, bytesRead, tt.expectedLen)
		}
	}
}

func Test_EncodeDecodeFrame_RoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		msgType uint16
		payload []byte
	}{
		{"empty payload", 100, []byte{}},
		{"small payload", 100, []byte("hello")},
		{"KV BEGIN", 100, []byte{0x00, 0x00, 0x00, 0x15 /* route */}},
		{"Notice SUBSCRIBE (escaped)", 501, []byte{0x00, 0x00, 0x00, 0x14 /* pattern */}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			frame := EncodeFrame(tt.msgType, tt.payload)

			// Decode
			decodedType, decodedPayload, err := DecodeFrame(frame)
			if err != nil {
				t.Fatalf("DecodeFrame() error: %v", err)
			}

			if decodedType != tt.msgType {
				t.Errorf("DecodeFrame() type = %d, want %d", decodedType, tt.msgType)
			}

			if !bytes.Equal(decodedPayload, tt.payload) {
				t.Errorf("DecodeFrame() payload = %v, want %v", decodedPayload, tt.payload)
			}
		})
	}
}

func Test_EncodeFrame_MatchesSpec(t *testing.T) {
	// Test KV BEGIN (MessageType=100) from CLIENT_SPEC.md Example 3
	// Payload: [route_len (4)][route (21)][mode (1)][durability (1)] = 27 bytes total
	payload := []byte{
		0x00, 0x00, 0x00, 0x15, // route_len=21
		// "kv://prod/app/users" (21 bytes)
		0x6b, 0x76, 0x3a, 0x2f, 0x2f, 0x70, 0x72, 0x6f,
		0x64, 0x2f, 0x61, 0x70, 0x70, 0x2f, 0x75, 0x73,
		0x65, 0x72, 0x73,
		0x01, // mode=1 (ReadWrite)
		0x01, // durability=1 (Sync)
	}

	frame := EncodeFrame(100, payload)

	// Expected frame:
	// [0x64] (MessageType=100, single byte)
	// [0x00 0x1B] (Length=27, which is 4+21+1+1)
	// [payload 27 bytes]
	if len(frame) < 3 {
		t.Fatal("frame too short")
	}

	if frame[0] != 0x64 {
		t.Errorf("MessageType byte = 0x%02X, want 0x64", frame[0])
	}

	payloadLen := int(frame[1])<<8 | int(frame[2])
	if payloadLen != len(payload) {
		t.Errorf("Length field = %d, want %d", payloadLen, len(payload))
	}

	if !bytes.Equal(frame[3:], payload) {
		t.Errorf("Payload mismatch")
	}
}

func Test_RouteDomain(t *testing.T) {
	tests := []struct {
		msgType uint16
		domain  string
	}{
		{100, "kv"},
		{108, "kv"},
		{200, "queue"},
		{204, "queue"},
		{300, "rpc"},
		{304, "rpc"},
		{400, "lease"},
		{403, "lease"},
		{500, "notice"},
		{504, "notice"},
		{600, "stream"},
		{603, "stream"},
		{700, "schedule"},
		{702, "schedule"},
		{999, "unknown"},
	}

	for _, tt := range tests {
		domain := RouteDomain(tt.msgType)
		if domain != tt.domain {
			t.Errorf("RouteDomain(%d) = %s, want %s", tt.msgType, domain, tt.domain)
		}
	}
}
