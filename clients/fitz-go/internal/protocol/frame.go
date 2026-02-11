package protocol

import (
	"encoding/binary"
	"fmt"
)

// Frame encoding/decoding per CLIENT_SPEC.md
// Wire format: [MessageType (variable 1-3 bytes)][Length (u16 BE)][Payload]
//
// MessageType encoding:
//   - If type <= 254: encoded as single byte
//   - If type > 254: escape byte 0xFF followed by u16 BE
//
// Length: u16 BE (max 65535 bytes)
// Payload: domain-specific concatenated fields

const (
	// MaxPayloadSize is the maximum allowed payload size per CLIENT_SPEC.md
	MaxPayloadSize = 65535

	// MessageTypeEscape is the escape byte for types > 254
	MessageTypeEscape = 0xFF
)

// EncodeMessageType encodes MessageType using variable-length encoding
// Per CLIENT_SPEC.md: types 0-254 = 1 byte, types 255+ = [0xFF][u16 BE]
func EncodeMessageType(msgType uint16) []byte {
	if msgType <= 254 {
		return []byte{byte(msgType)}
	}
	// Escape encoding for types 255+
	buf := make([]byte, 3)
	buf[0] = MessageTypeEscape
	binary.BigEndian.PutUint16(buf[1:3], msgType)
	return buf
}

// DecodeMessageType decodes variable-length MessageType
// Returns (messageType, bytesRead, error)
func DecodeMessageType(data []byte) (msgType uint16, bytesRead int, err error) {
	if len(data) < 1 {
		return 0, 0, fmt.Errorf("insufficient data for message type")
	}

	if data[0] != MessageTypeEscape {
		// Single-byte encoding
		return uint16(data[0]), 1, nil
	}

	// Escape encoding
	if len(data) < 3 {
		return 0, 0, fmt.Errorf("insufficient data for escaped message type")
	}
	msgType = binary.BigEndian.Uint16(data[1:3])
	return msgType, 3, nil
}

// EncodeFrame encodes a complete message frame
// Format: [MessageType (variable)][Length (u16 BE)][Payload]
func EncodeFrame(msgType uint16, payload []byte) []byte {
	if len(payload) > MaxPayloadSize {
		panic(fmt.Sprintf("payload too large: %d bytes (max %d)", len(payload), MaxPayloadSize))
	}

	msgTypeBytes := EncodeMessageType(msgType)
	frame := make([]byte, 0, len(msgTypeBytes)+2+len(payload))
	frame = append(frame, msgTypeBytes...)

	// Length (u16 BE)
	lengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(lengthBytes, uint16(len(payload)))
	frame = append(frame, lengthBytes...)

	// Payload
	frame = append(frame, payload...)
	return frame
}

// DecodeFrame decodes a message frame
// Returns (messageType, payload, error)
func DecodeFrame(data []byte) (msgType uint16, payload []byte, err error) {
	// Decode message type
	msgType, typeLen, err := DecodeMessageType(data)
	if err != nil {
		return 0, nil, fmt.Errorf("decode message type: %w", err)
	}

	offset := typeLen
	if len(data) < offset+2 {
		return 0, nil, fmt.Errorf("insufficient data for length field")
	}

	// Decode length
	length := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Extract payload
	if len(data) < offset+int(length) {
		return 0, nil, fmt.Errorf("insufficient data for payload: need %d, have %d", length, len(data)-offset)
	}

	payload = data[offset : offset+int(length)]
	return msgType, payload, nil
}

// EncodeTCPFrame encodes a frame for TCP transport
// Format: [Frame Length (u32 BE)][MessageType][Length][Payload]
// where Frame Length = total size of [MessageType][Length][Payload]
func EncodeTCPFrame(msgType uint16, payload []byte) []byte {
	// First encode the inner frame
	innerFrame := EncodeFrame(msgType, payload)

	// Prepend u32 BE length
	tcpFrame := make([]byte, 4+len(innerFrame))
	binary.BigEndian.PutUint32(tcpFrame[0:4], uint32(len(innerFrame)))
	copy(tcpFrame[4:], innerFrame)

	return tcpFrame
}

// DecodeTCPFrameLength reads the frame length prefix from TCP stream
// Returns the frame length (excluding the 4-byte prefix itself)
func DecodeTCPFrameLength(lengthPrefix []byte) (uint32, error) {
	if len(lengthPrefix) < 4 {
		return 0, fmt.Errorf("insufficient data for TCP frame length")
	}
	return binary.BigEndian.Uint32(lengthPrefix), nil
}
