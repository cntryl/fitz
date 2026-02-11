package integration

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/transport"
)

// TestDiagBrokerRawFrames is a diagnostic test that connects to the broker
// at the raw transport level and dumps frames to help debug protocol issues.
// This is NOT a normal test — it's a debugging aid.
func TestDiagBrokerRawFrames(t *testing.T) {
	addr := "localhost:4091"

	// 1. TCP connect
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Skipf("broker not reachable at %s: %v", addr, err)
	}
	defer conn.Close()
	t.Logf("TCP connected to %s", addr)

	framer := transport.NewTCPFramer(conn)
	mux := transport.NewMux(framer)
	mux.Start()

	// 2. Send CONNECT frame (empty token = anonymous)
	connectFrame := transport.Frame{
		Type:    transport.FrameTypeConnOpen,
		Flags:   0,
		Channel: transport.ChannelControl,
		Body:    []byte(""),
	}
	if err := mux.Send(connectFrame); err != nil {
		t.Fatalf("failed to send CONNECT: %v", err)
	}
	t.Logf("CONNECT sent (type=%d, channel=%d)", connectFrame.Type, connectFrame.Channel)

	// 3. Wait for any frames from broker after CONNECT
	t.Log("--- Waiting 1s for frames after CONNECT ---")
	drainFrames(t, mux.In(), 1*time.Second)

	// 4. Send a simple Notice PUBLISH (op code 500, channel=ChannelPub=1)
	route := "notice://diag/test/ping"
	enc := transport.NewTLVEncoder()
	enc.AddOp(500) // NoticePublish
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagBody, []byte("hello"))
	pubFrame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Flags:   0,
		Channel: transport.ChannelPub,
		Body:    enc.Encode(),
	}
	if err := mux.Send(pubFrame); err != nil {
		t.Fatalf("failed to send PUBLISH: %v", err)
	}
	t.Logf("PUBLISH sent (type=%d, channel=%d, route=%s, bodyHex=%s)", pubFrame.Type, pubFrame.Channel, route, hex.EncodeToString(pubFrame.Body))

	// 5. Wait for response
	t.Log("--- Waiting 1s for frames after PUBLISH ---")
	drainFrames(t, mux.In(), 1*time.Second)

	// 6. Send a Lease ACQUIRE (op code 400, channel=ChannelLease=4)
	enc2 := transport.NewTLVEncoder()
	enc2.AddOp(400) // LeaseAcquire
	enc2.AddString(transport.TagRoute, "lease://diag/test/lock1")
	enc2.AddUint32(transport.TagTTL, 30)
	leaseFrame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Flags:   0,
		Channel: transport.ChannelLease,
		Body:    enc2.Encode(),
	}
	if err := mux.Send(leaseFrame); err != nil {
		t.Fatalf("failed to send LEASE ACQUIRE: %v", err)
	}
	t.Logf("LEASE ACQUIRE sent (type=%d, channel=%d, bodyHex=%s)", leaseFrame.Type, leaseFrame.Channel, hex.EncodeToString(leaseFrame.Body))

	// 7. Wait for response
	t.Log("--- Waiting 1s for frames after LEASE ACQUIRE ---")
	drainFrames(t, mux.In(), 1*time.Second)

	// 8. Try with FrameTypeReq wrapping
	enc3 := transport.NewTLVEncoder()
	enc3.AddString(transport.TagRoute, "lease://diag/test/lock2")
	enc3.AddUint32(transport.TagTTL, 30)
	enc3.AddUint8(0x10, 40) // embed the op code as a TLV tag?
	leaseFrame2 := transport.Frame{
		Type:    transport.FrameTypeReq,
		Flags:   0,
		Channel: transport.ChannelLease,
		Body:    enc3.Encode(),
	}
	if err := mux.Send(leaseFrame2); err != nil {
		t.Fatalf("failed to send LEASE via FrameTypeReq: %v", err)
	}
	t.Logf("LEASE via FrameTypeReq sent (type=%d, channel=%d, bodyHex=%s)", leaseFrame2.Type, leaseFrame2.Channel, hex.EncodeToString(leaseFrame2.Body))

	t.Log("--- Waiting 1s for frames after LEASE via FrameTypeReq ---")
	drainFrames(t, mux.In(), 1*time.Second)

	_ = mux.Close()
}

func drainFrames(t *testing.T, in <-chan transport.Frame, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	count := 0
	for {
		select {
		case f, ok := <-in:
			if !ok {
				t.Logf("  mux channel closed after %d frames", count)
				return
			}
			count++
			bodyStr := hex.EncodeToString(f.Body)
			if len(bodyStr) > 200 {
				bodyStr = bodyStr[:200] + "..."
			}
			// Try to decode TLV fields for readability
			tlvStr := decodeTLVSummary(f.Body)
			t.Logf("  FRAME[%d]: type=%d flags=0x%02x channel=%d bodyLen=%d body=%s tlv=%s",
				count, f.Type, f.Flags, f.Channel, len(f.Body), bodyStr, tlvStr)
		case <-ctx.Done():
			t.Logf("  timeout after %v, received %d frames", timeout, count)
			return
		}
	}
}

func decodeTLVSummary(body []byte) string {
	dec, err := transport.NewTLVDecoder(body)
	if err != nil {
		return fmt.Sprintf("(not valid TLV: %v)", err)
	}
	summary := ""
	// Try to read known tags
	if v := dec.GetString(transport.TagRoute); v != "" {
		summary += fmt.Sprintf(" route=%q", v)
	}
	if v := dec.GetString(transport.TagErr); v != "" {
		summary += fmt.Sprintf(" err=%q", v)
	}
	if v := dec.GetBytes(transport.TagBody); len(v) > 0 {
		summary += fmt.Sprintf(" body=%q", string(v))
	}
	if v := dec.GetBytes(transport.TagLease); len(v) > 0 {
		summary += fmt.Sprintf(" lease=%s", hex.EncodeToString(v))
	}
	if v, err := dec.GetUint64(transport.TagID); err == nil {
		summary += fmt.Sprintf(" id=%d", v)
	}
	if v, err := dec.GetUint32(transport.TagTTL); err == nil {
		summary += fmt.Sprintf(" ttl=%d", v)
	}
	if v, err := dec.GetUint64(transport.TagSeq); err == nil {
		summary += fmt.Sprintf(" seq=%d", v)
	}
	if summary == "" {
		summary = "(empty)"
	}
	return summary
}
