package fixture

import "testing"

// TransportType identifies the transport protocol used for testing.
type TransportType string

const (
	TransportTCP       TransportType = "tcp"
	TransportWebSocket TransportType = "ws"
)

// RunWithBothTransports runs a test function with both TCP and WebSocket transports,
// verifying protocol equivalence per CLIENT_SPEC.md.
func RunWithBothTransports(t *testing.T, testFn func(t *testing.T, transport TransportType)) {
	t.Helper()

	transports := []TransportType{TransportTCP, TransportWebSocket}
	for _, transport := range transports {
		transport := transport // capture range variable
		t.Run(string(transport), func(t *testing.T) {
			// Run transports sequentially to avoid cross-transport race conditions in
			// Tests remain independent and deterministic.
			testFn(t, transport)
		})
	}
}
