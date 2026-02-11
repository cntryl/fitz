package fixture

import (
	"context"
	"fmt"
	"testing"
	"time"

	fitz "github.com/cntryl/cntryl-go"
	"github.com/cntryl/cntryl-go/internal/core/client"
	"github.com/cntryl/cntryl-go/internal/core/types"
)

// TestFixture manages broker connections and test lifecycle for integration tests.
// It supports both TCP and WebSocket transports to verify protocol equivalence.
type TestFixture struct {
	t            *testing.T
	transport    TransportType
	brokerAddr   string
	client       *client.Client
	cleanupFuncs []func()
}

// NewTestFixture creates a test fixture with the specified transport.
// Broker addresses are hardcoded to localhost (TCP: localhost:4091, WS: ws://localhost:4090/ws).
// Auth is disabled; an empty token is always sent.
func NewTestFixture(t *testing.T, transport TransportType) *TestFixture {
	t.Helper()

	// Hardcoded broker addresses for localhost development
	var brokerAddr string
	switch transport {
	case TransportTCP:
		brokerAddr = "localhost:4091"
	case TransportWebSocket:
		brokerAddr = "ws://localhost:4090/ws"
	default:
		t.Fatalf("unsupported transport type: %s", transport)
	}

	f := &TestFixture{
		t:            t,
		transport:    transport,
		brokerAddr:   brokerAddr,
		cleanupFuncs: []func(){},
	}

	// Automatically register cleanup with test
	t.Cleanup(f.cleanup)

	return f
}

// Connect establishes a connection to the broker with no authentication.
func (f *TestFixture) Connect(ctx context.Context) error {
	f.t.Helper()

	var tokenProvider types.TokenProvider = func(ctx context.Context) (string, error) {
		return "", nil
	}

	f.client = client.NewClient(f.brokerAddr, tokenProvider)
	return f.client.Connect(ctx)
}

// SetBrokerAddr overrides the fixture broker address (useful for simulators).
func (f *TestFixture) SetBrokerAddr(addr string) {
	f.brokerAddr = addr
}

// StartBrokerIfNeeded returns the hardcoded localhost broker address for the
// requested transport (TCP: localhost:4091, WS: ws://localhost:4090/ws).
// For unknown transports it falls back to the in-process simulator.
func StartBrokerIfNeeded(transport TransportType) (addr string, stop func(), err error) {
	// Hardcoded localhost broker addresses
	switch transport {
	case TransportTCP:
		return "localhost:4091", func() {}, nil
	case TransportWebSocket:
		return "ws://localhost:4090/ws", func() {}, nil
	default:
		return StartSimBroker(string(transport))
	}
}

// Client returns the connected Fitz client.
func (f *TestFixture) Client() fitz.Client {
	if f.client == nil {
		f.t.Fatal("client not connected; call Connect() first")
	}
	return f.client
}

// cleanup executes all registered cleanup functions and closes the client connection.
// This is automatically registered with t.Cleanup() during fixture creation.
func (f *TestFixture) cleanup() {
	// Run all cleanup functions in reverse order
	for i := len(f.cleanupFuncs) - 1; i >= 0; i-- {
		f.cleanupFuncs[i]()
	}

	// Close client connection
	if f.client != nil {
		if err := f.client.Close(); err != nil {
			f.t.Logf("error closing client: %v", err)
		}
	}
}

// AddCleanup registers a function to be called during cleanup.
func (f *TestFixture) AddCleanup(fn func()) {
	f.cleanupFuncs = append(f.cleanupFuncs, fn)
}

// ConnectOrSkip connects to the broker using StartBrokerIfNeeded and skips
// the test when no broker is available.
func (f *TestFixture) ConnectOrSkip(ctx context.Context) {
	f.t.Helper()

	addr, stop, err := StartBrokerIfNeeded(f.transport)
	if err != nil {
		f.t.Skipf("broker not available: %v", err)
	}
	f.SetBrokerAddr(addr)
	f.AddCleanup(func() { stop() })

	if err := f.Connect(ctx); err != nil {
		f.t.Skipf("broker not available: %v", err)
	}
}

// UniqueRealm generates a unique realm name for test isolation.
func (f *TestFixture) UniqueRealm() string {
	return fmt.Sprintf("test-%d", time.Now().UnixNano())
}

// UniqueArea generates a unique area name for test isolation.
func (f *TestFixture) UniqueArea() string {
	return fmt.Sprintf("area-%d", time.Now().UnixNano())
}

// UniqueResource generates a unique resource name for test isolation.
func (f *TestFixture) UniqueResource() string {
	return fmt.Sprintf("resource-%d", time.Now().UnixNano())
}

// UniqueRoute generates a unique route string for the given domain scheme.
func (f *TestFixture) UniqueRoute(scheme string) string {
	return fmt.Sprintf("%s://%s/%s/%s", scheme, f.UniqueRealm(), f.UniqueArea(), f.UniqueResource())
}
