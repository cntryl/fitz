package fixture

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	fitz "github.com/cntryl/fitz-go/fitz"
	"github.com/cntryl/fitz-go/internal/core/client"
	"github.com/cntryl/fitz-go/internal/core/types"
)

// Environment variables for broker configuration.
// Set these in CI/CD environments or when testing against remote brokers.
const (
	// EnvBrokerTCPAddr specifies the TCP broker address (default: localhost:4091)
	EnvBrokerTCPAddr = "FITZ_BROKER_TCP_ADDR"
	// EnvBrokerWSAddr specifies the WebSocket broker address (default: ws://localhost:4090/ws)
	EnvBrokerWSAddr = "FITZ_BROKER_WS_ADDR"
)

// Note: Integration tests require a running Fitz broker.
// Set FITZ_BROKER_TCP_ADDR and FITZ_BROKER_WS_ADDR environment variables to override defaults.

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
// Broker addresses can be configured via environment variables or use localhost defaults.
// Use FITZ_BROKER_TCP_ADDR (default: localhost:4091) and FITZ_BROKER_WS_ADDR (default: ws://localhost:4090/ws).
// Auth is disabled; an empty token is always sent.
func NewTestFixture(t *testing.T, transport TransportType) *TestFixture {
	t.Helper()

	// Get broker addresses from environment or use localhost defaults
	var brokerAddr string
	switch transport {
	case TransportTCP:
		brokerAddr = os.Getenv(EnvBrokerTCPAddr)
		if brokerAddr == "" {
			brokerAddr = "localhost:4091"
		}
	case TransportWebSocket:
		brokerAddr = os.Getenv(EnvBrokerWSAddr)
		if brokerAddr == "" {
			brokerAddr = "ws://localhost:4090/ws"
		}
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

// SetBrokerAddr overrides the fixture broker address.
func (f *TestFixture) SetBrokerAddr(addr string) {
	f.brokerAddr = addr
}

// StartBrokerIfNeeded returns the broker address for the requested transport.
// Addresses can be configured via environment variables (FITZ_BROKER_TCP_ADDR, FITZ_BROKER_WS_ADDR)
// or default to localhost (TCP: localhost:4091, WS: ws://localhost:4090/ws).
// Only TCP and WebSocket are supported; unknown transports return an error.
func StartBrokerIfNeeded(transport TransportType) (addr string, stop func(), err error) {
	switch transport {
	case TransportTCP:
		addr = os.Getenv(EnvBrokerTCPAddr)
		if addr == "" {
			addr = "localhost:4091"
		}
		return addr, func() {}, nil
	case TransportWebSocket:
		addr = os.Getenv(EnvBrokerWSAddr)
		if addr == "" {
			addr = "ws://localhost:4090/ws"
		}
		return addr, func() {}, nil
	default:
		return "", nil, fmt.Errorf("unsupported transport: %s", transport)
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
	realm := f.UniqueRealm()
	area := f.UniqueArea()
	resource := f.UniqueResource()
	if scheme == "schedule" {
		return fmt.Sprintf("%s://%s/%s/%s/%s", scheme, realm, area, resource, "run")
	}
	return fmt.Sprintf("%s://%s/%s/%s", scheme, realm, area, resource)
}
