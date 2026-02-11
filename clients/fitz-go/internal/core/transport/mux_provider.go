package transport

import "context"

// MuxProvider is the minimal transport abstraction used by all domain clients.
// Every domain client accepts this interface rather than depending on the
// concrete *Mux type, enabling test mocks and per-domain channel adapters.
type MuxProvider interface {
	Send(Frame) error
	In() <-chan Frame
	Ctx() context.Context
	OnReconnect(func())
}
