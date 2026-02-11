package fixture

import "fmt"

// StartSimBroker starts an in-process simulated broker for testing.
// Returns the broker address, a stop function, and any error.
// TODO: implement simulated broker for integration tests without external broker.
func StartSimBroker(transport string) (string, func(), error) {
	return "", nil, fmt.Errorf("simulated broker not implemented; set FITZ_BROKER_ADDR to use a real broker")
}
