# Quick Start

This guide provides the fastest path to a successful first Fitz request.

## 1. Start Fitz

Use the repository compose or local runtime command and confirm readiness via `/readyz`.

## 2. Connect a Client

Use any client that follows [clients/client-spec.md](../clients/client-spec.md).

## 3. Authenticate

Send CONNECT with valid JWT claims for realm and scopes.

## 4. Execute a Simple Operation

1. Choose a route and route family.
2. Send a single operation, such as KV get or put.
3. Confirm both the response and latency metrics.

## 5. Continue

- [api-guide.md](api-guide.md)
- [durability.md](durability.md)
- [troubleshooting.md](troubleshooting.md)
