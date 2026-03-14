# Quick Start

This page gives the shortest path to first successful Fitz traffic.

## 1. Start Fitz

Use the repository compose or local runtime command and confirm readiness via `/readyz`.

## 2. Connect A Client

Use any client that follows [clients/CLIENT_SPEC.md](../clients/CLIENT_SPEC.md).

## 3. Authenticate

Send CONNECT with valid JWT claims for realm and scopes.

## 4. Execute A Simple Operation

1. Choose a route and route family.
2. Send one operation (for example, KV get or put).
3. Confirm response and latency metrics.

## 5. Continue

- [api-guide.md](api-guide.md)
- [durability.md](durability.md)
- [troubleshooting.md](troubleshooting.md)
