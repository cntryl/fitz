# fitz-ts

TypeScript client library for [Fitz](https://github.com/cntryl/fitz), a distributed system with domains for KV, Queue, RPC, Lease, Notice, Stream, and Schedule operations.

## Features

- **Isomorphic**: Works in Node.js and browsers with automatic transport detection
- **7 Domains**: KV (transactions), Queue, RPC, Lease (distributed locking), Notice (pub/sub), Stream (append-only logs), Schedule (delayed tasks)
- **Async/Await**: Native TypeScript Promise-based API
- **Type-Safe**: Full TypeScript support with proper error types
- **Connection Pooling**: Single connection shared across all domain clients

## Installation

```bash
npm install @cntryl/fitz
```

## Quick Start

```typescript
import { Client } from "@cntryl/fitz";

// Create client
const client = new Client({
  url: "ws://localhost:4090",
  jwt: "your-jwt-token",
});

// Connect
await client.connect();

// Use KV
const kv = client.kv();
const tx = await kv.begin("kv://realm/area/users", "ReadWrite");

await tx.put(
  new TextEncoder().encode("user-1"),
  new TextEncoder().encode('{"name": "Alice"}'),
);

const value = await tx.get(new TextEncoder().encode("user-1"));
await tx.commit();

// Disconnect
await client.disconnect();
```

## Supported Transports

- **WebSocket**: `ws://` and `wss://` (browser and Node.js)
- **TCP**: `tcp://` (Node.js only, with length-prefixed framing)
- **Auto-detection**: URL automatically converted to appropriate transport

```typescript
// WebSocket
const client = new Client({
  url: "ws://localhost:4090",
  jwt: token,
});

// TCP
const client = new Client({
  url: "tcp://localhost:4090",
  jwt: token,
});

// Auto-detect
const client = new Client({
  url: "localhost:4090", // Defaults to WebSocket
  jwt: token,
});
```

## API

### Client Configuration

```typescript
interface ClientConfig {
  url: string;
  jwt: string;
  timeout?: number; // Default: 30000ms
  transport?: "ws" | "tcp" | "auto"; // Default: 'auto'
  retryAttempts?: number;
  retryDelayMs?: number;
}
```

### Connection Management

```typescript
const client = new Client(config);

await client.connect();
const isConnected = client.isConnected();
await client.disconnect();
```

### KV Domain

```typescript
const kv = client.kv();

// Begin transaction
const tx = await kv.begin("kv://realm/area/resources", "ReadWrite");

// Operations
await tx.put(key, value);
const value = await tx.get(key);
await tx.delete(key);
const { keys, nextCursor } = await tx.scan(cursor);

// Finalize
await tx.commit();
await tx.rollback();
```

### Other Domains

Queue, RPC, Lease, Notice, Stream, and Schedule clients are available:

```typescript
const queue = client.queue();
const rpc = client.rpc();
const lease = client.lease();
const notice = client.notice();
const stream = client.stream();
const schedule = client.schedule();
```

(Full implementations coming in follow-up phases)

## Error Handling

All errors subclass `FitzError`:

```typescript
import {
  FitzError,
  TimeoutError,
  ConnectionError,
  KvError,
} from "@cntryl/fitz";

try {
  await client.connect();
} catch (err) {
  if (err instanceof TimeoutError) {
    console.log("Connection timeout");
  } else if (err instanceof ConnectionError) {
    console.log("Connection failed");
  } else if (err instanceof FitzError) {
    console.log("Fitz error:", err.code);
  }
}
```

## Building

```bash
npm run build
```

Outputs:

- `dist/index.js` (CommonJS)
- `dist/index.mjs` (ESM)
- `dist/index.d.ts` (TypeScript declarations)

## Testing

```bash
# Run all tests
npm test

# Watch mode
npm test -- --watch

# With coverage
npm test -- --coverage

# Run benchmarks
npm run bench
```

## Development

```bash
# Format code
npm run fmt

# Lint
npm run lint

# Type check
npx tsc --noEmit
```

## Architecture

### Layers

1. **API** (`src/client/`) - Main Client and Connection management
2. **Transport** (`src/transport/`) - WebSocket/TCP abstraction
3. **Frame** (`src/frame/`) - Wire protocol encoding/decoding
4. **Domains** (`src/domains/`) - Domain-specific clients and codecs
5. **Core** (`src/core/`) - Common utilities (buffer, types, errors)

### Frame Protocol

```
[MessageType (1-3 bytes)] [Length (u16 BE)] [Payload]
```

- MessageType 0-254: single byte
- MessageType 255+: escape byte `0xFF` + u16 BE
- Payload: domain-specific concatenated fields (no nesting)

## License

Apache-2.0
