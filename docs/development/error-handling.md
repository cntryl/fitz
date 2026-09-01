# Error Handling

## Transport-Level Errors

The connection is closed on:

- frame size exceeded;
- invalid TLV encoding (unrecoverable);
- CONNECT missing or invalid; or
- another protocol violation.

```rust
if payload.len() > max_frame_size {
    // Close connection
    return Err(Error::FrameTooLarge);
}
```

## Domain-Level Errors

Errors are returned in response payloads using per-domain encoding:

```rust
pub enum DomainResponse {
    Ok { /* result */ },
    Error(String), // Encoded per domain
}
```

KV example (error as string):

```text
Response (error):
  [u32 BE error_len]
  [bytes error_msg]
```

Notice example (error with status byte):

```text
Response (error):
  [u8]     1 (error status)
  [u32 BE] error_len
  [bytes]  error_msg
```

## Idempotency

- Idempotent operations (GET, READ, SCAN) are safe to retry.
- Non-idempotent operations (PUT, PUBLISH, APPEND) must not be retried unless
  the client can deduplicate them.

Some operations use correlation IDs (RPC):

- the client generates a 16-byte UUID;
- the broker uses it to match live in-flight requests to responses; and
- it does not create durable replay, recovery, or broker-side deduplication.
