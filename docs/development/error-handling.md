# Error Handling

## Transport-Level Errors

The connection is closed on:

- frame size exceeded;
- a truncated header or a declared length that disagrees with the frame;
- CONNECT missing, or CONNECT_REJECTED flushed for an invalid CONNECT; or
- another protocol violation.

```rust
if payload.len() > max_frame_size {
    // Close connection
    return Err(Error::FrameTooLarge);
}
```

## Domain-Level Errors

Every domain uses the common response envelope. Error messages are diagnostic;
clients decide from the code, class, and effect:

```text
Response (error):
  [u8]     1 (error status)
  [u16 BE] code
  [u8]     class
  [u8]     effect
  [u32 BE] error_len
  [bytes]  error_msg
```

## Idempotency

BUSY means the request was declined with NOT_APPLIED; backpressure preserves
the connection. Reserved header flags receive protocol UNSUPPORTED on the
request's type and correlation and also preserve the connection. BACKEND,
TIMEOUT, and RPC WORKER_NOT_FOUND carry INDETERMINATE: worker disconnect
does not prove the operation had no effect.

- Idempotent operations (GET, READ, SCAN) are safe to retry.
- Non-idempotent operations (PUT, PUBLISH, APPEND) must not be retried unless
  the client can deduplicate them.

Fitz has two distinct live correlation mechanisms:

- Non-zero `u64` header correlations match requests to their response frames,
  including deferred Queue and Lease answers.
- RPC payload correlation IDs route one live call through caller, broker, and
  worker and remain part of the RPC domain contract.

For RPC payload correlation:

- the client generates a 16-byte UUID;
- the broker uses it to match live in-flight requests to responses; and
- it does not create durable replay, recovery, or broker-side deduplication.
