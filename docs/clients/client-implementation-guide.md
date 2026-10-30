# Client Implementation Guide

Use the [client specification](client-spec.md) as the wire and behavior
contract. This guide summarizes implementation rules shared by every SDK.

## Connection lifecycle

- Send `CONNECT` first and wait for `HELLO` or `CONNECT_REJECTED` before
  considering the session established.
- Read message and in-flight limits from `HELLO`.
- On disconnect, create a new session, authenticate again, and restore
  subscriptions and worker registrations. Treat connection-owned handles and
  pending requests as lost.

## Requests and errors

- Encode the documented domain payload exactly and match responses by their
  header correlation value.
- Preserve the response effect: `NOT_APPLIED` is safe to retry; `APPLIED` means
  the operation succeeded; `INDETERMINATE` means its outcome must be checked or
  the operation must be safe to repeat.
- A timeout or lost connection does not by itself prove that an operation was
  not applied.

## Domain APIs and conformance

- Keep route construction and validation consistent with the domain grammar.
- Expose domain operations through the language's normal error and cancellation
  patterns without changing their wire semantics.
- Use the shared [conformance scenarios](cross-language-conformance-suite.yaml)
  and [runner contract](cross-language-conformance-runner.md) to check observable
  behavior.
