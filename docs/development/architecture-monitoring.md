# Architecture monitoring

Add tracing for performance insights:
```rust
use tracing::{instrument, span, Level};
#[instrument(skip(msg))]
pub fn handle(&mut self, msg: DomainMessage) -> DomainResponse {
    let span = span!(Level::DEBUG, "domain_handler");
    let _guard = span.enter();

    tracing::debug!("handling message");
    // ... logic
    tracing::debug!("response ready");
}
```
