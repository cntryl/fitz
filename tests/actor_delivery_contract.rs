use fitz::runtime::actor::{ActorRef, SendError};
use fitz::runtime::router::{DeliveryError, MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::Envelope;
use std::sync::Arc;

struct RejectingSink(DeliveryError);

impl MailboxSink for RejectingSink {
    fn deliver(&self, _: Envelope) -> Result<(), DeliveryError> {
        Err(self.0.clone())
    }
}

#[test]
fn should_preserve_delivery_error_with_default_priority_delivery() {
    // Arrange
    let sink = RejectingSink(DeliveryError::UnsupportedPayload);
    let address = RouteAddress::new(RouteFamily::new(1), Route::new("notice://realm/events"));

    // Act
    let result = sink.deliver_high_priority(Envelope::new(address, 1_u32));

    // Assert
    assert_eq!(result, Err(DeliveryError::UnsupportedPayload));
}

fn sender(error: DeliveryError) -> ActorRef<u32> {
    let router = Arc::new(Router::new());
    let address = RouteAddress::new(RouteFamily::new(1), Route::new("notice://realm/events"));
    router.register(address.clone(), Arc::new(RejectingSink(error)));
    ActorRef::new(address, router)
}

#[test]
fn should_preserve_timeout_when_sending_through_actor_ref() {
    // Arrange
    let actor = sender(DeliveryError::Timeout);

    // Act
    let error = actor.send(1).unwrap_err();

    // Assert
    assert!(error.to_string().contains("timed out"), "{error}");
    assert!(matches!(error, SendError::Timeout { .. }));
    assert_eq!(error.target(), actor.address());
}

#[test]
fn should_preserve_invalid_payload_when_sending_through_actor_ref() {
    // Arrange
    let actor = sender(DeliveryError::InvalidPayload {
        len: 65536,
        max: 65535,
    });

    // Act
    let error = actor.send(1).unwrap_err();

    // Assert
    assert!(error.to_string().contains("65536"), "{error}");
    assert!(error.to_string().contains("65535"), "{error}");
    assert!(matches!(
        error,
        SendError::InvalidPayload {
            len: 65536,
            max: 65535,
            ..
        }
    ));
    assert_eq!(error.target(), actor.address());
}

#[test]
fn should_preserve_unsupported_payload_when_sending_through_actor_ref() {
    // Arrange
    let actor = sender(DeliveryError::UnsupportedPayload);

    // Act
    let error = actor.send(1).unwrap_err();

    // Assert
    assert!(error.to_string().contains("Unsupported"), "{error}");
    assert!(matches!(error, SendError::UnsupportedPayload { .. }));
    assert_eq!(error.target(), actor.address());
}
