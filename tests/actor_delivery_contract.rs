use fitz::runtime::actor::{ActorRef, SendError};
use fitz::runtime::router::{DeliveryError, MailboxSink, RouteError, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::Envelope;
use std::sync::Arc;

struct RejectingSink(DeliveryError);

impl MailboxSink for RejectingSink {
    fn deliver(&self, _: Envelope) -> Result<(), DeliveryError> {
        Err(self.0.clone())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[test]
fn should_preserve_delivery_error_with_explicit_single_lane_priority_delivery() {
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
    let error = actor.send_detailed(1).unwrap_err();

    // Assert
    assert!(error.to_string().contains("timed out"), "{error}");
    assert!(
        matches!(error, RouteError::DeliveryFailed(ref target, DeliveryError::Timeout)
        if target == actor.address())
    );
}

#[test]
fn should_preserve_invalid_payload_when_sending_through_actor_ref() {
    // Arrange
    let actor = sender(DeliveryError::InvalidPayload {
        len: 65536,
        max: 65535,
    });

    // Act
    let error = actor.send_detailed(1).unwrap_err();

    // Assert
    assert!(error.to_string().contains("65536"), "{error}");
    assert!(error.to_string().contains("65535"), "{error}");
    assert!(matches!(
        error,
        RouteError::DeliveryFailed(ref target, DeliveryError::InvalidPayload {
            len: 65536,
            max: 65535,
        }) if target == actor.address()
    ));
}

#[test]
fn should_preserve_unsupported_payload_when_sending_through_actor_ref() {
    // Arrange
    let actor = sender(DeliveryError::UnsupportedPayload);

    // Act
    let error = actor.send_detailed(1).unwrap_err();

    // Assert
    assert!(error.to_string().contains("Unsupported"), "{error}");
    assert!(
        matches!(error, RouteError::DeliveryFailed(ref target, DeliveryError::UnsupportedPayload)
        if target == actor.address())
    );
}

#[test]
fn should_preserve_exhaustive_legacy_send_error_matches() {
    // Arrange
    let actor = sender(DeliveryError::Timeout);

    // Act
    let classification = match actor.send(1).unwrap_err() {
        SendError::MailboxFull { .. } => "full",
        SendError::ActorStopped { .. } => "stopped",
        SendError::SinkPanicked { .. } => "panic",
        SendError::RouteNotFound { .. } => "missing",
    };

    // Assert
    assert_eq!(classification, "stopped");
}

struct TestActor;

impl fitz::runtime::Actor for TestActor {
    type Message = u32;

    fn receive(&mut self, _: u32, _: &mut fitz::runtime::Context<Self>) {}
}

#[test]
fn should_preserve_invalid_payload_when_sending_through_context() {
    // Arrange
    let router = Arc::new(Router::new());
    let target = RouteAddress::new(RouteFamily::new(1), Route::new("notice://realm/events"));
    router.register(
        target.clone(),
        Arc::new(RejectingSink(DeliveryError::InvalidPayload {
            len: 65536,
            max: 65535,
        })),
    );
    let source = RouteAddress::new(RouteFamily::new(1), Route::new("inbox://session/7"));
    let context = fitz::runtime::Context::<TestActor>::new(source, router);

    // Act
    let result = context.send_detailed(target.clone(), 1_u32);

    // Assert
    assert!(matches!(result, Err(RouteError::DeliveryFailed(destination,
        DeliveryError::InvalidPayload { len: 65536, max: 65535 })) if destination == target));
}
