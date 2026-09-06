//! Compatible send helpers and lossless routing-error APIs.

use super::{route_error_to_send_error, Actor, ActorRef, Context, SendError};
use crate::runtime::envelope::Envelope;
use crate::runtime::router::RouteError;
use crate::runtime::routing::RouteAddress;

impl<A: Actor + ?Sized> Context<A> {
    /// Send a message to another actor
    ///
    /// For full failure classification, use [`Self::send_detailed`]. Both methods:
    /// - Sets the source to this actor's route address
    /// - Automatically tracks causation from the current message
    /// - Inherits deadline from the current message if present
    ///
    /// # Semantics
    ///
    /// **CRITICAL**: This is a **synchronous best-effort** send with **no retries**.
    /// If the destination mailbox is full, the send fails immediately with `MailboxFull`.
    /// Callers must implement exponential backoff or use message buffering.
    ///
    /// **WARNING**: Sending to self during `receive()` can deadlock if the mailbox is full.
    /// Consider using deferred sends or checking mailbox capacity first.
    ///
    /// # Errors
    ///
    /// Returns `SendError` when the route is unknown, the destination actor has
    /// stopped, or backpressure prevents mailbox delivery. Legacy classification
    /// folds timeouts into stopped actors and payload rejections into sink panics.
    pub fn send<M>(&self, dest: RouteAddress, msg: M) -> Result<(), SendError>
    where
        M: Send + Sync + 'static,
    {
        self.send_detailed(dest, msg)
            .map_err(route_error_to_send_error)
    }

    /// Same delivery and metadata behavior as [`Self::send`], preserving the
    /// original routing failure without legacy error classification.
    ///
    /// # Errors
    ///
    /// Returns [`RouteError`] with the destination and exact delivery cause.
    pub fn send_detailed<M>(&self, dest: RouteAddress, msg: M) -> Result<(), RouteError>
    where
        M: Send + Sync + 'static,
    {
        if self.current_metadata.is_none() {
            return self
                .router
                .route(Envelope::from_route(self.address.clone(), dest, msg));
        }

        let mut envelope = Envelope::from_route(self.address.clone(), dest, msg);

        // Set causation from current envelope metadata
        if let Some(metadata) = &self.current_metadata {
            envelope = envelope.with_causation(metadata.id);

            // Inherit deadline if present
            if let Some(deadline) = metadata.deadline {
                envelope = envelope.with_deadline(deadline);
            }
        }

        self.router.route(envelope)
    }

    /// Send a message without attaching source, causation, or deadline metadata.
    ///
    /// This is intended for internal fire-and-forget fanout where the receiver
    /// does not rely on reply routing or trace ancestry.
    ///
    /// # Errors
    ///
    /// Returns `SendError` when the route is unknown, the destination actor has
    /// stopped, or backpressure prevents mailbox delivery. Legacy classification
    /// folds timeouts into stopped actors and payload rejections into sink panics.
    pub fn send_untracked<M>(&self, dest: RouteAddress, msg: M) -> Result<(), SendError>
    where
        M: Send + Sync + 'static,
    {
        self.send_untracked_detailed(dest, msg)
            .map_err(route_error_to_send_error)
    }

    /// Same delivery and metadata behavior as [`Self::send_untracked`], preserving the
    /// original routing failure without legacy error classification.
    ///
    /// # Errors
    ///
    /// Returns [`RouteError`] with the destination and exact delivery cause.
    pub fn send_untracked_detailed<M>(&self, dest: RouteAddress, msg: M) -> Result<(), RouteError>
    where
        M: Send + Sync + 'static,
    {
        self.router.route(Envelope::new(dest, msg))
    }

    /// Publish a domain event to the router.
    ///
    /// This is a convenience method for emitting `DomainPublishEvent`s.
    /// The event is routed based on its route field to the appropriate domain sink,
    /// which performs subscription matching and fanout internally.
    ///
    /// # Semantics
    ///
    /// Same as `send()`: synchronous best-effort with no retries.
    /// The route in the event determines which domain sink receives it.
    ///
    /// # Errors
    ///
    /// Returns `SendError` when routing the event fails for the same reasons as
    /// [`Self::send`].
    pub fn publish_event(
        &self,
        event: crate::runtime::domain_event::DomainPublishEvent,
    ) -> Result<(), SendError> {
        self.publish_event_detailed(event)
            .map_err(route_error_to_send_error)
    }

    /// Same delivery and metadata behavior as [`Self::publish_event`], preserving the
    /// original routing failure without legacy error classification.
    ///
    /// # Errors
    ///
    /// Returns [`RouteError`] with the destination and exact delivery cause.
    pub fn publish_event_detailed(
        &self,
        event: crate::runtime::domain_event::DomainPublishEvent,
    ) -> Result<(), RouteError> {
        let addr = RouteAddress::new(event.family_id, event.route.clone());
        self.send_detailed(addr, event)
    }

    /// Reply to the sender of the current message
    ///
    /// This creates a reply envelope that:
    /// - Is addressed to the original sender
    /// - Has causation set to the current message ID
    /// - Inherits the deadline from the current message
    ///
    /// # Returns
    ///
    /// Returns `Err(SendError::RouteNotFound)` if:
    /// - There is no current envelope (called outside message processing)
    /// - The current envelope has no source (external message)
    ///
    /// # Errors
    ///
    /// Returns `SendError` when no reply target is available or when routing the
    /// reply fails.
    pub fn reply<M>(&self, msg: M) -> Result<(), SendError>
    where
        M: Send + Sync + 'static,
    {
        self.reply_detailed(msg).map_err(route_error_to_send_error)
    }

    /// Same delivery and metadata behavior as [`Self::reply`], preserving the
    /// original routing failure without legacy error classification.
    ///
    /// # Errors
    ///
    /// Returns [`RouteError`] with the destination and exact delivery cause.
    pub fn reply_detailed<M>(&self, msg: M) -> Result<(), RouteError>
    where
        M: Send + Sync + 'static,
    {
        let metadata = self
            .current_metadata
            .as_ref()
            .ok_or(RouteError::RouteNotFound(self.address.clone()))?;

        let source = metadata
            .source
            .as_ref()
            .ok_or(RouteError::RouteNotFound(self.address.clone()))?;

        let mut reply_envelope = Envelope::from_route(self.address.clone(), source.clone(), msg)
            .with_causation(metadata.id);

        if let Some(deadline) = metadata.deadline {
            reply_envelope = reply_envelope.with_deadline(deadline);
        }

        self.router.route(reply_envelope)
    }
}

impl<M: Send + 'static> ActorRef<M> {
    /// Send a message to this actor (non-blocking, may fail if mailbox is full)
    ///
    /// The message is wrapped in an Envelope and routed to the destination actor.
    /// The source is not set (external message).
    ///
    /// # Semantics
    ///
    /// This is a **synchronous best-effort** send with **no retries**.
    /// For full failure classification, use [`Self::send_detailed`].
    ///
    /// # Errors
    ///
    /// Returns `SendError` when the route is unknown, the actor has stopped,
    /// or backpressure prevents mailbox delivery.
    pub fn send(&self, msg: M) -> Result<(), SendError>
    where
        M: Send + Sync + 'static,
    {
        self.send_detailed(msg).map_err(route_error_to_send_error)
    }

    /// Same delivery and metadata behavior as [`Self::send`], preserving the
    /// original routing failure without legacy error classification.
    ///
    /// # Errors
    ///
    /// Returns [`RouteError`] with the destination and exact delivery cause.
    pub fn send_detailed(&self, msg: M) -> Result<(), RouteError>
    where
        M: Send + Sync + 'static,
    {
        let envelope = Envelope::new(self.address.clone(), msg);
        self.router.route(envelope)
    }
}
