//! Example: Message envelopes for routing and tracing

use fitz::runtime::ActorId;
use fitz::transport::envelope::{Envelope, MessageId};
use std::time::{Duration, Instant};

fn main() {
    println!("=== Fitz Envelope Example ===\n");

    // Basic envelope
    let destination = ActorId::new(1);
    let envelope = Envelope::new(destination, "Hello, Actor!");

    println!("Created envelope:");
    println!("  ID: {}", envelope.id());
    println!("  Destination: {}", envelope.destination());
    println!("  Source: {:?}", envelope.source());
    println!("  Payload: {:?}", envelope.payload::<&str>());

    // Envelope with source
    println!("\n--- Envelope with Source ---");
    let source = ActorId::new(100);
    let dest = ActorId::new(200);
    let request = Envelope::from_actor(source, dest, "Request data");

    println!("Request envelope:");
    println!("  ID: {}", request.id());
    println!("  From: {}", request.source().unwrap());
    println!("  To: {}", request.destination());

    // Reply envelope (causation tracking)
    println!("\n--- Reply with Causation ---");
    let reply = request.reply_to("Here's your data");

    println!("Reply envelope:");
    println!("  ID: {}", reply.id());
    println!("  From: {}", reply.source().unwrap());
    println!("  To: {}", reply.destination());
    println!("  Caused by: {}", reply.causation().unwrap());
    println!("  (Reply goes back to original sender)");

    // Deadline handling
    println!("\n--- Deadline Tracking ---");
    let deadline = Instant::now() + Duration::from_secs(5);
    let urgent = Envelope::new(destination, "Urgent message")
        .with_deadline(deadline);

    println!("Urgent envelope:");
    println!("  Deadline: {:?}", urgent.deadline());
    println!("  Is expired: {}", urgent.is_expired());

    // Causation chain
    println!("\n--- Causation Chain ---");
    let parent_id = MessageId::new();
    let child = Envelope::new(destination, "Child message")
        .with_causation(parent_id);

    println!("Child envelope:");
    println!("  ID: {}", child.id());
    println!("  Parent (causation): {}", child.causation().unwrap());
    println!("  (Enables distributed tracing)");

    // Type erasure demonstration
    println!("\n--- Type Erasure ---");
    
    #[derive(Debug)]
    struct CustomMessage {
        action: String,
        value: i32,
    }

    let custom = CustomMessage {
        action: "process".to_string(),
        value: 42,
    };

    let custom_envelope = Envelope::new(destination, custom);
    println!("Envelope with custom type:");
    println!("  Can downcast: {}", custom_envelope.payload::<CustomMessage>().is_some());
    println!("  Wrong type: {}", custom_envelope.payload::<String>().is_some());

    println!("\n=== Example Complete ===");
}
