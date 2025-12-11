//! Example: Basic Fitz v2 system startup.
//!
//! This demonstrates:
//! - Creating a Fitz system with the builder
//! - Spawning global actors (Router, Midge, Metrics)
//! - Starting the actor scheduler
//!
//! Run with: cargo run --example basic_system

use fitz::prelude::*;

fn main() -> Result<(), String> {
    println!("═══════════════════════════════════════════");
    println!("  Fitz v2 - Actor Model Messaging Runtime");
    println!("═══════════════════════════════════════════\n");

    // Build the Fitz system
    println!("Building Fitz system...");
    let system = FitzSystemBuilder::new()
        .with_name("fitz-example")
        .with_workers(4)
        .with_tcp("127.0.0.1:7070")
        .with_websocket("127.0.0.1:8080")
        .build()?;

    println!("System built successfully!\n");

    // Get references to global actors
    let global = system.global_actors();
    println!("Global actors:");
    println!("  - Midge:   {}", global.midge.name());
    println!("  - Router:  {}", global.router.name());
    println!("  - Metrics: {}", global.metrics.name());
    println!();

    println!("Starting Fitz system (press Ctrl+C to stop)...\n");

    // Start the system (this would block in a real implementation)
    // For now it returns immediately since scheduler.start() is a stub
    system.start()?;

    println!("\n✅ Fitz system started successfully!");

    Ok(())
}
