// LAYER: BIN
//! Fitz single-node broker
//!
//! Minimal entry point that bootstraps the broker using the boot module.
//! All startup logic is modularized in src/boot/ for testability.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = fitz::boot::BootConfig::new();
    fitz::boot::boot(config).await
}

