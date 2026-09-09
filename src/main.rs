// LAYER: BIN
//! Fitz single-node broker
//!
//! Minimal entry point that bootstraps the broker using the boot module.
//! All startup logic is modularized in src/boot/ for testability.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    fitz::boot::enforce_startup_resource_limits()?;

    let config = fitz::boot::BootConfig::new();
    fitz::api::run(config)
}
