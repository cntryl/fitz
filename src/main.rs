// LAYER: BIN
//! Fitz single-node broker
//!
//! Minimal entry point that bootstraps the broker using the boot module.
//! All startup logic is modularized in src/boot/ for testability.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    fitz::boot::enforce_startup_resource_limits()?;

    let config = fitz::boot::BootConfig::new();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(fitz::boot::boot(config))
}
