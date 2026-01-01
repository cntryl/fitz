//! **DEPRECATED: This layer is being consolidated into `api/`.**
//!
//! Socket transport utilities and protocol codecs.
//!
//! **Architecture Note:** The socket accept loops, protocol framing, and session lifecycle
//! have been moved to `src/api/` (the canonical edge layer). This module remains only
//! for backward compatibility and contains shared transport utilities.
//!
//! **Routing types have been moved** to `src/runtime::routing` where they logically belong
//! as part of the core addressing model.

pub mod backpressure;
pub mod codecs;
pub mod config;
pub mod tcp;
pub mod ws;

