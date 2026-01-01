//! Transport utilities and protocol codecs.
//!
//! This module provides shared transport utilities:
//! - `backpressure`: Flow control mechanisms
//! - `codecs`: Wire format encoding/decoding
//! - `config`: Transport configuration
//!
//! **Architecture Note:** Socket accept loops and session lifecycle
//! live in `src/api/` (the canonical edge layer). This module contains
//! only shared utilities.

pub mod backpressure;
pub mod codecs;
pub mod config;

