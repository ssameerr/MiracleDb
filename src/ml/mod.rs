//! Machine Learning Module
//!
//! Provides ML inference capabilities using Candle (Rust-native)
//!
//! Features:
//! - Text embedding generation for vector search
//! - Model inference
//! - Batch processing

#[cfg(feature = "nlp")]
pub mod candle_inference;

#[cfg(feature = "nlp")]
pub use candle_inference::block_on_candle;
#[cfg(feature = "nlp")]
pub use candle_inference::CandleEngine;

pub mod feature_store;
pub mod automl;
