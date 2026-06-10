//! Layer 2: SAPIENT ↔ peat-schema bidirectional transformers.
//!
//! Each sub-module owns one message-type mapping. All modules are stubs
//! until Phase 3 implementation — the module structure and public API
//! are declared here so the feature-flag plumbing compiles from day one.

pub mod alert;
pub mod detection;
pub mod registration;
pub mod status;
pub mod task;
