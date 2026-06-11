//! # peat-sapient
//!
//! SAPIENT (BSI Flex 335 v2.0) protocol library and Peat mesh bridge.
//!
//! ## Two layers
//!
//! **Layer 1 — general SAPIENT library** (always compiled):
//! - [`proto`] — prost-generated types for all BSI Flex 335 v2.0 messages
//! - [`codec`] — `tokio_util` codec for the 4-byte LE length-prefix TCP framing
//! - [`connection`] — TCP connection management (HLDMM listener / DLMM client with retry)
//!
//! **Layer 2 — Peat transformer** (feature `peat`, default on):
//! - [`transform`] — bidirectional SAPIENT ↔ `peat-schema` type mappings
//!
//! To use as a standalone SAPIENT library without Peat:
//! ```toml
//! peat-sapient = { version = "0.1", default-features = false }
//! ```
//!
//! ## Quick start (DLMM relay)
//!
//! ```rust,no_run
//! use peat_sapient::connection::{BridgeRole, ReconnectConfig, connect_with_retry};
//! use peat_sapient::connection;
//!
//! # async fn run() -> Result<(), peat_sapient::error::SapientError> {
//! let addr = "127.0.0.1:5066".parse().unwrap();
//! let mut framed = connect_with_retry(addr, &ReconnectConfig::default()).await?;
//!
//! while let Some(msg) = connection::recv(&mut framed).await? {
//!     println!("received: {:?}", msg.node_id);
//! }
//! # Ok(())
//! # }
//! ```

pub mod codec;
pub mod connection;
pub mod error;
pub mod proto;

#[cfg(feature = "peat")]
pub mod bridge;
#[cfg(feature = "peat")]
pub mod registry;
#[cfg(feature = "peat")]
pub mod transform;

pub use error::SapientError;
pub use proto::{
    Alert, AlertAck, Content, DetectionReport, Registration, RegistrationAck, SapientMessage,
    SapientProtoError, StatusReport, Task, TaskAck,
};
