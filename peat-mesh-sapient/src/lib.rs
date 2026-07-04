//! # peat-mesh-sapient
//!
//! `peat_mesh::transport::Translator`/`Transport` adapter for `peat-sapient`.
//!
//! One-way adapter crate per ADR-059 Amendment 4 (peat repo): SAPIENT is an
//! application-domain-specific transport (like TAK, unlike BLE), so its
//! `Translator` impl lives here rather than behind a `mesh-translator`
//! back-edge feature inside `peat-sapient` itself. `peat-sapient` retains
//! zero `peat-mesh` dependency; this crate depends on both, one-way.
//!
//! v1 scope: `DetectionReport` → `tracks`, `Registration`/`StatusReport` →
//! `platforms`. `Task`/`TaskAck` stay on `peat-sapient`'s existing
//! `SapientBridge`/`TaskQueue` path — see [`translator`] module docs for why.

pub mod subscriber;
pub mod translator;
pub mod transport;

pub use subscriber::run_bridge_subscriber;
pub use translator::SapientTranslator;
pub use transport::{PeatSapientTransport, SapientOutboundSink, SapientRole};
