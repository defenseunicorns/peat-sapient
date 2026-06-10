//! Generated protobuf types for BSI Flex 335 v2.0 (SAPIENT).
//!
//! Vendored from <https://github.com/dstl/SAPIENT-Proto-Files> (Apache 2.0).
//! Upstream commit: see `proto/VERSION`.

#[allow(clippy::all, clippy::pedantic)]
pub mod sapient_msg {
    pub mod bsi_flex_335_v2_0 {
        include!(concat!(
            env!("OUT_DIR"),
            "/sapient_msg.bsi_flex_335_v2_0.rs"
        ));
    }
}

// Convenience re-exports for the types callers use most.
pub use sapient_msg::bsi_flex_335_v2_0::{
    sapient_message::Content, Alert, AlertAck, DetectionReport,
    Error as SapientProtoError, Registration, RegistrationAck,
    SapientMessage, StatusReport, Task, TaskAck,
};
