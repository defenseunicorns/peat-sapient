use thiserror::Error;

#[derive(Debug, Error)]
pub enum SapientError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),

    #[error("Protobuf encode error: {0}")]
    Encode(#[from] prost::EncodeError),

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Node {0} not registered")]
    NodeNotFound(String),

    #[error("Message mapping error ({kind}): {detail}")]
    MappingError { kind: &'static str, detail: String },

    #[error("Unsupported coordinate system: {0}")]
    UnsupportedCoordinateSystem(String),

    #[error("Task rejected by node {node_id}: {reason}")]
    TaskRejected { node_id: String, reason: String },

    #[error("Frame too large: {size} bytes exceeds maximum {max}")]
    FrameTooLarge { size: usize, max: usize },
}
