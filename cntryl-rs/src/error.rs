/// Fitz client error types
#[derive(Debug, thiserror::Error)]
pub enum FitzError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Codec error: {0}")]
    Codec(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Domain error: {0}")]
    DomainError(String),

    #[error("Auth failed: {0}")]
    AuthFailed(String),

    #[error("Timeout")]
    Timeout,

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Frame too large: {0}")]
    FrameTooLarge(usize),

    #[error("JWT error: {0}")]
    JwtError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(String),
}

pub type Result<T> = std::result::Result<T, FitzError>;
