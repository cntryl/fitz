//! Actor error types.

use std::fmt;

pub type ActorResult<T> = Result<T, ActorError>;

#[derive(Debug, Clone)]
pub enum ActorError {
    /// Mailbox is full.
    MailboxFull,
    /// Actor is stopped/terminated.
    ActorStopped,
    /// Actor not found.
    ActorNotFound,
    /// Invalid message.
    InvalidMessage(String),
    /// Send error.
    SendError(String),
    /// System error.
    SystemError(String),
}

impl fmt::Display for ActorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorError::MailboxFull => write!(f, "mailbox full"),
            ActorError::ActorStopped => write!(f, "actor stopped"),
            ActorError::ActorNotFound => write!(f, "actor not found"),
            ActorError::InvalidMessage(msg) => write!(f, "invalid message: {}", msg),
            ActorError::SendError(msg) => write!(f, "send error: {}", msg),
            ActorError::SystemError(msg) => write!(f, "system error: {}", msg),
        }
    }
}

impl std::error::Error for ActorError {}

