//! Token management (stub)

/// A simple token type (stub)
#[derive(Debug, Clone)]
pub struct Token {
    pub id: String,
}

impl Token {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}
