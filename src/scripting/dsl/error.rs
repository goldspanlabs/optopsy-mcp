//! DSL error types with line-number tracking.

use std::fmt;

/// An error encountered during DSL parsing or transpilation.
#[derive(Debug, Clone)]
pub struct DslError {
    pub line: usize,
    pub message: String,
}

impl fmt::Display for DslError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.line == 0 {
            write!(f, "DSL error: {}", self.message)
        } else {
            write!(f, "DSL error on line {}: {}", self.line, self.message)
        }
    }
}

impl std::error::Error for DslError {}

impl DslError {
    pub fn new(line: usize, message: impl Into<String>) -> Self {
        Self {
            line,
            message: message.into(),
        }
    }

    pub fn general(message: impl Into<String>) -> Self {
        Self {
            line: 0,
            message: message.into(),
        }
    }
}
