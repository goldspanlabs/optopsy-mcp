use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationErrorKind {
    InvalidInput,
    NotFound,
    Storage,
    Internal,
}

#[derive(Debug, Clone)]
pub struct ApplicationError {
    kind: ApplicationErrorKind,
    message: String,
}

impl ApplicationError {
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self {
            kind: ApplicationErrorKind::InvalidInput,
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            kind: ApplicationErrorKind::NotFound,
            message: message.into(),
        }
    }

    pub fn storage(message: impl Into<String>) -> Self {
        Self {
            kind: ApplicationErrorKind::Storage,
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            kind: ApplicationErrorKind::Internal,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn kind(&self) -> ApplicationErrorKind {
        self.kind
    }
}

impl Display for ApplicationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for ApplicationError {}

pub type ApplicationResult<T> = Result<T, ApplicationError>;
