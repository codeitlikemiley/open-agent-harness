use thiserror::Error;

pub type Result<T> = std::result::Result<T, CoreError>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CoreError {
    #[error("{0}")]
    InvalidId(String),
    #[error("canonical json: {0}")]
    Json(String),
    #[error("conversation record invariant: {0}")]
    RecordInvariant(String),
    #[error("persisted format version {found} is unsupported (expected {expected})")]
    FormatVersion { found: u32, expected: u32 },
    #[error("agent name {0} is invalid")]
    InvalidAgentName(String),
}

impl CoreError {
    pub fn invariant(msg: impl Into<String>) -> Self {
        Self::RecordInvariant(msg.into())
    }
}
