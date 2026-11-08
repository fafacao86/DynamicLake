use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetastoreError {
    #[error("metastore backend error: {0}")]
    Backend(String),
    #[error("metastore transaction conflict: {0}")]
    Conflict(String),
    #[error("invalid metastore key: {0}")]
    InvalidKey(String),
    #[error("metastore encoding error: {0}")]
    Encoding(String),
    #[error("metastore transaction is already finished")]
    TransactionFinished,
}

pub type MetastoreResult<T> = Result<T, MetastoreError>;
