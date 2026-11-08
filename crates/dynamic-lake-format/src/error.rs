use thiserror::Error;

#[derive(Debug, Error)]
pub enum LakeFormatError {
    #[error("unsupported format capability: {0}")]
    Unsupported(String),
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("catalog error: {0}")]
    Catalog(String),
    #[error("planning error: {0}")]
    Planning(#[from] datafusion::error::DataFusionError),
    #[error("commit error: {0}")]
    Commit(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

pub type LakeFormatResult<T> = Result<T, LakeFormatError>;
