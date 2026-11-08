use thiserror::Error;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("invalid pipeline: {0}")]
    Invalid(String),
    #[error("pipeline catalog error: {0}")]
    Catalog(String),
    #[error("node execution failed: {0}")]
    Execution(String),
}

pub type PipelineResult<T> = Result<T, PipelineError>;
