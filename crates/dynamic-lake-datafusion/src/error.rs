use datafusion::error::DataFusionError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DataFusionPipelineError {
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("table writer error: {0}")]
    Writer(String),
    #[error("pipeline error: {0}")]
    Pipeline(String),
}

impl From<DataFusionPipelineError> for dynamic_lake_core::PipelineError {
    fn from(error: DataFusionPipelineError) -> Self {
        Self::Execution(error.to_string())
    }
}
