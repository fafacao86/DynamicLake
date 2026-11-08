use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use dynamic_lake_core::{DynamicTable, PipelineResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableWriteResult {
    pub rows_written: u64,
    pub output_version: Option<i64>,
}

#[async_trait]
pub trait TableWriter: Send + Sync {
    async fn write(
        &self,
        table: &DynamicTable,
        batches: &[RecordBatch],
    ) -> PipelineResult<TableWriteResult>;
}

/// Useful for dry runs and planner tests where DataFusion computes batches but
/// no lake table is committed.
#[derive(Debug, Clone, Copy, Default)]
pub struct CollectOnlyWriter;

#[async_trait]
impl TableWriter for CollectOnlyWriter {
    async fn write(
        &self,
        _table: &DynamicTable,
        batches: &[RecordBatch],
    ) -> PipelineResult<TableWriteResult> {
        Ok(TableWriteResult {
            rows_written: batches.iter().map(RecordBatch::num_rows).sum::<usize>() as u64,
            output_version: None,
        })
    }
}
