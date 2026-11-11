//! Operations exposed by a pipeline client backend.

mod in_process;
pub use in_process::InProcessPipelineClient;

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use dynamic_lake_core::{PipelineBuildOptions, PipelineResult, TableIdentifier};
use dynamic_lake_format::{CreateDatabaseRequest, CreateTableRequest};

use crate::ViewDefinition;

#[async_trait]
pub trait PipelineClientBackend: Send + Sync {
    async fn create_views(
        &self,
        definitions: Vec<ViewDefinition>,
        options: PipelineBuildOptions,
    ) -> PipelineResult<()>;

    async fn query_sql(&self, sql: &str) -> PipelineResult<Vec<RecordBatch>>;

    async fn create_database(
        &self,
        catalog: &str,
        request: CreateDatabaseRequest,
    ) -> PipelineResult<()>;

    async fn create_table(&self, request: CreateTableRequest) -> PipelineResult<()>;

    async fn latest_version(&self, table: &TableIdentifier) -> PipelineResult<i64>;

    async fn append_batches(
        &self,
        table: &TableIdentifier,
        batches: Vec<RecordBatch>,
    ) -> PipelineResult<()>;

    async fn register_table_snapshot(&self, table: &TableIdentifier) -> PipelineResult<()>;

    async fn refresh_incremental(&self) -> PipelineResult<()>;

    async fn refresh_full(&self) -> PipelineResult<()>;
}
