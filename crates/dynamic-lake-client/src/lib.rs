//! Client API for materialized-view definitions and refresh requests.

mod backend;
pub use backend::{InProcessPipelineClient, PipelineClientBackend};

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use dynamic_lake_core::{PipelineBuildOptions, PipelineResult, TableIdentifier};
use dynamic_lake_format::{CreateDatabaseRequest, CreateTableRequest};

/// A SQL definition sent to the pipeline server for analysis.
#[derive(Debug, Clone)]
pub struct ViewDefinition {
    pub name: String,
    pub sql: String,
}

#[derive(Clone)]
pub struct PipelineClient {
    backend: Arc<dyn PipelineClientBackend>,
    build_options: PipelineBuildOptions,
}

impl PipelineClient {
    pub fn new(backend: Arc<dyn PipelineClientBackend>) -> Self {
        Self {
            backend,
            build_options: PipelineBuildOptions::default(),
        }
    }

    pub fn with_build_options(mut self, options: PipelineBuildOptions) -> Self {
        self.build_options = options;
        self
    }

    pub fn materialized_view(
        &self,
        name: impl Into<String>,
        sql: impl Into<String>,
    ) -> MaterializedViewBuilder {
        MaterializedViewBuilder {
            client: self.clone(),
            definitions: vec![ViewDefinition {
                name: name.into(),
                sql: sql.into(),
            }],
        }
    }

    pub async fn query_sql(&self, sql: &str) -> PipelineResult<Vec<RecordBatch>> {
        self.backend.query_sql(sql).await
    }

    pub async fn create_database(
        &self,
        catalog: &str,
        request: CreateDatabaseRequest,
    ) -> PipelineResult<()> {
        self.backend.create_database(catalog, request).await
    }

    pub async fn create_table(&self, request: CreateTableRequest) -> PipelineResult<()> {
        self.backend.create_table(request).await
    }

    pub async fn latest_version(&self, table: &TableIdentifier) -> PipelineResult<i64> {
        self.backend.latest_version(table).await
    }

    pub async fn append_batches(
        &self,
        table: &TableIdentifier,
        batches: Vec<RecordBatch>,
    ) -> PipelineResult<()> {
        self.backend.append_batches(table, batches).await
    }

    pub async fn register_table_snapshot(&self, table: &TableIdentifier) -> PipelineResult<()> {
        self.backend.register_table_snapshot(table).await
    }

    pub async fn trigger_incremental_refresh(&self, view: &TableIdentifier) -> PipelineResult<()> {
        self.backend.refresh_incremental(view).await
    }

    pub async fn trigger_full_refresh(&self, view: &TableIdentifier) -> PipelineResult<()> {
        self.backend.refresh_full(view).await
    }
}

pub struct MaterializedViewBuilder {
    client: PipelineClient,
    definitions: Vec<ViewDefinition>,
}

impl MaterializedViewBuilder {
    pub fn materialized_view(mut self, name: impl Into<String>, sql: impl Into<String>) -> Self {
        self.definitions.push(ViewDefinition {
            name: name.into(),
            sql: sql.into(),
        });
        self
    }

    pub async fn build(self) -> PipelineResult<()> {
        self.client
            .backend
            .define_views(self.definitions, self.client.build_options, false)
            .await
    }

    pub async fn create(self) -> PipelineResult<()> {
        self.client
            .backend
            .define_views(self.definitions, self.client.build_options, true)
            .await
    }
}
