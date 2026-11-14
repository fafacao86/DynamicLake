//! In-process implementation of the pipeline client backend.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use dynamic_lake_core::{
    GraphOperations, IvmStreamObject, MetastoreFactory, MetastoreRepository, PipelineBuildOptions,
    PipelineError, PipelineGraph, PipelineResult, RefreshMode, TableIdentifier,
};
use dynamic_lake_datafusion::ivm::{
    compile_view_definitions, normalize::register_ivm_functions, physical::install_lake_scans,
};
use dynamic_lake_format::{CreateDatabaseRequest, CreateTableRequest, LakeFormatRegistry};

use crate::ViewDefinition;

use super::PipelineClientBackend;

struct RegisteredViewSet {
    graph: PipelineGraph,
    plan_signatures: BTreeMap<TableIdentifier, String>,
    refresh_gate: tokio::sync::Mutex<()>,
    streams:
        tokio::sync::Mutex<BTreeMap<TableIdentifier, BTreeMap<TableIdentifier, IvmStreamObject>>>,
}

impl RegisteredViewSet {
    async fn build(
        session: &SessionContext,
        formats: &LakeFormatRegistry,
        definitions: Vec<(ViewDefinition, PipelineBuildOptions)>,
    ) -> PipelineResult<Self> {
        let (graph, plan_signatures) =
            compile_view_definitions(session, formats, definitions).await?;
        let streams = graph
            .nodes()
            .iter()
            .map(|(id, node)| {
                let sources = node.upstream.iter().chain(&node.external_inputs);
                let streams = sources
                    .map(|source| {
                        (
                            source.clone(),
                            IvmStreamObject::create(id.clone(), source.clone(), None),
                        )
                    })
                    .collect();
                (id.clone(), streams)
            })
            .collect();
        Ok(Self {
            graph,
            plan_signatures,
            refresh_gate: tokio::sync::Mutex::new(()),
            streams: tokio::sync::Mutex::new(streams),
        })
    }
}

/// Runs pipeline operations in the caller's process and DataFusion session.
#[derive(Clone)]
pub struct InProcessPipelineClient {
    session: SessionContext,
    formats: LakeFormatRegistry,
    metadata: MetastoreRepository,
    view_set: Arc<RwLock<Option<Arc<RegisteredViewSet>>>>,
}

impl InProcessPipelineClient {
    pub async fn new(
        session: SessionContext,
        formats: LakeFormatRegistry,
        metadata_path: impl AsRef<Path>,
        metadata_factory: &dyn MetastoreFactory,
    ) -> PipelineResult<Self> {
        let metadata = MetastoreRepository::new(
            metadata_factory
                .open(metadata_path.as_ref())
                .await
                .map_err(metadata_error)?,
        );
        register_ivm_functions(&session);
        let session = install_lake_scans(session, formats.clone());
        let server = Self {
            session,
            formats,
            metadata,
            view_set: Arc::new(RwLock::new(None)),
        };
        server.load_views().await?;
        Ok(server)
    }

    fn refresh_engine(&self) -> PipelineResult<refresh::RefreshEngine> {
        let definitions = self
            .view_set
            .read()
            .expect("view set lock poisoned")
            .as_ref()
            .cloned()
            .ok_or_else(|| {
                PipelineError::Invalid("no materialized views are registered".to_string())
            })?;
        Ok(refresh::RefreshEngine::new(
            self.session.clone(),
            self.formats.clone(),
            self.metadata.clone(),
            definitions,
        ))
    }
}

#[async_trait]
impl PipelineClientBackend for InProcessPipelineClient {
    async fn create_views(
        &self,
        definitions: Vec<ViewDefinition>,
        options: PipelineBuildOptions,
    ) -> PipelineResult<()> {
        self.create_views_impl(definitions, options).await
    }

    async fn query_sql(&self, sql: &str) -> PipelineResult<Vec<RecordBatch>> {
        self.session
            .sql(sql)
            .await
            .map_err(|error| PipelineError::Execution(error.to_string()))?
            .collect()
            .await
            .map_err(|error| PipelineError::Execution(error.to_string()))
    }

    async fn create_database(
        &self,
        catalog: &str,
        request: CreateDatabaseRequest,
    ) -> PipelineResult<()> {
        self.formats
            .create_database(catalog, request)
            .await
            .map_err(|error| PipelineError::Catalog(error.to_string()))
    }

    async fn create_table(&self, request: CreateTableRequest) -> PipelineResult<()> {
        self.formats
            .create_table(request)
            .await
            .map_err(|error| PipelineError::Catalog(error.to_string()))?;
        Ok(())
    }

    async fn latest_version(&self, table: &TableIdentifier) -> PipelineResult<i64> {
        table_latest_version(&self.formats, table).await
    }

    async fn append_batches(
        &self,
        table: &TableIdentifier,
        batches: Vec<RecordBatch>,
    ) -> PipelineResult<()> {
        if self
            .view_set
            .read()
            .expect("view set lock poisoned")
            .as_ref()
            .is_some_and(|registered| registered.graph.node(table).is_some())
        {
            return Err(PipelineError::Invalid(format!(
                "cannot append directly to materialized view `{table}`"
            )));
        }
        self.formats
            .append_batches(&self.session, table, batches)
            .await
            .map_err(|error| PipelineError::Execution(error.to_string()))
    }

    async fn register_table_snapshot(&self, table: &TableIdentifier) -> PipelineResult<()> {
        self.formats
            .register_table_snapshot(&self.session, table)
            .await
            .map_err(|error| PipelineError::Catalog(error.to_string()))
    }

    async fn refresh_incremental(&self) -> PipelineResult<()> {
        self.refresh_engine()?.run(RefreshMode::Incremental).await
    }

    async fn refresh_full(&self) -> PipelineResult<()> {
        self.refresh_engine()?.run(RefreshMode::Full).await
    }
}

fn metadata_error(error: impl std::fmt::Display) -> PipelineError {
    PipelineError::Execution(format!("metadata: {error}"))
}

async fn table_latest_version(
    formats: &LakeFormatRegistry,
    table: &TableIdentifier,
) -> PipelineResult<i64> {
    formats
        .get_table(table)
        .await
        .map_err(|error| PipelineError::Catalog(error.to_string()))?
        .latest_version()
        .ok_or_else(|| PipelineError::Catalog(format!("table `{table}` has no snapshot")))
}

mod refresh;
mod registration;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use dynamic_lake_core::MaterializedViewDefinition;
    use dynamic_lake_datafusion::ivm::ROW_ID_COLUMN;
    use dynamic_lake_metastore_redb::RedbMetastoreFactory;

    use super::*;
    use crate::PipelineClient;

    async fn client_with_source() -> PipelineResult<(PipelineClient, tempfile::TempDir)> {
        let session = SessionContext::new();
        session
            .register_table(
                "source",
                Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![
                    Field::new(ROW_ID_COLUMN, DataType::Int64, false),
                    Field::new("value", DataType::Int64, false),
                ])))),
            )
            .map_err(|error| PipelineError::Execution(error.to_string()))?;
        let directory =
            tempfile::tempdir().map_err(|error| PipelineError::Execution(error.to_string()))?;
        let backend = InProcessPipelineClient::new(
            session,
            LakeFormatRegistry::new(),
            directory.path().join("metadata.redb"),
            &RedbMetastoreFactory,
        )
        .await?;
        Ok((PipelineClient::new(Arc::new(backend)), directory))
    }

    #[tokio::test]
    async fn in_process_tracks_mv_on_mv_dependency() -> PipelineResult<()> {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new(ROW_ID_COLUMN, DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        for name in ["source", "mv"] {
            session
                .register_table(name, Arc::new(EmptyTable::new(Arc::clone(&schema))))
                .map_err(|error| PipelineError::Execution(error.to_string()))?;
        }
        let directory =
            tempfile::tempdir().map_err(|error| PipelineError::Execution(error.to_string()))?;
        let backend = InProcessPipelineClient::new(
            session.clone(),
            LakeFormatRegistry::new(),
            directory.path().join("metadata.redb"),
            &RedbMetastoreFactory,
        )
        .await?;
        let planned = RegisteredViewSet::build(
            &backend.session,
            &backend.formats,
            vec![
                (
                    ViewDefinition {
                        name: "paimon.default.mv".to_string(),
                        sql: "SELECT value FROM source".to_string(),
                    },
                    PipelineBuildOptions::default(),
                ),
                (
                    ViewDefinition {
                        name: "paimon.default.downstream".to_string(),
                        sql: "SELECT value FROM mv".to_string(),
                    },
                    PipelineBuildOptions::default(),
                ),
            ],
        )
        .await?;
        let mv: TableIdentifier = "paimon.default.mv".parse()?;
        let downstream: TableIdentifier = "paimon.default.downstream".parse()?;
        assert!(
            planned
                .graph
                .node(&downstream)
                .expect("downstream view")
                .upstream
                .contains(&mv)
        );
        for (id, sql, cursor) in [
            (&mv, "SELECT value FROM source", 3),
            (&downstream, "SELECT value FROM mv", 4),
        ] {
            let definition = MaterializedViewDefinition::new(
                id.clone(),
                sql.to_string(),
                PipelineBuildOptions::default(),
                planned.plan_signatures[id].clone(),
            );
            let streams = planned
                .streams
                .lock()
                .await
                .get(id)
                .expect("planned streams")
                .values()
                .cloned()
                .map(|mut stream| {
                    stream.last_version = Some(cursor);
                    stream
                })
                .collect::<Vec<_>>();
            backend
                .metadata
                .create_materialized_view(&definition, &streams)
                .await
                .map_err(metadata_error)?;
        }
        drop(backend);
        let restored = InProcessPipelineClient::new(
            session,
            LakeFormatRegistry::new(),
            directory.path().join("metadata.redb"),
            &RedbMetastoreFactory,
        )
        .await?;
        let downstream_set = restored
            .view_set
            .read()
            .expect("view set lock poisoned")
            .as_ref()
            .cloned()
            .expect("restored view set");
        assert!(downstream_set.graph.node(&mv).is_some());
        assert!(
            downstream_set
                .graph
                .node(&downstream)
                .expect("restored downstream view")
                .upstream
                .contains(&mv)
        );
        assert_eq!(
            downstream_set.streams.lock().await[&downstream][&mv].last_version,
            Some(4)
        );
        Ok(())
    }

    #[tokio::test]
    async fn in_process_rejects_invalid_view_name() -> PipelineResult<()> {
        let (client, _directory) = client_with_source().await?;
        let result = client
            .materialized_view("a b", "SELECT value FROM source")
            .create()
            .await;
        assert!(matches!(result, Err(PipelineError::Invalid(_))));
        Ok(())
    }
}
