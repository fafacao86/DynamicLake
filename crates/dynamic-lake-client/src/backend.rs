//! Client backend for SQL planning, materialized views, and storage operations.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::TableReference;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::ObjectName;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use dynamic_lake_core::{
    DynamicTable, GraphOperations, IncrementInterval, IvmStreamObject, MaterializedViewDefinition,
    MetastoreFactory, MetastoreRepository, PipelineBuildOptions, PipelineError, PipelineGraph,
    PipelineResult, StreamStatus, TableIdentifier,
};
use dynamic_lake_datafusion::{
    DataFusionPlanOptimizer,
    ivm::{
        IvmNormalizer, IvmPlanSignatureGenerator, IvmRefreshContext, IvmRewriter,
        normalize::{
            materialized_view_delta_plan, materialized_view_plan, materialized_view_schema,
            register_ivm_functions, row_id_columns,
        },
        physical::install_lake_scans,
    },
};
use dynamic_lake_format::{CreateDatabaseRequest, CreateTableRequest, LakeFormatRegistry};
use serde::{Deserialize, Serialize};

use crate::ViewDefinition;

#[async_trait]
pub trait PipelineClientBackend: Send + Sync {
    async fn define_views(
        &self,
        definitions: Vec<ViewDefinition>,
        options: PipelineBuildOptions,
        create_storage: bool,
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

    async fn refresh_incremental(&self, view: &TableIdentifier) -> PipelineResult<()>;

    async fn refresh_full(&self, view: &TableIdentifier) -> PipelineResult<()>;
}

struct RegisteredViewSet {
    graph: PipelineGraph,
    plan_signatures: BTreeMap<TableIdentifier, String>,
    streams:
        tokio::sync::Mutex<BTreeMap<TableIdentifier, BTreeMap<TableIdentifier, IvmStreamObject>>>,
}

const SQL_DEFINITION_FORMAT: &str = "dynamic-lake-sql-v1";

#[derive(Serialize, Deserialize)]
struct StoredSqlDefinition {
    sql: String,
    options: PipelineBuildOptions,
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

    async fn load_views(&self) -> PipelineResult<()> {
        let stored = self.stored_definitions().await?;
        if stored.is_empty() {
            return Ok(());
        }
        let mut view_set = self
            .build_views(
                stored
                    .iter()
                    .map(|(definition, payload)| {
                        (
                            ViewDefinition {
                                name: definition.node_id.to_string(),
                                sql: payload.sql.clone(),
                            },
                            payload.options.clone(),
                        )
                    })
                    .collect(),
            )
            .await?;
        self.restore_streams(&mut view_set, &stored).await?;
        *self.view_set.write().expect("view set lock poisoned") = Some(Arc::new(view_set));
        Ok(())
    }

    fn view_set(&self, view: &TableIdentifier) -> PipelineResult<Arc<RegisteredViewSet>> {
        self.view_set
            .read()
            .expect("view set lock poisoned")
            .as_ref()
            .cloned()
            .filter(|registered| registered.graph.node(view).is_some())
            .ok_or_else(|| PipelineError::Invalid(format!("view `{view}` is not registered")))
    }

    async fn stored_definitions(
        &self,
    ) -> PipelineResult<Vec<(MaterializedViewDefinition, StoredSqlDefinition)>> {
        let mut definitions = Vec::new();
        for definition in self
            .metadata
            .list_mv_definitions()
            .await
            .map_err(metadata_error)?
        {
            if definition.definition_format != SQL_DEFINITION_FORMAT {
                return Err(PipelineError::Invalid(format!(
                    "unsupported MV definition format `{}` for `{}`",
                    definition.definition_format, definition.node_id
                )));
            }
            let payload = serde_json::from_slice(&definition.definition)
                .map_err(|error| PipelineError::Invalid(error.to_string()))?;
            definitions.push((definition, payload));
        }
        Ok(definitions)
    }

    async fn restore_streams(
        &self,
        view_set: &mut RegisteredViewSet,
        stored: &[(MaterializedViewDefinition, StoredSqlDefinition)],
    ) -> PipelineResult<()> {
        for (definition, _) in stored {
            let signature = view_set
                .plan_signatures
                .get(&definition.node_id)
                .ok_or_else(|| {
                    PipelineError::Invalid(format!("MV `{}` was not rebuilt", definition.node_id))
                })?;
            if signature != &definition.plan_signature {
                return Err(PipelineError::Invalid(format!(
                    "IVM plan signature changed for `{}`; full refresh required",
                    definition.node_id
                )));
            }
            let streams = self
                .metadata
                .list_stream_objects(&definition.node_id)
                .await
                .map_err(metadata_error)?;
            let expected = view_set
                .streams
                .get_mut()
                .get_mut(&definition.node_id)
                .ok_or_else(|| {
                    PipelineError::Invalid(format!("MV `{}` has no streams", definition.node_id))
                })?;
            if expected.len() != streams.len()
                || streams.iter().any(|stream| {
                    stream.mv_node_id != definition.node_id
                        || !expected.contains_key(&stream.source)
                })
            {
                return Err(PipelineError::Invalid(format!(
                    "source streams changed for `{}`; full refresh required",
                    definition.node_id
                )));
            }
            for stream in streams {
                expected.insert(stream.source.clone(), stream);
            }
        }
        Ok(())
    }

    async fn execute_plan(&self, plan: &LogicalPlan) -> PipelineResult<Vec<RecordBatch>> {
        let execution = self
            .session
            .state()
            .create_physical_plan(plan)
            .await
            .map_err(|error| PipelineError::Execution(error.to_string()))?;
        collect(execution, self.session.task_ctx())
            .await
            .map_err(|error| PipelineError::Execution(error.to_string()))
    }

    async fn plan_incremental(
        &self,
        graph: &PipelineGraph,
        view: &TableIdentifier,
        expected_signature: &str,
        refresh: IvmRefreshContext,
    ) -> PipelineResult<Option<LogicalPlan>> {
        let node = graph.node(view).ok_or_else(|| {
            PipelineError::Invalid(format!("materialized view `{view}` is not defined"))
        })?;
        let current_signature = IvmPlanSignatureGenerator.generate(&node.query);
        if current_signature.sha256 != expected_signature {
            return Err(PipelineError::Invalid(format!(
                "IVM plan signature changed for `{view}`: expected {expected_signature}, current {}; full refresh required",
                current_signature.sha256
            )));
        }
        let rewriter = IvmRewriter::new(&node.query, refresh)
            .map_err(|error| PipelineError::Invalid(error.to_string()))?;
        let plan = rewriter
            .rewrite_outer_join_primary(&node.query)
            .map_err(|error| PipelineError::Invalid(error.to_string()))?;
        match plan {
            Some(plan) => {
                let plan = dynamic_lake_datafusion::ivm::materialize_ctes(&self.session, plan)
                    .await
                    .map_err(|error| PipelineError::Execution(error.to_string()))?;
                let plan =
                    materialized_view_delta_plan(plan, row_id_columns(node.query.schema()).len())
                        .map_err(|error| PipelineError::Invalid(error.to_string()))?;
                Ok(Some(plan))
            }
            None => Ok(None),
        }
    }

    async fn build_views(
        &self,
        definitions: Vec<(ViewDefinition, PipelineBuildOptions)>,
    ) -> PipelineResult<RegisteredViewSet> {
        let optimizer = DataFusionPlanOptimizer::new(self.session.state());
        let mut drafts = Vec::new();
        for (definition, options) in definitions {
            let query = self
                .session
                .state()
                .create_logical_plan(&definition.sql)
                .await
                .map_err(|error| PipelineError::Execution(error.to_string()))?;
            let optimized = optimizer.optimize(query)?;
            let normalized = IvmNormalizer::default()
                .normalize(optimized)
                .map_err(|error| {
                    PipelineError::Invalid(format!("IVM normalize failed: {error}"))
                })?;
            drafts.push(prepare_table(definition.name, normalized.plan, &options)?);
        }
        let ids = drafts
            .iter()
            .map(|draft| draft.id.clone())
            .collect::<BTreeSet<_>>();
        if ids.len() != drafts.len() {
            return Err(PipelineError::Invalid(
                "duplicate dynamic table name".to_string(),
            ));
        }
        let plan_signatures: BTreeMap<_, _> = drafts
            .iter()
            .map(|draft| {
                (
                    draft.id.clone(),
                    IvmPlanSignatureGenerator.generate(&draft.query).sha256,
                )
            })
            .collect();
        let mut nodes = BTreeMap::new();
        let mut streams = BTreeMap::new();
        for draft in drafts {
            let mut upstream = BTreeSet::new();
            let mut external_inputs = BTreeSet::new();
            for reference in draft.references {
                if ids.contains(&reference) {
                    upstream.insert(reference);
                } else {
                    external_inputs.insert(reference);
                }
            }
            let stream_objects: BTreeMap<_, _> = upstream
                .iter()
                .chain(&external_inputs)
                .map(|source| {
                    (
                        source.clone(),
                        IvmStreamObject::create(draft.id.clone(), source.clone(), None),
                    )
                })
                .collect();
            streams.insert(draft.id.clone(), stream_objects.clone());
            nodes.insert(
                draft.id.clone(),
                DynamicTable {
                    id: draft.id,
                    query: draft.query,
                    upstream,
                    external_inputs,
                    stream_objects,
                },
            );
        }
        Ok(RegisteredViewSet {
            graph: PipelineGraph::try_new(nodes)?,
            plan_signatures,
            streams: tokio::sync::Mutex::new(streams),
        })
    }
}

#[async_trait]
impl PipelineClientBackend for InProcessPipelineClient {
    async fn define_views(
        &self,
        definitions: Vec<ViewDefinition>,
        options: PipelineBuildOptions,
        create_storage: bool,
    ) -> PipelineResult<()> {
        if !create_storage {
            self.build_views(
                definitions
                    .into_iter()
                    .map(|definition| (definition, options.clone()))
                    .collect(),
            )
            .await?;
            return Ok(());
        }
        let existing = self.stored_definitions().await?;
        let existing_ids = existing
            .iter()
            .map(|(definition, _)| definition.node_id.clone())
            .collect::<BTreeSet<_>>();
        let mut new_ids = BTreeSet::new();
        for definition in &definitions {
            let id = options.canonical_table(&parse_object_name(&definition.name)?);
            if existing_ids.contains(&id) || !new_ids.insert(id.clone()) {
                return Err(PipelineError::Invalid(format!(
                    "materialized view `{id}` already exists"
                )));
            }
        }
        let all_definitions = existing
            .iter()
            .map(|(definition, payload)| {
                (
                    ViewDefinition {
                        name: definition.node_id.to_string(),
                        sql: payload.sql.clone(),
                    },
                    payload.options.clone(),
                )
            })
            .chain(
                definitions
                    .iter()
                    .cloned()
                    .map(|definition| (definition, options.clone())),
            )
            .collect();
        let mut view_set = self.build_views(all_definitions).await?;
        self.restore_streams(&mut view_set, &existing).await?;
        let mut new_metadata = Vec::with_capacity(definitions.len());
        for definition in definitions {
            let id = options.canonical_table(&parse_object_name(&definition.name)?);
            let payload = StoredSqlDefinition {
                sql: definition.sql,
                options: options.clone(),
            };
            new_metadata.push(MaterializedViewDefinition {
                node_id: id.clone(),
                definition_format: SQL_DEFINITION_FORMAT.to_string(),
                definition: serde_json::to_vec(&payload)
                    .map_err(|error| PipelineError::Invalid(error.to_string()))?,
                plan_signature: view_set.plan_signatures[&id].clone(),
                output_version: None,
            });
        }
        for view in view_set
            .graph
            .topological_order()
            .iter()
            .filter(|view| new_ids.contains(*view))
        {
            let node = view_set.graph.node(view).expect("planned MV node");
            let catalog = view.parts().first().ok_or_else(|| {
                PipelineError::Invalid(format!("materialized view `{view}` has no catalog"))
            })?;
            let format = self.formats.catalog(catalog).ok_or_else(|| {
                PipelineError::Catalog(format!("no lake format registered for catalog `{catalog}`"))
            })?;
            format
                .create_materialized_view_table(view.clone(), materialized_view_schema(&node.query))
                .await
                .map_err(|error| PipelineError::Catalog(error.to_string()))?;
        }
        for definition in &new_metadata {
            let streams = view_set
                .streams
                .lock()
                .await
                .get(&definition.node_id)
                .expect("planned MV streams")
                .values()
                .cloned()
                .collect::<Vec<_>>();
            self.metadata
                .create_materialized_view(definition, &streams)
                .await
                .map_err(metadata_error)?;
        }
        *self.view_set.write().expect("view set lock poisoned") = Some(Arc::new(view_set));
        Ok(())
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
        self.formats
            .get_table(table)
            .await
            .map_err(|error| PipelineError::Catalog(error.to_string()))?
            .latest_version()
            .ok_or_else(|| PipelineError::Catalog(format!("table `{table}` has no snapshot")))
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

    async fn refresh_incremental(&self, view: &TableIdentifier) -> PipelineResult<()> {
        let definitions = self.view_set(view)?;
        // Serialize refreshes of this view set through cursor discovery and commit.
        let mut streams = definitions.streams.lock().await;
        let view_streams = streams.get_mut(view).ok_or_else(|| {
            PipelineError::Invalid(format!("materialized view `{view}` is not defined"))
        })?;
        let mut intervals = BTreeMap::new();
        let mut changed = Vec::new();
        for (source, stream) in view_streams.iter() {
            if stream.status != StreamStatus::Active {
                return Err(PipelineError::Invalid(format!(
                    "stream `{}` is not active",
                    stream.name
                )));
            }
            let latest = self.latest_version(source).await?;
            let interval = stream
                .pending_interval(latest)
                .map_err(|error| PipelineError::Invalid(error.to_string()))?;
            if interval.is_some() {
                changed.push(source.clone());
            }
            let before = stream.last_version.expect("validated stream cursor");
            intervals.insert(
                source.clone(),
                IncrementInterval::try_new(before, latest)
                    .map_err(|error| PipelineError::Invalid(error.to_string()))?,
            );
        }
        let changed_table = match changed.as_slice() {
            [] => return Ok(()),
            [source] => source.clone(),
            _ => {
                return Err(PipelineError::Invalid(format!(
                    "multiple sources changed for `{view}`; full refresh required"
                )));
            }
        };
        let refresh = IvmRefreshContext::new(changed_table, intervals.clone())
            .map_err(|error| PipelineError::Invalid(error.to_string()))?;
        let expected_signature = definitions.plan_signatures.get(view).ok_or_else(|| {
            PipelineError::Invalid(format!("materialized view `{view}` is not defined"))
        })?;
        match self
            .plan_incremental(&definitions.graph, view, expected_signature, refresh)
            .await?
        {
            Some(plan) => {
                let expected_snapshot = self.latest_version(view).await?;
                let batches = self.execute_plan(&plan).await?;
                self.formats
                    .write_changes(&self.session, view, batches, expected_snapshot)
                    .await
                    .map_err(|error| PipelineError::Execution(error.to_string()))?;
                let advanced = view_streams
                    .iter()
                    .map(|(source, stream)| {
                        stream
                            .advanced_to(intervals[source].after_inclusive)
                            .map_err(|error| PipelineError::Invalid(error.to_string()))
                    })
                    .collect::<PipelineResult<Vec<_>>>()?;
                self.save_refresh_metadata(view, &advanced).await?;
                for stream in advanced {
                    view_streams.insert(stream.source.clone(), stream);
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    async fn refresh_full(&self, view: &TableIdentifier) -> PipelineResult<()> {
        let definitions = self.view_set(view)?;
        let mut streams = definitions.streams.lock().await;
        let view_streams = streams.get_mut(view).ok_or_else(|| {
            PipelineError::Invalid(format!("materialized view `{view}` is not defined"))
        })?;
        let mut source_versions = BTreeMap::new();
        for source in view_streams.keys() {
            source_versions.insert(source.clone(), self.latest_version(source).await?);
        }
        let node = definitions.graph.node(view).ok_or_else(|| {
            PipelineError::Invalid(format!("materialized view `{view}` is not defined"))
        })?;
        let plan = materialized_view_plan(node.query.clone())
            .map_err(|error| PipelineError::Invalid(error.to_string()))?;
        let batches = self.execute_plan(&plan).await?;
        self.formats
            .overwrite_batches(&self.session, view, batches)
            .await
            .map_err(|error| PipelineError::Execution(error.to_string()))?;
        let advanced = view_streams
            .iter()
            .map(|(source, stream)| {
                let mut stream = stream.clone();
                stream.last_version = Some(source_versions[source]);
                stream
            })
            .collect::<Vec<_>>();
        self.save_refresh_metadata(view, &advanced).await?;
        for stream in advanced {
            view_streams.insert(stream.source.clone(), stream);
        }
        Ok(())
    }
}

impl InProcessPipelineClient {
    async fn save_refresh_metadata(
        &self,
        view: &TableIdentifier,
        streams: &[IvmStreamObject],
    ) -> PipelineResult<()> {
        let mut definition = self
            .metadata
            .load_mv_definition(view)
            .await
            .map_err(metadata_error)?
            .ok_or_else(|| PipelineError::Invalid(format!("MV `{view}` has no metadata")))?;
        definition.output_version = Some(self.latest_version(view).await?);
        self.metadata
            .save_refresh_metadata(&definition, streams)
            .await
            .map_err(metadata_error)
    }
}

fn metadata_error(error: impl std::fmt::Display) -> PipelineError {
    PipelineError::Execution(format!("metadata: {error}"))
}

struct PreparedTable {
    id: TableIdentifier,
    query: LogicalPlan,
    references: BTreeSet<TableIdentifier>,
}

fn prepare_table(
    name: String,
    query: LogicalPlan,
    options: &PipelineBuildOptions,
) -> PipelineResult<PreparedTable> {
    let id = options.canonical_table(&parse_object_name(&name)?);
    let mut references = BTreeSet::new();
    query
        .apply(&mut |node: &LogicalPlan| {
            if let LogicalPlan::TableScan(scan) = node {
                let identifier =
                    table_reference_to_identifier(&scan.table_name).map_err(|error| {
                        datafusion::error::DataFusionError::External(Box::new(error))
                    })?;
                references.insert(identifier);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .map_err(|error| PipelineError::Invalid(error.to_string()))?;
    Ok(PreparedTable {
        id,
        query,
        references: references
            .into_iter()
            .map(|reference| options.canonical_table(reference.parts()))
            .collect(),
    })
}

fn parse_object_name(name: &str) -> PipelineResult<Vec<String>> {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(name)
        .map_err(|error| PipelineError::Invalid(format!("invalid table name `{name}`: {error}")))?;
    let object = parser
        .parse_object_name(false)
        .map_err(|error| PipelineError::Invalid(format!("invalid table name `{name}`: {error}")))?;
    if parser.peek_token().token != Token::EOF {
        return Err(PipelineError::Invalid(format!(
            "invalid table name `{name}`: trailing tokens"
        )));
    }
    object_name_parts(&object)
}

fn object_name_parts(name: &ObjectName) -> PipelineResult<Vec<String>> {
    name.0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|identifier| identifier.value.clone())
                .ok_or_else(|| {
                    PipelineError::Invalid(
                        "dynamic table name must not contain identifier functions".to_string(),
                    )
                })
        })
        .collect()
}

fn table_reference_to_identifier(reference: &TableReference) -> PipelineResult<TableIdentifier> {
    let parts = match reference {
        TableReference::Bare { table } => vec![table.to_string()],
        TableReference::Partial { schema, table } => vec![schema.to_string(), table.to_string()],
        TableReference::Full {
            catalog,
            schema,
            table,
        } => {
            vec![catalog.to_string(), schema.to_string(), table.to_string()]
        }
    };
    TableIdentifier::try_new(parts)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
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
    async fn in_process_validates_view_sql_without_registering_it() -> PipelineResult<()> {
        let (client, _directory) = client_with_source().await?;
        let view: TableIdentifier = "paimon.default.mv".parse()?;
        client
            .materialized_view(view.to_string(), "SELECT value FROM source")
            .build()
            .await?;
        let error = client
            .trigger_incremental_refresh(&view)
            .await
            .expect_err("validation must not register the view");
        assert!(error.to_string().contains("not registered"));
        Ok(())
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
        let planned = backend
            .build_views(vec![
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
            ])
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
            let definition = MaterializedViewDefinition {
                node_id: id.clone(),
                definition_format: SQL_DEFINITION_FORMAT.to_string(),
                definition: serde_json::to_vec(&StoredSqlDefinition {
                    sql: sql.to_string(),
                    options: PipelineBuildOptions::default(),
                })
                .map_err(|error| PipelineError::Invalid(error.to_string()))?,
                plan_signature: planned.plan_signatures[id].clone(),
                output_version: None,
            };
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
        let upstream_set = restored.view_set(&mv)?;
        let downstream_set = restored.view_set(&downstream)?;
        assert!(Arc::ptr_eq(&upstream_set, &downstream_set));
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
            .build()
            .await;
        assert!(matches!(result, Err(PipelineError::Invalid(_))));
        Ok(())
    }
}
