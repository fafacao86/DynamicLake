//! User-facing DynamicLake materialized-view definition client.
//!
//! Users define materialized views. Dependency graphs, execution plans, and
//! refresh scopes are derived internally.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

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
    DynamicTable, GraphOperations, IvmStreamObject, PipelineBuildOptions, PipelineError,
    PipelineGraph, PipelineResult, TableIdentifier,
};
use dynamic_lake_datafusion::{
    DataFusionPipelinePlan, DataFusionPlanOptimizer, compile_pipeline_plan,
    ivm::{
        IvmNormalizer, IvmPlanSignatureGenerator, IvmRefreshContext, IvmRewriter,
        normalize::{
            materialized_view_delta_plan, materialized_view_plan, materialized_view_schema,
            register_ivm_functions, row_id_columns,
        },
        physical::install_lake_scans,
    },
};
pub use dynamic_lake_format;
use dynamic_lake_format::{
    CreateDatabaseRequest, CreateTableRequest, LakeFormatRegistry, LakeTable,
};

#[derive(Clone)]
pub struct PipelineClient {
    session: SessionContext,
    build_options: PipelineBuildOptions,
    formats: LakeFormatRegistry,
}

impl PipelineClient {
    pub fn new(session: SessionContext) -> Self {
        register_ivm_functions(&session);
        Self {
            session,
            build_options: PipelineBuildOptions::default(),
            formats: LakeFormatRegistry::new(),
        }
    }

    pub fn with_build_options(mut self, options: PipelineBuildOptions) -> Self {
        self.build_options = options;
        self
    }

    pub fn with_formats(mut self, formats: LakeFormatRegistry) -> Self {
        self.session = install_lake_scans(self.session, formats.clone());
        self.formats = formats;
        self
    }

    pub fn session(&self) -> &SessionContext {
        &self.session
    }

    pub fn formats(&self) -> &LakeFormatRegistry {
        &self.formats
    }

    pub async fn plan_sql(&self, sql: &str) -> PipelineResult<LogicalPlan> {
        self.session
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|error| PipelineError::Execution(error.to_string()))
    }

    pub async fn query_sql(&self, sql: &str) -> PipelineResult<Vec<RecordBatch>> {
        self.session
            .sql(sql)
            .await
            .map_err(|error| PipelineError::Execution(error.to_string()))?
            .collect()
            .await
            .map_err(|error| PipelineError::Execution(error.to_string()))
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

    pub async fn create_database(
        &self,
        catalog: &str,
        request: CreateDatabaseRequest,
    ) -> PipelineResult<()> {
        self.formats
            .create_database(catalog, request)
            .await
            .map_err(|error| PipelineError::Catalog(error.to_string()))
    }

    pub async fn create_table(
        &self,
        request: CreateTableRequest,
    ) -> PipelineResult<Arc<dyn LakeTable>> {
        self.formats
            .create_table(request)
            .await
            .map_err(|error| PipelineError::Catalog(error.to_string()))
    }

    pub async fn latest_version(&self, table: &TableIdentifier) -> PipelineResult<i64> {
        self.formats
            .get_table(table)
            .await
            .map_err(|error| PipelineError::Catalog(error.to_string()))?
            .latest_version()
            .ok_or_else(|| PipelineError::Catalog(format!("table `{table}` has no snapshot")))
    }

    pub async fn append_batches(
        &self,
        table: &TableIdentifier,
        batches: Vec<RecordBatch>,
    ) -> PipelineResult<()> {
        self.formats
            .append_batches(&self.session, table, batches)
            .await
            .map_err(|error| PipelineError::Execution(error.to_string()))
    }

    pub async fn register_table_snapshot(&self, table: &TableIdentifier) -> PipelineResult<()> {
        self.formats
            .register_table_snapshot(&self.session, table)
            .await
            .map_err(|error| PipelineError::Catalog(error.to_string()))
    }

    pub fn materialized_view(
        &self,
        name: impl Into<String>,
        query: LogicalPlan,
    ) -> PipelineResult<MaterializedViewBuilder> {
        MaterializedViewBuilder {
            client: self.clone(),
            tables: Vec::new(),
        }
        .materialized_view(name, query)
    }

    pub async fn materialized_view_sql(
        &self,
        name: impl Into<String>,
        sql: &str,
    ) -> PipelineResult<MaterializedViewBuilder> {
        self.materialized_view(name, self.plan_sql(sql).await?)
    }

    pub fn compile(
        &self,
        definitions: &MaterializedViewGraph,
    ) -> PipelineResult<DataFusionPipelinePlan> {
        compile_pipeline_plan(definitions.graph())
    }

    async fn create_materialized_view_tables(
        &self,
        definitions: &MaterializedViewGraph,
    ) -> PipelineResult<()> {
        for view in definitions.graph().topological_order() {
            let node = definitions.graph().node(view).ok_or_else(|| {
                PipelineError::Invalid(format!("materialized view `{view}` is not defined"))
            })?;
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
        Ok(())
    }

    pub async fn trigger_incremental_refresh(
        &self,
        definitions: &MaterializedViewGraph,
        view: &TableIdentifier,
        expected_signature: &str,
        refresh: IvmRefreshContext,
    ) -> PipelineResult<Option<Vec<RecordBatch>>> {
        match self
            .plan_incremental(definitions, view, expected_signature, refresh)
            .await?
        {
            Some(plan) => {
                let expected_snapshot = self.latest_version(view).await?;
                let batches = self.execute_plan(&plan).await?;
                self.formats
                    .write_changes(&self.session, view, batches.clone(), expected_snapshot)
                    .await
                    .map_err(|error| PipelineError::Execution(error.to_string()))?;
                Ok(Some(batches))
            }
            None => Ok(None),
        }
    }

    /// Recompute one view and persist its current snapshot.
    pub async fn trigger_full_refresh(
        &self,
        definitions: &MaterializedViewGraph,
        view: &TableIdentifier,
    ) -> PipelineResult<()> {
        let node = definitions.graph().node(view).ok_or_else(|| {
            PipelineError::Invalid(format!("materialized view `{view}` is not defined"))
        })?;
        let plan = materialized_view_plan(node.query.clone())
            .map_err(|error| PipelineError::Invalid(error.to_string()))?;
        let batches = self.execute_plan(&plan).await?;
        self.formats
            .overwrite_batches(&self.session, view, batches)
            .await
            .map_err(|error| PipelineError::Execution(error.to_string()))
    }

    async fn plan_incremental(
        &self,
        definitions: &MaterializedViewGraph,
        view: &TableIdentifier,
        expected_signature: &str,
        refresh: IvmRefreshContext,
    ) -> PipelineResult<Option<LogicalPlan>> {
        let node = definitions.graph().node(view).ok_or_else(|| {
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
}

pub struct MaterializedViewBuilder {
    client: PipelineClient,
    tables: Vec<PendingTable>,
}

impl MaterializedViewBuilder {
    /// Build definitions and create their managed storage tables.
    pub async fn create(self) -> PipelineResult<MaterializedViewGraph> {
        let client = self.client.clone();
        let definitions = self.build()?;
        client.create_materialized_view_tables(&definitions).await?;
        Ok(definitions)
    }

    /// Add a persisted dynamic table from an already analyzed DataFusion plan.
    ///
    /// Every materialized view is IVM-normalized when it is added. Whether a
    /// later refresh is incremental is decided by [`RefreshPlan`], not by
    /// having a separate view creation API.
    pub fn materialized_view(
        self,
        name: impl Into<String>,
        query: LogicalPlan,
    ) -> PipelineResult<Self> {
        let optimizer = DataFusionPlanOptimizer::new(self.client.session().state());
        let optimized = optimizer.optimize(query)?;
        let normalized = IvmNormalizer::default()
            .normalize(optimized)
            .map_err(|error| PipelineError::Invalid(format!("IVM normalize failed: {error}")))?;
        let mut builder = self;
        builder.tables.push(PendingTable {
            name: name.into(),
            query: normalized.plan,
        });
        Ok(builder)
    }

    pub fn build(self) -> PipelineResult<MaterializedViewGraph> {
        let drafts = self
            .tables
            .into_iter()
            .map(|table| prepare_table(table, &self.client.build_options))
            .collect::<PipelineResult<Vec<_>>>()?;
        let ids = drafts
            .iter()
            .map(|draft| draft.id.clone())
            .collect::<BTreeSet<_>>();
        if ids.len() != drafts.len() {
            return Err(PipelineError::Invalid(
                "duplicate dynamic table name".to_string(),
            ));
        }
        let plan_signatures = drafts
            .iter()
            .map(|draft| {
                (
                    draft.id.clone(),
                    IvmPlanSignatureGenerator.generate(&draft.query).sha256,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut nodes = BTreeMap::new();
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
            let stream_objects = external_inputs
                .iter()
                .map(|source| {
                    (
                        source.clone(),
                        IvmStreamObject::create(draft.id.clone(), source.clone(), None),
                    )
                })
                .collect();
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
        Ok(MaterializedViewGraph {
            graph: PipelineGraph::try_new(nodes)?,
            plan_signatures,
        })
    }
}

pub struct MaterializedViewGraph {
    graph: PipelineGraph,
    plan_signatures: BTreeMap<TableIdentifier, String>,
}

impl MaterializedViewGraph {
    pub fn graph(&self) -> &PipelineGraph {
        &self.graph
    }

    /// Persist this digest with the materialized view's initial output.
    pub fn plan_signature(&self, view: &TableIdentifier) -> Option<&str> {
        self.plan_signatures.get(view).map(String::as_str)
    }

    pub fn into_graph(self) -> PipelineGraph {
        self.graph
    }
}

struct PendingTable {
    name: String,
    query: LogicalPlan,
}

struct PreparedTable {
    id: TableIdentifier,
    query: LogicalPlan,
    references: BTreeSet<TableIdentifier>,
}

fn prepare_table(
    table: PendingTable,
    options: &PipelineBuildOptions,
) -> PipelineResult<PreparedTable> {
    let name = parse_object_name(&table.name)?;
    let id = options.canonical_table(&name);
    let mut references = BTreeSet::new();
    table
        .query
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
        query: table.query,
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
        TableReference::Partial { schema, table } => {
            vec![schema.to_string(), table.to_string()]
        }
        TableReference::Full {
            catalog,
            schema,
            table,
        } => vec![catalog.to_string(), schema.to_string(), table.to_string()],
    };
    TableIdentifier::try_new(parts)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use dynamic_lake_core::{GraphOperations, IncrementInterval};
    use dynamic_lake_datafusion::ivm::ROW_ID_COLUMN;

    use super::*;

    #[tokio::test]
    async fn client_builds_graph_from_logical_plans() -> PipelineResult<()> {
        let session = SessionContext::new();
        let clean_schema = Arc::new(Schema::new(vec![
            Field::new(ROW_ID_COLUMN, DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        session
            .register_table("clean", Arc::new(EmptyTable::new(clean_schema)))
            .map_err(|error| PipelineError::Execution(error.to_string()))?;
        let client = PipelineClient::new(session);
        let clean_plan = client.plan_sql("SELECT 1 AS amount").await?;
        let summary_plan = client.plan_sql("SELECT * FROM clean").await?;
        let builder = client.materialized_view("clean", clean_plan)?;
        let definitions = builder
            .materialized_view("summary", summary_plan)?
            .build()?;
        let clean: TableIdentifier = "paimon.default.clean".parse()?;
        let summary: TableIdentifier = "paimon.default.summary".parse()?;
        assert_eq!(
            definitions
                .graph()
                .node(&summary)
                .map(|node| &node.upstream),
            Some(&BTreeSet::from([clean]))
        );
        Ok(())
    }

    #[test]
    fn client_rejects_invalid_table_name() {
        let client = PipelineClient::new(SessionContext::new());
        let query = datafusion::logical_expr::LogicalPlanBuilder::empty(false)
            .build()
            .expect("empty plan");
        let result = client
            .materialized_view("a b", query)
            .and_then(|builder| builder.build());
        assert!(matches!(result, Err(PipelineError::Invalid(_))));
    }

    #[tokio::test]
    async fn client_normalizes_datafusion_logical_plans() -> PipelineResult<()> {
        let client = PipelineClient::new(SessionContext::new());
        let query = client.plan_sql("SELECT 42 AS answer").await?;
        let builder = client.materialized_view("answer", query)?;
        let definitions = builder.build()?;
        let plan = client.compile(&definitions)?;
        let answer: TableIdentifier = "paimon.default.answer".parse()?;
        assert!(plan.node(&answer).is_some());
        Ok(())
    }

    #[tokio::test]
    async fn materialized_view_normalizes_before_storing_definition() -> PipelineResult<()> {
        let session = SessionContext::new();
        for (name, key) in [("left_t", "left_id"), ("right_t", "right_id")] {
            session
                .register_table(
                    name,
                    Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![
                        Field::new(ROW_ID_COLUMN, DataType::Int64, false),
                        Field::new(key, DataType::Int64, false),
                    ])))),
                )
                .map_err(|error| PipelineError::Execution(error.to_string()))?;
        }
        let client = PipelineClient::new(session);
        let query = client
            .plan_sql(
                "SELECT left_id, right_id
                 FROM left_t LEFT JOIN right_t ON left_id = right_id",
            )
            .await?;
        let builder = client.materialized_view("joined", query)?;
        let definitions = builder.build()?;
        let joined: TableIdentifier = "paimon.default.joined".parse()?;
        let definition = definitions
            .graph()
            .node(&joined)
            .expect("normalized dynamic table");
        assert!(definitions.plan_signature(&joined).is_some());
        let row_id_count = definition
            .query
            .schema()
            .iter()
            .filter(|(_, field)| field.name() == ROW_ID_COLUMN)
            .count();
        assert_eq!(row_id_count, 2);
        assert_eq!(definition.stream_objects.len(), 2);
        assert!(
            definition
                .stream_objects
                .values()
                .all(|stream| stream.mv_node_id == joined)
        );
        Ok(())
    }

    #[tokio::test]
    async fn incremental_plan_rejects_mismatched_signature()
    -> Result<(), Box<dyn std::error::Error>> {
        let session = SessionContext::new();
        session.register_table(
            "source",
            Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![
                Field::new(ROW_ID_COLUMN, DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ])))),
        )?;
        let client = PipelineClient::new(session);
        let query = client.plan_sql("SELECT value FROM source").await?;
        let definitions = client.materialized_view("mv", query)?.build()?;
        let view: TableIdentifier = "paimon.default.mv".parse()?;
        let source: TableIdentifier = "paimon.default.source".parse()?;
        let refresh = IvmRefreshContext::new(
            source.clone(),
            BTreeMap::from([(source, IncrementInterval::try_new(1, 2)?)]),
        )?;
        let error = client
            .plan_incremental(&definitions, &view, "wrong-signature", refresh)
            .await
            .expect_err("signature mismatch must reject incremental refresh");
        assert!(error.to_string().contains("full refresh required"));
        Ok(())
    }
}
