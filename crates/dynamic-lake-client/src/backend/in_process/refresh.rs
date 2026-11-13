use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use dynamic_lake_core::{
    DynamicTable, GraphExecution, GraphOperations, IncrementInterval, IvmStreamObject,
    MetastoreRepository, NodeExecution, NodeExecutionPlanner, PipelineError, PipelineResult,
    RefreshMode, StreamStatus, TableIdentifier,
};
use dynamic_lake_datafusion::ivm::cte::{CteQuery, CteQueryExecutor, collect_with_peak};
use dynamic_lake_datafusion::ivm::{
    IvmPlanSignatureGenerator, IvmRefreshContext, IvmSnapshotPlanner,
    normalize::materialized_view_plan, plan_incremental_view,
};
use dynamic_lake_format::LakeFormatRegistry;
use tracing::info;

use super::{RegisteredViewSet, metadata_error, table_latest_version};

#[derive(Clone)]
pub(super) struct RefreshEngine {
    session: SessionContext,
    formats: LakeFormatRegistry,
    metadata: MetastoreRepository,
    definitions: Arc<RegisteredViewSet>,
}

impl RefreshEngine {
    pub(super) fn new(
        session: SessionContext,
        formats: LakeFormatRegistry,
        metadata: MetastoreRepository,
        definitions: Arc<RegisteredViewSet>,
    ) -> Self {
        Self {
            session,
            formats,
            metadata,
            definitions,
        }
    }

    pub(super) async fn run(&self, mode: RefreshMode) -> PipelineResult<()> {
        let _guard = self.definitions.refresh_gate.lock().await;
        GraphExecution::new(&self.definitions.graph, self)
            .run(mode)
            .await
    }

    async fn latest_version(&self, table: &TableIdentifier) -> PipelineResult<i64> {
        table_latest_version(&self.formats, table).await
    }

    async fn create_physical_plan(
        &self,
        plan: &LogicalPlan,
    ) -> PipelineResult<Arc<dyn ExecutionPlan>> {
        self.session
            .state()
            .create_physical_plan(plan)
            .await
            .map_err(|error| PipelineError::Execution(error.to_string()))
    }

    /// Rewrite a view query for one fixed source interval, after checking its plan signature.
    fn rewrite_incremental_query(
        &self,
        view: &TableIdentifier,
        expected_signature: &str,
        refresh: IvmRefreshContext,
    ) -> PipelineResult<Option<CteQuery>> {
        let node = self.definitions.graph.node(view).ok_or_else(|| {
            PipelineError::Invalid(format!("materialized view `{view}` is not defined"))
        })?;
        let current_signature = IvmPlanSignatureGenerator.generate(&node.query);
        if current_signature.sha256 != expected_signature {
            return Err(PipelineError::Invalid(format!(
                "IVM plan signature changed for `{view}`: expected {expected_signature}, current {}; full refresh required",
                current_signature.sha256
            )));
        }
        plan_incremental_view(&node.query, refresh)
    }

    /// Resolve source versions and streams, then prepare one executable MV refresh.
    /// An uninitialized stream uses a full snapshot refresh to establish its cursor.
    async fn plan_incremental_refresh(
        &self,
        node: &DynamicTable,
        started: Instant,
    ) -> PipelineResult<Option<InProcessNodeExecution>> {
        let view = &node.id;
        let streams = self.definitions.streams.lock().await;
        let view_streams = streams.get(view).ok_or_else(|| {
            PipelineError::Invalid(format!("materialized view `{view}` is not defined"))
        })?;
        for stream in view_streams.values() {
            if stream.status != StreamStatus::Active {
                return Err(PipelineError::Invalid(format!(
                    "stream `{}` is not active",
                    stream.name
                )));
            }
        }
        if view_streams
            .values()
            .any(|stream| stream.last_version.is_none())
        {
            drop(streams);
            return self.plan_full_node(node, started).await.map(Some);
        }
        let mut intervals = BTreeMap::new();
        let mut changed = Vec::new();
        for (source, stream) in view_streams {
            let latest = self.latest_version(source).await?;
            if stream
                .pending_interval(latest)
                .map_err(|error| PipelineError::Invalid(error.to_string()))?
                .is_some()
            {
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
            [] => return Ok(None),
            [source] => source.clone(),
            _ => {
                return Err(PipelineError::Invalid(format!(
                    "multiple sources changed for `{view}`; full refresh required"
                )));
            }
        };
        let refresh = IvmRefreshContext::new(changed_table, intervals.clone())
            .map_err(|error| PipelineError::Invalid(error.to_string()))?;
        let expected_signature = self.definitions.plan_signatures.get(view).ok_or_else(|| {
            PipelineError::Invalid(format!("materialized view `{view}` is not defined"))
        })?;
        let Some(query) = self.rewrite_incremental_query(view, expected_signature, refresh)? else {
            return Ok(None);
        };
        let expected_snapshot = self.latest_version(view).await?;
        let advanced = view_streams
            .values()
            .map(|stream| {
                stream
                    .advanced_to(intervals[&stream.source].after_inclusive)
                    .map_err(|error| PipelineError::Invalid(error.to_string()))
            })
            .collect::<PipelineResult<Vec<_>>>()?;
        Ok(Some(InProcessNodeExecution {
            engine: self.clone(),
            view: view.clone(),
            started,
            execution: PlannedQuery::Cte(query),
            write: RefreshWrite::Changes { expected_snapshot },
            advanced,
        }))
    }

    async fn plan_full_node(
        &self,
        node: &DynamicTable,
        started: Instant,
    ) -> PipelineResult<InProcessNodeExecution> {
        let view = &node.id;
        let streams = self.definitions.streams.lock().await;
        let view_streams = streams.get(view).ok_or_else(|| {
            PipelineError::Invalid(format!("materialized view `{view}` is not defined"))
        })?;
        let mut source_versions = BTreeMap::new();
        for source in view_streams.keys() {
            source_versions.insert(source.clone(), self.latest_version(source).await?);
        }
        let query = if let Some(source) = source_versions.keys().next() {
            let intervals = source_versions
                .iter()
                .map(|(source, version)| {
                    Ok((
                        source.clone(),
                        IncrementInterval::try_new(*version, *version)
                            .map_err(|error| PipelineError::Invalid(error.to_string()))?,
                    ))
                })
                .collect::<PipelineResult<BTreeMap<_, _>>>()?;
            let refresh = IvmRefreshContext::new(source.clone(), intervals)
                .map_err(|error| PipelineError::Invalid(error.to_string()))?;
            IvmSnapshotPlanner::new(&refresh)
                .after(&node.query)
                .map_err(|error| PipelineError::Invalid(error.to_string()))?
        } else {
            node.query.clone()
        };
        let logical = materialized_view_plan(query)
            .map_err(|error| PipelineError::Invalid(error.to_string()))?;
        let execution = self.create_physical_plan(&logical).await?;
        let advanced = view_streams
            .values()
            .map(|stream| {
                let mut stream = stream.clone();
                stream.last_version = Some(source_versions[&stream.source]);
                stream
            })
            .collect();
        Ok(InProcessNodeExecution {
            engine: self.clone(),
            view: view.clone(),
            started,
            execution: PlannedQuery::Physical(execution),
            write: RefreshWrite::Overwrite,
            advanced,
        })
    }

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

#[async_trait]
impl NodeExecutionPlanner for RefreshEngine {
    async fn plan(
        &self,
        node: &DynamicTable,
        mode: RefreshMode,
    ) -> PipelineResult<Option<Box<dyn NodeExecution>>> {
        let started = Instant::now();
        let execution = match mode {
            RefreshMode::Full => Some(self.plan_full_node(node, started).await?),
            RefreshMode::Incremental => self.plan_incremental_refresh(node, started).await?,
        };
        Ok(execution.map(|execution| Box::new(execution) as Box<dyn NodeExecution>))
    }
}

#[derive(Clone, Copy)]
enum RefreshWrite {
    Overwrite,
    Changes { expected_snapshot: i64 },
}

struct InProcessNodeExecution {
    engine: RefreshEngine,
    view: TableIdentifier,
    started: Instant,
    execution: PlannedQuery,
    write: RefreshWrite,
    advanced: Vec<IvmStreamObject>,
}

enum PlannedQuery {
    Physical(Arc<dyn ExecutionPlan>),
    Cte(CteQuery),
}

#[async_trait]
impl NodeExecution for InProcessNodeExecution {
    async fn execute(&self) -> PipelineResult<()> {
        let result = match &self.execution {
            PlannedQuery::Physical(execution) => {
                collect_with_peak(&self.engine.session, Arc::clone(execution))
                    .await
                    .map_err(|error| PipelineError::Execution(error.to_string()))?
            }
            PlannedQuery::Cte(query) => CteQueryExecutor::new(&self.engine.session)
                .execute(query)
                .await
                .map_err(|error| PipelineError::Execution(error.to_string()))?,
        };
        let peak_memory_bytes = result.peak_memory_bytes;
        let batches = result.batches;
        match self.write {
            RefreshWrite::Overwrite => self
                .engine
                .formats
                .overwrite_batches(&self.engine.session, &self.view, batches)
                .await
                .map_err(|error| PipelineError::Execution(error.to_string()))?,
            RefreshWrite::Changes { expected_snapshot } => self
                .engine
                .formats
                .write_changes(&self.engine.session, &self.view, batches, expected_snapshot)
                .await
                .map_err(|error| PipelineError::Execution(error.to_string()))?,
        }
        self.engine
            .save_refresh_metadata(&self.view, &self.advanced)
            .await?;
        let mut streams = self.engine.definitions.streams.lock().await;
        let view_streams = streams.get_mut(&self.view).ok_or_else(|| {
            PipelineError::Invalid(format!("materialized view `{}` is not defined", self.view))
        })?;
        for stream in &self.advanced {
            view_streams.insert(stream.source.clone(), stream.clone());
        }
        let mode = match self.write {
            RefreshWrite::Overwrite => "full",
            RefreshWrite::Changes { .. } => "incremental",
        };
        info!(view = %self.view, mode, elapsed_ms = self.started.elapsed().as_millis(),
            query_peak_memory_bytes = peak_memory_bytes, "materialized view refresh completed");
        Ok(())
    }
}
