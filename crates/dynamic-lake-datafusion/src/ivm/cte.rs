use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchemaRef};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::logical_plan::Extension;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use futures::StreamExt;

use super::plan_utils::project_plan;

static NEXT_CTE_ID: AtomicU64 = AtomicU64::new(1);

/// Logical marker removed by CteQueryExecutor before physical planning.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CteMarkerNode {
    id: u64,
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
}

impl PartialOrd for CteMarkerNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.id == other.id && self != other {
            None
        } else {
            self.id.partial_cmp(&other.id)
        }
    }
}

impl UserDefinedLogicalNodeCore for CteMarkerNode {
    fn name(&self) -> &str {
        "SharedCte"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SharedCte: id={}", self.id)
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "shared CTE requires one input and no expressions".into(),
            ));
        }
        let input = inputs.remove(0);
        Ok(Self {
            id: self.id,
            schema: input.schema().clone(),
            input: Arc::new(input),
        })
    }
}

/// Mark a logical subplan for reuse while preserving its qualified output schema.
pub fn cte_ref(input: LogicalPlan) -> Result<LogicalPlan> {
    let id = NEXT_CTE_ID.fetch_add(1, Ordering::Relaxed);
    let original = input.schema().columns();
    // A shared subplan can have equal field names under different qualifiers.
    // Give cached columns unique names, then restore the original schema.
    let names = (0..original.len())
        .map(|index| format!("__shared_cte_{id}_column_{index}"))
        .collect::<Vec<_>>();
    let renamed = project_plan(
        input,
        original
            .iter()
            .zip(&names)
            .map(|(column, name)| Expr::Column(column.clone()).alias(name))
            .collect(),
    )?;
    let marked = LogicalPlan::Extension(Extension {
        node: Arc::new(CteMarkerNode {
            id,
            schema: renamed.schema().clone(),
            input: Arc::new(renamed),
        }),
    });
    project_plan(
        marked,
        original
            .into_iter()
            .zip(names)
            .map(|(column, name)| {
                Expr::Column(Column::from_name(name)).alias_qualified(column.relation, column.name)
            })
            .collect(),
    )
}

struct CteDefinition {
    id: u64,
    producer: LogicalPlan,
    dependencies: BTreeSet<u64>,
}

/// A logical query and its shared producers, ordered from dependencies to users.
///
/// DataFusion 54 does not materialize and reuse CTE results for these
/// subplans: repeated logical references would execute the producer again.
/// We keep the producers here so the executor can evaluate each one once and
/// substitute its cached result.
/// Constructing this value does not execute any part of the query.
pub struct CteQuery {
    result: LogicalPlan,
    definitions: Vec<CteDefinition>,
}

impl CteQuery {
    pub fn new(result: LogicalPlan) -> Result<Self> {
        let mut pending = BTreeMap::new();
        result.apply(|node| {
            if let LogicalPlan::Extension(extension) = node {
                if let Some(marker) = extension.node.as_any().downcast_ref::<CteMarkerNode>() {
                    if pending.contains_key(&marker.id) {
                        return Ok(TreeNodeRecursion::Continue);
                    }
                    let producer = (*marker.input).clone();
                    let mut dependencies = BTreeSet::new();
                    producer.apply(|inner| {
                        if let LogicalPlan::Extension(inner_extension) = inner {
                            if let Some(dependency) = inner_extension
                                .node
                                .as_any()
                                .downcast_ref::<CteMarkerNode>()
                            {
                                dependencies.insert(dependency.id);
                            }
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                    pending.insert(
                        marker.id,
                        CteDefinition {
                            id: marker.id,
                            producer,
                            dependencies,
                        },
                    );
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        let mut definitions = Vec::with_capacity(pending.len());
        let mut ready = BTreeSet::new();
        while !pending.is_empty() {
            let id = pending
                .iter()
                .find(|(_, definition)| definition.dependencies.is_subset(&ready))
                .map(|(id, _)| *id)
                .ok_or_else(|| DataFusionError::Plan("cyclic or missing CTE dependency".into()))?;
            definitions.push(pending.remove(&id).expect("selected CTE exists"));
            ready.insert(id);
        }
        Ok(Self {
            result,
            definitions,
        })
    }
}

pub struct QueryExecutionResult {
    pub batches: Vec<RecordBatch>,
    /// Peak DataFusion reservations plus retained CTE and result batch sizes.
    /// This is query accounting, not process RSS.
    pub peak_memory_bytes: usize,
}

pub struct CteQueryExecutor<'a> {
    session: &'a SessionContext,
}

struct TrackedExecution {
    pool: Arc<PeakMemoryPool>,
    memory_pool: Arc<dyn MemoryPool>,
    task: Arc<TaskContext>,
}

impl TrackedExecution {
    fn new(session: &SessionContext) -> Result<Self> {
        let pool = Arc::new(PeakMemoryPool::new(Arc::clone(
            &session.runtime_env().memory_pool,
        )));
        let runtime = RuntimeEnvBuilder::from_runtime_env(&session.runtime_env())
            .with_memory_pool(pool.clone())
            .build_arc()?;
        let session_task = session.task_ctx();
        let task = Arc::new(TaskContext::new(
            session_task.task_id(),
            session_task.session_id(),
            session_task.session_config().clone(),
            session_task.scalar_functions().clone(),
            session_task.higher_order_functions().clone(),
            session_task.aggregate_functions().clone(),
            session_task.window_functions().clone(),
            runtime,
        ));
        let memory_pool: Arc<dyn MemoryPool> = pool.clone();
        Ok(Self {
            pool,
            memory_pool,
            task,
        })
    }

    async fn collect(
        &self,
        execution: Arc<dyn ExecutionPlan>,
        consumer: String,
    ) -> Result<(Vec<RecordBatch>, MemoryReservation)> {
        collect_tracked(
            execution,
            Arc::clone(&self.task),
            &self.memory_pool,
            consumer,
        )
        .await
    }
}

/// Collect a physical plan with the same peak-memory accounting as CTE queries.
pub async fn collect_with_peak(
    session: &SessionContext,
    execution: Arc<dyn ExecutionPlan>,
) -> Result<QueryExecutionResult> {
    let tracked = TrackedExecution::new(session)?;
    let (batches, _reservation) = tracked
        .collect(execution, "query-result".to_owned())
        .await?;
    Ok(QueryExecutionResult {
        batches,
        peak_memory_bytes: tracked.pool.peak(),
    })
}

impl<'a> CteQueryExecutor<'a> {
    pub fn new(session: &'a SessionContext) -> Self {
        Self { session }
    }

    pub async fn execute(&self, query: &CteQuery) -> Result<QueryExecutionResult> {
        let tracked = TrackedExecution::new(self.session)?;
        let mut cached = BTreeMap::new();
        let mut reservations = Vec::new();

        // Evaluate shared producers in dependency order, then run the result:
        //
        //   producer A -> producer B -> result query
        //   cache A       cache B       execute
        //
        // All stages use one pool so peak memory spans the whole query.
        for definition in &query.definitions {
            let producer = replace_refs(definition.producer.clone(), &cached)?;
            let execution = self.session.state().create_physical_plan(&producer).await?;
            let schema = execution.schema();
            let (batches, reservation) = tracked
                .collect(execution, format!("cte-{}", definition.id))
                .await?;
            let table = Arc::new(MemTable::try_new(schema, vec![batches])?);
            let plan = self.session.read_table(table)?.into_unoptimized_plan();
            cached.insert(definition.id, plan);
            reservations.push(reservation);
        }
        let result = replace_refs(query.result.clone(), &cached)?;
        let execution = self.session.state().create_physical_plan(&result).await?;
        let (batches, _result_reservation) =
            tracked.collect(execution, "cte-result".to_owned()).await?;
        let peak_memory_bytes = tracked.pool.peak();
        Ok(QueryExecutionResult {
            batches,
            peak_memory_bytes,
        })
    }
}

async fn collect_tracked(
    execution: Arc<dyn ExecutionPlan>,
    task: Arc<TaskContext>,
    pool: &Arc<dyn MemoryPool>,
    consumer: String,
) -> Result<(Vec<RecordBatch>, MemoryReservation)> {
    let mut stream: SendableRecordBatchStream = execute_stream(execution, task)?;
    let reservation = MemoryConsumer::new(consumer).register(pool);
    let mut batches = Vec::new();
    // Record batches retained by collect are not reserved by DataFusion's
    // operators. Reserve each batch as it arrives so it overlaps with live
    // operator reservations in the measured peak.
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        reservation.try_grow(batch.get_array_memory_size())?;
        batches.push(batch);
    }
    Ok((batches, reservation))
}

fn replace_refs(plan: LogicalPlan, cached: &BTreeMap<u64, LogicalPlan>) -> Result<LogicalPlan> {
    plan.transform_up(|node| match node {
        LogicalPlan::Extension(extension) => {
            if let Some(marker) = extension.node.as_any().downcast_ref::<CteMarkerNode>() {
                let source = cached.get(&marker.id).ok_or_else(|| {
                    DataFusionError::Plan(format!("CTE {} was not cached", marker.id))
                })?;
                Ok(Transformed::yes(source.clone()))
            } else {
                Ok(Transformed::no(LogicalPlan::Extension(extension)))
            }
        }
        other => Ok(Transformed::no(other)),
    })
    .map(|transformed| transformed.data)
}

#[derive(Debug)]
struct PeakMemoryPool {
    inner: Arc<dyn MemoryPool>,
    used: AtomicUsize,
    peak: AtomicUsize,
}

impl PeakMemoryPool {
    fn new(inner: Arc<dyn MemoryPool>) -> Self {
        Self {
            inner,
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }
    fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }
    fn record_growth(&self, bytes: usize) {
        let used = self.used.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak.fetch_max(used, Ordering::Relaxed);
    }
}

impl Drop for PeakMemoryPool {
    fn drop(&mut self) {
        // The last pool reference is released only after all query reservations.
        // A nonzero balance means a reservation leaked or grow/shrink diverged.
        assert_eq!(
            self.used.load(Ordering::Relaxed),
            0,
            "CTE query memory reservations were not fully released"
        );
    }
}

impl fmt::Display for PeakMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "peak-tracking({})", self.inner)
    }
}

impl MemoryPool for PeakMemoryPool {
    fn name(&self) -> &str {
        "cte-peak"
    }
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }
    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.record_growth(additional);
    }
    fn shrink(&self, reservation: &MemoryReservation, bytes: usize) {
        self.inner.shrink(reservation, bytes);
        self.used.fetch_sub(bytes, Ordering::Relaxed);
    }
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.record_growth(additional);
        Ok(())
    }
    fn reserved(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::logical_plan::Union;

    use super::*;

    #[tokio::test]
    async fn caches_nested_ctes_and_restores_duplicate_column_names() -> Result<()> {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        for table in ["a", "b"] {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![1]))],
            )?;
            session.register_table(
                table,
                Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])?),
            )?;
        }
        let source = session
            .sql("SELECT a.id, b.id FROM a CROSS JOIN b")
            .await?
            .into_unoptimized_plan();
        let expected_schema = source.schema().clone();
        let first = cte_ref(source)?;
        assert_eq!(first.schema(), &expected_schema);
        let second = cte_ref(first)?;
        assert_eq!(second.schema(), &expected_schema);
        let plan = LogicalPlan::Union(Union::try_new(vec![
            Arc::new(second.clone()),
            Arc::new(second),
        ])?);
        let query = CteQuery::new(plan)?;
        let result = CteQueryExecutor::new(&session).execute(&query).await?;
        assert!(result.peak_memory_bytes > 0);
        assert_eq!(
            result
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        assert_eq!(result.batches[0].num_columns(), 2);
        Ok(())
    }
}
