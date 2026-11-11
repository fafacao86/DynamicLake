//! Physical planning for IVM snapshot and change scans.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, PhysicalExpr};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use dynamic_lake_format::{
    CHANGE_KIND_COLUMN, ChangeReadPlanRequest, LakeFormatRegistry, ReadPlanRequest,
};
use futures::StreamExt;

use super::delta::{IvmScanKind, IvmScanNode};
use super::normalize::ROW_KIND_COLUMN;
use super::plan_utils::table_identifier;

/// Attach lake scan planning to a DataFusion session.
pub fn install_lake_scans(session: SessionContext, formats: LakeFormatRegistry) -> SessionContext {
    SessionContext::new_with_state(
        session
            .into_state_builder()
            .with_query_planner(Arc::new(LakeScanQueryPlanner { formats }))
            .build(),
    )
}

struct LakeScanQueryPlanner {
    formats: LakeFormatRegistry,
}

impl fmt::Debug for LakeScanQueryPlanner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LakeScanQueryPlanner")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl QueryPlanner for LakeScanQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(IvmExtensionPlanner {
            formats: self.formats.clone(),
        })])
        .create_physical_plan(logical_plan, session_state)
        .await
    }
}

struct IvmExtensionPlanner {
    formats: LakeFormatRegistry,
}

#[async_trait]
impl ExtensionPlanner for IvmExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(scan_node) = node.as_any().downcast_ref::<IvmScanNode>() else {
            return Ok(None);
        };
        let scan = scan_node.source();
        let table = table_identifier(&scan.table_name)?;
        let planner = self.planner_for(&table)?;
        let execution = match scan_node.kind() {
            IvmScanKind::Snapshot(version) => {
                planner
                    .plan_read(
                        session_state,
                        ReadPlanRequest {
                            table,
                            snapshot: Some(version),
                            projection: scan.projection.clone(),
                            filters: scan.filters.clone(),
                            limit: scan.fetch,
                        },
                    )
                    .await
                    .map_err(format_error)?
                    .execution
            }
            IvmScanKind::Delta(interval) => {
                let read = planner
                    .plan_changes(session_state, ChangeReadPlanRequest { table, interval })
                    .await
                    .map_err(format_error)?;
                let source_schema = read.execution.schema();
                let mut projections = scan
                    .projected_schema
                    .fields()
                    .iter()
                    .map(|field| project_column(field.name(), field.name(), &source_schema))
                    .collect::<Result<Vec<_>>>()?;
                projections.push(project_column(
                    CHANGE_KIND_COLUMN,
                    ROW_KIND_COLUMN,
                    &source_schema,
                )?);
                Arc::new(ProjectionExec::try_new(projections, read.execution)?)
            }
        };
        let schema = Arc::new(scan_node.schema().as_arrow().clone());
        Ok(Some(adapt_scan_schema(execution, schema)?))
    }
}

impl IvmExtensionPlanner {
    fn planner_for(
        &self,
        table: &dynamic_lake_core::TableIdentifier,
    ) -> Result<&dyn dynamic_lake_format::LakeTablePlanner> {
        let catalog = table
            .parts()
            .first()
            .ok_or_else(|| DataFusionError::Plan(format!("IVM table `{table}` has no catalog")))?;
        self.formats.planner(catalog).ok_or_else(|| {
            DataFusionError::Plan(format!("no lake format registered for catalog `{catalog}`"))
        })
    }
}

fn project_column(
    source: &str,
    target: &str,
    schema: &datafusion::arrow::datatypes::Schema,
) -> Result<ProjectionExpr> {
    let index = schema.index_of(source)?;
    Ok(ProjectionExpr {
        expr: Arc::new(Column::new(source, index)) as Arc<dyn PhysicalExpr>,
        alias: target.to_string(),
    })
}

fn format_error(error: dynamic_lake_format::LakeFormatError) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}

/// Paimon.rs currently marks every source field nullable. Keep the logical source
/// constraints and verify them on each batch produced by the native scan.
fn adapt_scan_schema(
    execution: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let source = execution.schema();
    if source.fields().len() != schema.fields().len() {
        return Err(DataFusionError::Plan(
            "IVM scan field count differs from the logical scan".into(),
        ));
    }
    for (actual, expected) in source.fields().iter().zip(schema.fields()) {
        if actual.name() != expected.name()
            || (actual.data_type() != expected.data_type()
                && !matches!(
                    (actual.data_type(), expected.data_type()),
                    (DataType::Utf8View, DataType::Utf8)
                ))
        {
            return Err(DataFusionError::Plan(format!(
                "IVM scan field differs: physical {actual:?}, logical {expected:?}"
            )));
        }
    }
    let partitions = (0..execution.output_partitioning().partition_count())
        .map(|index| {
            Arc::new(AdaptedPartition {
                execution: Arc::clone(&execution),
                index,
                schema: Arc::clone(&schema),
            }) as Arc<dyn PartitionStream>
        })
        .collect();
    Ok(Arc::new(StreamingTableExec::try_new(
        schema,
        partitions,
        None,
        std::iter::empty(),
        false,
        None,
    )?))
}

#[derive(Debug)]
struct AdaptedPartition {
    execution: Arc<dyn ExecutionPlan>,
    index: usize,
    schema: SchemaRef,
}

impl PartitionStream for AdaptedPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, context: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let output_schema = Arc::clone(&schema);
        let stream = match self.execution.execute(self.index, context) {
            Ok(stream) => stream
                .map(move |batch| adapt_batch_schema(batch?, Arc::clone(&output_schema)))
                .boxed(),
            Err(error) => futures::stream::once(async move { Err(error) }).boxed(),
        };
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

fn adapt_batch_schema(batch: RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column, field.data_type()).map_err(DataFusionError::from)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    for (field, column) in schema.fields().iter().zip(&columns) {
        if !field.is_nullable() && column.null_count() != 0 {
            return Err(DataFusionError::Execution(format!(
                "IVM scan produced nulls for required field `{}`",
                field.name()
            )));
        }
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}
