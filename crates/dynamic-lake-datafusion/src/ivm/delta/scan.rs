use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::logical_plan::{Extension, TableScan};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::ivm::normalize::ROW_KIND_COLUMN;

use super::IncrementInterval;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IvmDeltaScanNode {
    source: TableScan,
    interval: IncrementInterval,
    schema: DFSchemaRef,
}

impl IvmDeltaScanNode {
    pub fn new(source: TableScan, interval: IncrementInterval) -> DataFusionResult<Self> {
        let schema = schema_with_row_kind(&source, true)?;
        Ok(Self {
            source,
            interval,
            schema,
        })
    }

    pub fn source(&self) -> &TableScan {
        &self.source
    }

    pub fn interval(&self) -> IncrementInterval {
        self.interval
    }

    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

impl PartialOrd for IvmDeltaScanNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (
            self.source.table_name.to_string(),
            self.source.projection.as_ref(),
            self.interval,
            self.schema.to_string(),
        )
            .partial_cmp(&(
                other.source.table_name.to_string(),
                other.source.projection.as_ref(),
                other.interval,
                other.schema.to_string(),
            ))
    }
}

impl UserDefinedLogicalNodeCore for IvmDeltaScanNode {
    fn name(&self) -> &str {
        "IvmDeltaScan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "IvmDeltaScan: table={}, interval=({}, {}]",
            self.source.table_name, self.interval.before_exclusive, self.interval.after_inclusive
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        ensure_leaf(exprs, inputs)?;
        Ok(self.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IvmSnapshotScanNode {
    source: TableScan,
    version: i64,
    schema: DFSchemaRef,
}

impl IvmSnapshotScanNode {
    pub fn new(source: TableScan, version: i64) -> DataFusionResult<Self> {
        let schema = schema_with_row_kind(&source, false)?;
        Ok(Self {
            source,
            version,
            schema,
        })
    }

    pub fn source(&self) -> &TableScan {
        &self.source
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

impl PartialOrd for IvmSnapshotScanNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (
            self.source.table_name.to_string(),
            self.source.projection.as_ref(),
            self.version,
            self.schema.to_string(),
        )
            .partial_cmp(&(
                other.source.table_name.to_string(),
                other.source.projection.as_ref(),
                other.version,
                other.schema.to_string(),
            ))
    }
}

impl UserDefinedLogicalNodeCore for IvmSnapshotScanNode {
    fn name(&self) -> &str {
        "IvmSnapshotScan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "IvmSnapshotScan: table={}, version={}",
            self.source.table_name, self.version
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        ensure_leaf(exprs, inputs)?;
        Ok(self.clone())
    }
}

fn schema_with_row_kind(source: &TableScan, with_row_kind: bool) -> DataFusionResult<DFSchemaRef> {
    let mut fields = source
        .projected_schema
        .iter()
        .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
        .collect::<Vec<_>>();
    if with_row_kind {
        fields.push((
            None,
            Arc::new(Field::new(ROW_KIND_COLUMN, DataType::Int32, false)),
        ));
    }
    Ok(Arc::new(DFSchema::new_with_metadata(
        fields,
        source.projected_schema.metadata().clone(),
    )?))
}

fn ensure_leaf(exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DataFusionResult<()> {
    if !exprs.is_empty() || !inputs.is_empty() {
        return Err(datafusion::error::DataFusionError::Plan(
            "IVM scan nodes are leaves without expressions".to_string(),
        ));
    }
    Ok(())
}
