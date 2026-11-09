use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::logical_plan::{Extension, TableScan};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::ivm::normalize::ROW_KIND_COLUMN;

use super::IncrementInterval;

/// The storage read requested by an IVM logical scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IvmScanKind {
    Snapshot(i64),
    Delta(IncrementInterval),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IvmScanNode {
    source: TableScan,
    kind: IvmScanKind,
    schema: DFSchemaRef,
}

impl IvmScanNode {
    pub fn snapshot(source: TableScan, version: i64) -> DataFusionResult<Self> {
        Self::new(source, IvmScanKind::Snapshot(version))
    }

    pub fn delta(source: TableScan, interval: IncrementInterval) -> DataFusionResult<Self> {
        Self::new(source, IvmScanKind::Delta(interval))
    }

    fn new(source: TableScan, kind: IvmScanKind) -> DataFusionResult<Self> {
        let mut fields = source
            .projected_schema
            .iter()
            .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
            .collect::<Vec<_>>();
        if matches!(kind, IvmScanKind::Delta(_)) {
            fields.push((
                None,
                Arc::new(Field::new(ROW_KIND_COLUMN, DataType::Int32, false)),
            ));
        }
        let schema = Arc::new(DFSchema::new_with_metadata(
            fields,
            source.projected_schema.metadata().clone(),
        )?);
        Ok(Self {
            source,
            kind,
            schema,
        })
    }

    pub fn source(&self) -> &TableScan {
        &self.source
    }

    pub fn kind(&self) -> IvmScanKind {
        self.kind
    }

    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

impl PartialOrd for IvmScanNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (
            self.source.table_name.to_string(),
            self.source.projection.as_ref(),
            self.kind,
            self.schema.to_string(),
        )
            .partial_cmp(&(
                other.source.table_name.to_string(),
                other.source.projection.as_ref(),
                other.kind,
                other.schema.to_string(),
            ))
    }
}

impl UserDefinedLogicalNodeCore for IvmScanNode {
    fn name(&self) -> &str {
        match self.kind {
            IvmScanKind::Snapshot(_) => "IvmSnapshotScan",
            IvmScanKind::Delta(_) => "IvmDeltaScan",
        }
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
        match self.kind {
            IvmScanKind::Snapshot(version) => write!(
                formatter,
                "IvmSnapshotScan: table={}, version={version}",
                self.source.table_name
            ),
            IvmScanKind::Delta(interval) => write!(
                formatter,
                "IvmDeltaScan: table={}, interval=({}, {}]",
                self.source.table_name, interval.before_exclusive, interval.after_inclusive
            ),
        }
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if !exprs.is_empty() || !inputs.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "IVM scan nodes are leaves without expressions".to_string(),
            ));
        }
        Ok(self.clone())
    }
}
