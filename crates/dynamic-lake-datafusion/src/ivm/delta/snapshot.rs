use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::LogicalPlan;
use dynamic_lake_core::TableIdentifier;

use crate::ivm::error::IvmError;

use super::scan::IvmSnapshotScanNode;
use super::{IvmRefreshContext, scope::reference_parts};

/// Rewrites every table scan in a subtree to a fixed before/after version.
pub struct IvmSnapshotPlanner<'a> {
    refresh: &'a IvmRefreshContext,
}

impl<'a> IvmSnapshotPlanner<'a> {
    pub fn new(refresh: &'a IvmRefreshContext) -> Self {
        Self { refresh }
    }

    pub fn before(&self, plan: &LogicalPlan) -> Result<LogicalPlan, IvmError> {
        self.snapshot(plan, true)
    }

    pub fn after(&self, plan: &LogicalPlan) -> Result<LogicalPlan, IvmError> {
        self.snapshot(plan, false)
    }

    fn snapshot(&self, plan: &LogicalPlan, before: bool) -> Result<LogicalPlan, IvmError> {
        plan.clone()
            .transform_up(|plan| {
                let LogicalPlan::TableScan(scan) = plan else {
                    return Ok(Transformed::no(plan));
                };
                let table = table_identifier(&scan.table_name)?;
                let interval = self.refresh.interval_for(&table).ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "missing IVM interval for `{table}`"
                    ))
                })?;
                let version = if before {
                    interval.before_exclusive
                } else {
                    interval.after_inclusive
                };
                Ok(Transformed::yes(
                    IvmSnapshotScanNode::new(scan, version)?.into_plan(),
                ))
            })
            .map(|transformed| transformed.data)
            .map_err(IvmError::DataFusion)
    }
}

fn table_identifier(
    reference: &datafusion::common::TableReference,
) -> datafusion::error::Result<TableIdentifier> {
    TableIdentifier::try_new(reference_parts(reference))
        .map_err(|error| datafusion::error::DataFusionError::Plan(error.to_string()))
}
